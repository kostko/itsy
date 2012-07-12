from __future__ import absolute_import

import copy

from . import base as fields_base
from .. import references
from ..document import Document, EmbeddedDocument, RESTRICT
from .base import Field, FieldSearchMapping

__all__ = [
  "CachedReferenceField",
]

class ReferencedDynamicField(fields_base.DynamicField):
  def pre_save(self, value, document, update = False):
    """
    Since we will be using the dynamic function on the source document, we
    shouldn't do anything with the value here.
    """
    return value

class CachedReferenceDocument(EmbeddedDocument):
  """
  The base class for customized cached reference documents. It is a simple
  embedded document with some additional methods. Should never be instantiated
  directly.
  """
  class Meta:
    abstract = True

  # Referenced document class
  _referenced_doc = None

  def sync(self, document = None):
    """
    Syncs the cached fields with the source document. If the document is not
    provided, it is automatically fetched from the database, based on the
    identifier.

    @param document: Optional document instance
    """
    if document is None:
      document = self._referenced_doc.get(pk = self.id)
    elif self.id is not None and document.pk != self.id:
      raise ValueError("Referenced document identifier mismatch!")
    elif self.version is not None and document._version < self.version:
      return

    # Copy values from source document and evaluate any DynamicFields on the
    # source document
    for name, field in self._meta.fields.items():
      if name in ('id', 'version'):
        continue

      if isinstance(field, ReferencedDynamicField):
        setattr(self, name, field.function(document))
      else:
        setattr(self, name, getattr(document, name))

    # Update the id and version number
    self.id = document.pk
    self.version = document._version

  def follow(self):
    """
    Dereferences this cached reference and returns the complete version of
    this document.
    """
    return self._referenced_doc.get(pk = self.id)

def create_cached_reference_document(name, document, fields):
  """
  Creates a new specialized cached reference (embedded) document with some
  fields copied from the original document.

  @param name: Class suffix (only used internally)
  @param document: Source (referenced) document
  @param fields: A list of fields to cache
  @return: A new class that describes the cached reference
  """
  attrs = dict(
    # Identifier (references primary key of the source document)
    id = document._meta.get_primary_key_field().__class__(db_name = "id"),
    # Version number
    version = fields_base.IntegerField(db_name = "_version"),
  )

  # Ensure that serial primary keys don't get incremented
  attrs["id"].no_pre_save = True

  for field in fields:
    if isinstance(field, CachedReferenceField):
      # Prevent inclusion of other cached references
      raise TypeError("Cached references cannot cache other cached references!")
    elif isinstance(field, CachedReferenceField.DynamicRef):
      # Insert ReferencedDynamicFields when encountering DynamicRefs
      attrs[field.alias] = ReferencedDynamicField(field.field_type, field.getter, on_change = field.dependencies)
    else:
      # For all other fields, simply copy them from the source document, but make sure that
      # any pre_save handlers are disabled (otherwise they would corrupt cached data on save)
      attrs[field] = copy.deepcopy(document._meta.get_field_by_name(field))
      attrs[field].no_pre_save = True

  # Setup the referenced document class
  attrs['_referenced_doc'] = document

  # Setup the module name
  attrs['__module__'] = __name__

  new_cls = type("_CachedReference_%s" % name.capitalize(), (CachedReferenceDocument,), attrs)
  return new_cls

class ReverseCachedReferenceDescriptor(Field):
  """
  A descriptor that is automatically created for performing reverse queries.
  """
  def __init__(self, dst_class, dst_field_path, searchable_fields):
    """
    Class constructor.
    
    @param dst_class: Destination document class
    @param dst_field_path: Cached reference field path
    @param searchable_fields: Fields of destination document that should be searchable
    """
    self.dst_class = dst_class
    self.dst_field_path = dst_field_path.replace('.', '__')
    self.searchable_fields = searchable_fields
    
    super(ReverseCachedReferenceDescriptor, self).__init__(
      virtual = True, revisable = False, searchable = searchable_fields is not None
    )
  
  def __get__(self, obj, typ):
    """
    Performs a query that returns documents that reference the current object.
    """
    if obj.pk is None:
      raise AttributeError("Object must be saved before reverse references can be traversed!")
    
    return self.dst_class.find(**{ self.dst_field_path : obj.pk })
  
  def __delete__(self, obj):
    """
    Forbids deletion of this descriptor.
    """
    raise AttributeError("You cannot remove reverse references from a document!")
  
  def __set__(self, obj, value):
    """
    Forbids modification of this descriptor.
    """
    raise AttributeError("You cannot change reverse references in a document!")
  
  def from_search(self, value, document):
    """
    Converts value from Elastic Search store.
    """
    return value
  
  def to_search(self, value, document):
    """
    Converts value to Elastic Search store.
    """
    if self.searchable_fields is None:
      return

    value = []
    for rel_doc in self.__get__(document, type(document)).only(*self.searchable_fields):
      doc = {}
      for field in self.searchable_fields:
        fval = reduce(getattr, field.split('.'), rel_doc)
        if hasattr(fval, '_search_prepare'):
          fval = fval._search_prepare()
        
        doc[field.replace('.', '_')] = fval
      
      value.append(doc)

    return value

  def get_search_mapping(self):
    """
    Returns field mapping for Elastic Search.
    """
    mapping = super(ReverseCachedReferenceDescriptor, self).get_search_mapping()
    properties = FieldSearchMapping()
    for name in self.searchable_fields:
      field = self.dst_class._meta.resolve_subfield_hierarchy(name.split('.'), get_field = True)[1]
      if field is not None:
        properties[name.replace('.', '_')] = field.get_search_mapping()

    mapping.update(dict(
      type = "object",
      dynamic = True,
      enabled = self.searchable,
      properties = properties
    ))
    return mapping

class CachedReferenceField(Field):
  """
  Reference to some external document, but with a portion of external
  fields cached so no dereferencing is required.
  """
  class DynamicRef(object):
    """
    Marker for a dynamic reference field.
    """
    def __init__(self, field_type, alias, getter, dependencies, requires_document_class = None):
      self.field_type = field_type
      self.alias = alias
      self.getter = getter
      self.dependencies = dependencies
      self.requires_document_class = requires_document_class
  
  def __init__(self, document, fields, related_name = None, related_searchable = None, 
               no_id_index = False, on_delete = RESTRICT, **kwargs):
    """
    Class constructor.
    """
    self.document = document
    self.pending_resolve = True
    self.resolved_callback = None
    self.fields = fields
    self.related_name = related_name
    self.related_searchable = related_searchable
    self.no_id_index = no_id_index
    self.on_delete = on_delete
    
    # Calculate dependent fields
    self.dependencies = set()
    for field in self.fields:
      if isinstance(field, CachedReferenceField.DynamicRef):
        self.dependencies.update(field.dependencies)
      else:
        self.dependencies.add(field)
    
    super(CachedReferenceField, self).__init__(**kwargs)
  
  def get_indices(self):
    """
    This method may return a dictionary of indices for this field. Keys
    indicate subfields (for nested documents), to index the field itself
    use '.' as a key name. Dictionary values represent sort order.
    """
    if self.no_id_index:
      return {}
    else:
      return { 'id' : Document.ASCENDING }

  def get_subfield_metadata(self):
    """
    If this field has any subfields that have type metadata in the form
    of Field instances, it should be returned here.
    """
    return self.embedded._meta
  
  def setup_reverse_references(self, document_class, field_name):
    """
    This method may recursively setup reverse references.
    """
    def setup_reference(document):
      self.reference_path = '{0}.id'.format(field_name)
      document._meta.reverse_references.append((document_class, self.reference_path, self))
      
      if self.related_name is not None:
        # Create reverse accessor field
        if self.related_name in document.__dict__:
          raise AttributeError("Related name conflict for '{0}' in document '{1}' while processing document '{2}'!".format(
            self.related_name, document.__name__, self.cls.__name__
          ))
        
        field = ReverseCachedReferenceDescriptor(document_class, self.reference_path, self.related_searchable)
        field.contribute_to_class(document, self.related_name)
    
    if self.pending_resolve:
      self.resolved_callback = setup_reference
    else:
      setup_reference(self.document)
  
  def prepare(self):
    """
    Called when constructing the parent class, when name and cls are
    already known.
    """
    def reference_resolved(document):
      if document._meta.abstract:
        raise ValueError("Referenced document class '{0}' is abstract!".format(document))

      self.document = document
      self.pending_resolve = False
      self.embedded = create_cached_reference_document(self.name, document, self.fields)
      
      if self.resolved_callback is not None:
        self.resolved_callback(document)
    
    references.track(self, self.document, reference_resolved)
  
  def check_configuration(self):
    """
    Called after all fields on the document have been prepared to enable the
    field to check its configuration and raise configuration errors.
    """
    def reference_resolved(document):
      # Check that fields actually exist
      for field in self.fields:
        try:
          if isinstance(field, CachedReferenceField.DynamicRef):
            # Verify that the class requirements (if any) are satisfied
            for cls in (field.requires_document_class or []):
              if not issubclass(document, cls):
                raise TypeError("Dynamic field '{0}' requires the referenced document class '{1}' to inherit '{2}'!".format(
                  field.alias, document.__name__, cls.__name__
                ))
          else:
            document._meta.get_field_by_name(field)
        except KeyError:
          raise KeyError("Cached field '{0}' does not exist on document '{1}' for cached reference '{2}'!".format(
            field, document.__name__, self.name))
    
    references.track(self, self.document, reference_resolved)
  
  def check_referenced_class(self):
    """
    Checks whether the referenced class has been resolved and raises a ValueError
    in case it hasn't been.
    """
    try:
      if not issubclass(self.document, Document):
        raise TypeError
    except TypeError:
      raise ValueError("Invalid document class set for field '{0}' or document class not resolved!".format(self.name))

  def from_store(self, value, document):
    """
    Converts value from MongoDB store.
    """
    self.check_referenced_class()

    # Load document from the database (same as embedded document)
    reference = self.embedded()
    reference._parent = document
    reference._set_from_db(value)
    
    # Register cached reference instance for direct access, so one does not need
    # to traverse the whole (potential) hierarchy when updating references
    refs = document.get_top_level_document()._reference_fields
    refs.setdefault("{0}/{1}".format(self.reference_path, reference.id), []).append(reference)

    return reference

  def to_store(self, value, document):
    """
    Converts value to MongoDB store.
    """
    self.check_referenced_class()

    if isinstance(value, self.document):
      if value.pk is None:
        raise ValueError("Referenced document is missing an identifier!")

      reference = self.embedded()
      reference.sync(value)
      reference._parent = document
      return reference._db_prepare()
    elif isinstance(value, self.embedded):
      value._parent = document
      return value._db_prepare()
    else:
      raise ValueError("Unsupported value for field '{0}'!".format(self.name))

  def from_search(self, value, document):
    """
    Converts value from Elastic Search store.
    """
    self.check_referenced_class()

    # Load document from the database (same as embedded document)
    reference = self.embedded()
    reference._parent = document
    reference._set_from_search(value)

    # Register cached reference instance for direct access, so one does not need
    # to traverse the whole (potential) hierarchy when updating references
    refs = document.get_top_level_document()._reference_fields
    refs.setdefault("{0}/{1}".format(self.reference_path, reference.id), []).append(reference)

    return reference

  def to_search(self, value, document):
    """
    Converts value to Elastic Search store.
    """
    self.check_referenced_class()

    if isinstance(value, self.document):
      if value.pk is None:
        raise ValueError("Referenced document is missing an identifier!")

      reference = self.embedded()
      reference.sync(value)
      reference._parent = document
      return reference._search_prepare()
    elif isinstance(value, self.embedded):
      value._parent = document
      return value._search_prepare()
    else:
      raise ValueError("Unsupported value for field '{0}'!".format(self.name))

  def get_search_mapping(self):
    """
    Returns field mapping for Elastic Search.
    """
    mapping = super(CachedReferenceField, self).get_search_mapping()
    mapping.update(dict(
      type = "object",
      dynamic = "strict",
      enabled = self.searchable,
      properties = self.embedded._meta.search_mapping_prepare()
    ))
    return mapping
