from __future__ import absolute_import

from .. import references
from ..document import Document, EmbeddedDocument, RESTRICT, CASCADE
from .base import Field

# Exported classes
__all__ = [
  "CachedReferenceField",
]

class CachedField(object):
  def __init__(self, name, value):
    self.name = name
    self.value = value

class AutoField(CachedField):
  pass

class DynamicField(CachedField):
  def __init__(self, name, value, getter):
    super(DynamicField, self).__init__(name, value)
    self.getter = getter

class CachedReference(object):
  """
  Wrapper class for document references.
  """
  def __init__(self, fields, data, ext_document, src_document, src_fname):
    """
    Class constructor.
    
    @param fields: Cached fields
    @param data: Stored data or Document instance
    @param ext_document: External (referenced) document
    @param src_document: Source document class
    @param src_fname: Source field name
    """
    self.__dict__['_fields'] = {}
    self.__dict__['_document'] = ext_document
    self.__dict__['_src_document'] = src_document
    self.__dict__['_src_fname'] = src_fname
    
    if isinstance(data, ext_document):
      for field in fields:
        if isinstance(field, CachedReferenceField.DynamicRef):
          self._fields[field.alias] = DynamicField(field.alias, None, field.getter)
        else:
          self._fields[field] = CachedField(field, None)
      
      self._fields['id'] = AutoField('id', data._id)
      self._fields['_version'] = AutoField('_version', data._version)
      
      self.sync(data)
    else:
      for field in fields:
        if isinstance(field, CachedReferenceField.DynamicRef):
          self._fields[field.alias] = DynamicField(field.alias, data.get(field.alias, None), field.getter)
        else:
          self._fields[field] = CachedField(field, ext_document._meta.field_from_data(field, data))
      
      self._fields['id'] = AutoField('id', data.get('id'))
      self._fields['_version'] = AutoField('_version', data.get('version'))
  
  def __getstate__(self):
    """
    Returns state for serialization.
    """
    # Remove getters for dynamic fields as they will fail serialization otherwise
    fields = self._fields.copy()
    for name, field in fields.iteritems():
      if isinstance(field, DynamicField):
        field.getter = None
    
    return fields, self._document, self._src_document, self._src_fname
  
  def __setstate__(self, state):
    """
    Sets up state from serialized data.
    """
    self.__dict__['_fields'], self.__dict__['_document'], \
    self.__dict__['_src_document'], self.__dict__['_src_fname'] = state
    
    # Re-bind getters for dynamic fields
    descriptors = self._src_document._meta.get_field_by_name(self._src_fname).fields
    for field in descriptors:
      if isinstance(field, CachedReferenceField.DynamicRef):
        self._fields[field.alias].getter = field.getter
  
  def __getattr__(self, name):
    """
    Transparently resolve cached fields.
    """
    try:
      return self._fields[name].value
    except KeyError:
      raise AttributeError, name
  
  def __setattr__(self, name, value):
    """
    Prevent setting of attributes.
    """
    raise AttributeError("Cached document reference is read-only!")
  
  def follow(self):
    """
    Dereferences this cached reference and returns the complete version of
    this document.
    """
    return self._document(pk = self.id)
  
  def sync(self, document = None):
    """
    Synchronizes cache with actually referenced document.
    
    @param document: Optional referenced document
    """
    if document is None:
      document = self._document.find(_id = self.id).one()
    elif document._id != self.id:
      raise ValueError("Referenced document identifier mismatch!")
    elif document._version < self._version:
      return
    
    db_fields = document._db_prepare(fields = self._fields.keys(), db_names = False)
    for field in self._fields.values():
      if field.name in db_fields:
        field.value = db_fields[field.name]
      elif isinstance(field, DynamicField):
        field.value = field.getter(document)
    
    self._fields['_version'].value = document._version
  
  def to_store(self):
    """
    Prepares this cached reference to be suitable for storing into the database.
    """
    cache = {}
    for field in self._fields.values():
      if isinstance(field, (AutoField, DynamicField)):
        cache[field.name] = field.value
      else:
        self._document._meta.field_to_data(field.name, field.value, cache)
    
    return cache
  
  def to_search(self):
    """
    Prepares this cached reference to be suitable for storing into the search
    engine.
    """
    cache = {}
    for field in self._fields.values():
      cache[field.name] = field.value
    
    return cache

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
    if obj._id is None:
      raise AttributeError("Object must be saved before reverse references can be traversed!")
    
    return self.dst_class.find(**{ self.dst_field_path : obj._id })
  
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
    for rel_doc in self.__get__(document, type(document)):
      doc = {}
      for field in self.searchable_fields:
        fval = reduce(getattr, field.split('.'), rel_doc)
        if isinstance(fval, CachedReference):
          fval = fval.to_search()
        elif isinstance(fval, EmbeddedDocument):
          fval = fval._search_prepare()
        
        doc[field.replace('.', '_')] = fval
      
      value.append(doc)
    
    return value

  def get_search_mapping(self):
    """
    Returns field mapping for Elastic Search.
    """
    mapping = super(ReverseCachedReferenceDescriptor, self).get_search_mapping()
    mapping.update(dict(
      type = "object",
      # TODO Making this non-dynamic requires type resolution for cached references
      dynamic = True,
      enabled = self.searchable
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
    def __init__(self, alias, getter, dependencies, requires_document_class = None):
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
                raise TypeError("Dynamic field '{0}' requires the reference document class '{1}' to inherit '{2}'!".format(
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
    
    # Register cached reference instance for direct access, so one does not need
    # to traverse the whole (potential) hierarchy when updating references
    reference = CachedReference(self.fields, value, self.document, document.__class__, self.name)
    refs = document.get_top_level_document()._reference_fields
    refs.setdefault("{0}/{1}".format(self.reference_path, value['id']), []).append(reference)
    
    return reference
  
  def to_store(self, value, document):
    """
    Converts value to MongoDB store.
    """
    self.check_referenced_class()
    if isinstance(value, self.document):
      if value._id is None:
        raise ValueError("Referenced document is missing an identifier!")
      
      value = CachedReference(self.fields, value, self.document, document.__class__, self.name)
      return value.to_store()
    elif isinstance(value, CachedReference):
      if not issubclass(value._document, self.document):
        raise ValueError("Referenced document does not match the class specified in field definition!")
      
      return value.to_store()
    else:
      raise ValueError("Unsupported value for field '{0}'!".format(self.name))
  
  from_search = from_store
  to_search = to_store

  def get_search_mapping(self):
    """
    Returns field mapping for Elastic Search.
    """
    return dict(
      type = "object",
      enabled = False,
    )
