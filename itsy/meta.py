from django.core.exceptions import ImproperlyConfigured

from .connection import store, search

class FieldMetadata(object):
  """
  This class contains field metadata.
  """
  # Field descriptors
  fields = None
  db_fields = None

  def __init__(self):
    """
    Class constructor.
    """
    self.fields = {}
    self.db_fields = {}

  def get_field_by_name(self, name):
    """
    Returns a field instance identified by its name.

    @param name: Field's name
    @return: Field instance
    """
    return self.fields[name]

  def get_field_by_db_name(self, db_name):
    """
    Returns a field instance identified by its database name.

    @param name: Field's database name
    @return: Field instance or None
    """
    return self.db_fields.get(db_name, None)

  def add_field(self, field):
    """
    Adds a new field for this document.
    """
    if field.name in self.fields:
      raise KeyError("Field with name '%s' already exists!" % field.name)
    elif field.db_name in self.db_fields:
      raise KeyError("Field with db_name '%s' already exists!" % field.db_name)

    # TODO this should use ordered dictionary
    self.fields[field.name] = field
    self.db_fields[field.db_name] = field

  def add_field_alias(self, field, alias):
    """
    Sets up an alias for the field. Note that this alias does not behave entirely
    like the original field - you cannot access this field as a document attribute,
    but you can use it in queries etc.

    @param field: Original field
    @param alias: New alias name
    """
    if alias in self.fields:
      raise KeyError("Field with name '%s' already exists!" % alias)

    self.fields[alias] = field

  def field_from_data(self, name, data):
    """
    Extracts a field from database data.

    @param name: Field's name
    @param data: Database data dictionary
    @return: Extracted field value or None
    """
    return data.get(self.get_field_by_name(name).db_name)

  def field_to_data(self, name, value, data):
    """
    Sets a field in database dictionary data.

    @param name: Field's name
    @param value: Field's value
    @param data: Data dictionary
    """
    data[self.get_field_by_name(name).db_name] = value

  def resolve_subfield_hierarchy(self, field_elements):
    """
    Resolves Itsy field hierarchy into a database field hierarchy.

    @param field_elements: Ordered Itsy field names
    @return: Ordered database field names
    """
    db_field = []
    subfields = self
    for element in field_elements:
      if subfields is not None:
        field = subfields.get_field_by_name(element)
        db_field.append(field.db_name)
        subfields = field.get_subfield_metadata()
      else:
        db_field.append(element)

    return db_field

class DocumentMetadata(FieldMetadata):
  """
  This class contains document metadata. 
  """
  # Database
  collection = None
  revisions = None

  # Search
  search_engine = None

  # Reverse references
  reverse_references = None

  def __init__(self, embedded = False, metadata = None):
    """
    Class constructor.
    """
    super(DocumentMetadata, self).__init__()
    self.embedded = embedded

    if metadata is not None:
      self.abstract = metadata.get('abstract', False)
      self.collection_base = metadata.get('collection', None)
      self.index_fields = metadata.get('index_fields', [])
      self.classname = metadata['classname']
      self.searchable = metadata['searchable']
      self.revisable = metadata['revisable']
    else:
      self.abstract = False

    self.field_list = []
    self.reverse_references = []
    self.primary_key_field = None

    if not self.abstract and not self.embedded:
      if not self.collection_base:
        raise ImproperlyConfigured("Collection metadata is required in the model!")

      self.collection = store.collection(self.collection_base)
      self.revisions = store.collection("{0}.revisions".format(self.collection_base))
      self.search_engine = search.index(self.collection_base, self.classname.lower())

  def get_primary_key_field(self):
    """
    Returns the key that is marked as a primary key in this document.
    """
    return self.primary_key_field

  def add_field(self, field):
    """
    Adds a new field for this document.
    """
    super(DocumentMetadata, self).add_field(field)

    if field.primary_key:
      if self.primary_key_field is not None:
        raise ImproperlyConfigured("Only one field can be marked as a primary!")
      elif self.embedded:
        raise ImproperlyConfigured("Embedded documents can't contain primary keys!")
      elif field.db_name != "_id":
        raise ImproperlyConfigured("Primary key's db_name must be _id!")

      self.primary_key_field = field

    if not self.embedded and not self.abstract:
      # Process index for this field
      for ifield, order in field.get_indices().iteritems():
        if ifield == '.':
          self.collection.ensure_index([(field.db_name, order)])
        else:
          self.collection.ensure_index([('{0}.{1}'.format(field.db_name, ifield), order)])

      # Process reverse references
      field.setup_reverse_references(field.cls, field.name)

  def search_mapping_prepare(self):
    """
    Prepares the field mappings for Elastic Search.
    """
    from .fields.base import FieldSearchMapping

    mappings = FieldSearchMapping()
    for name, obj in self.fields.iteritems():
      if obj.searchable:
        mappings[name] = obj.get_search_mapping()
    return mappings

  def emit_search_mappings(self):
    """
    Emits the search mappings.
    """
    if not self.searchable or self.abstract or self.embedded:
      return

    # Prepare mappings according to our document's fields
    mapping = self.search_mapping_prepare()
    mapping.update({
      "_version" : dict(type = "integer", store = "no")
    })

    # Setup index configuration
    analyzers = {}
    tokenizers = {}
    filters = {}
    for a in mapping.analyzers:
      analyzers[a.get_unique_id()] = a.serialize()
      tokenizers.update(a.get_tokenizers())
      filters.update(a.get_filters())

    self.search_engine.set_configuration(dict(
      analysis = dict(
        analyzer = analyzers,
        tokenizer = tokenizers,
        filter = filters
      )
    ))

    # Send mappings to our search engine instance
    self.search_engine.set_mapping(dict(
      dynamic = "strict",
      properties = mapping
    ))
