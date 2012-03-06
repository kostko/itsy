from __future__ import absolute_import

import copy
import datetime
import random
import pymongo

from django.core.exceptions import ImproperlyConfigured

from . import exceptions, signals, registry
from . import tasks as common_tasks
from .connection import store, search
from .resultset import DbResultSet, SearchResultSet

# Tasks to invoke by default when saving a document
DOCUMENT_DEFAULT_TASKS = {
  'reference_cache' : True,
  'search_indices'  : True
}

# Possible resolutions when referenced documents get deleted
CASCADE = 1
RESTRICT = 2

class DocumentSource:
  """
  Document source.
  """
  Db = 1
  Search = 2

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
    mappings = {}
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

    # Send mappings to our search engine instance
    self.search_engine.set_mapping(dict(
      dynamic = "strict",
      properties = mapping
    ))

class MetaDocument(type):
  """
  Meta class for generating document classes.
  """
  def __new__(meta, classname, bases, attrs):
    """
    Constructs a new document type.
    """
    if classname == "Document":
      return type.__new__(meta, classname, bases, attrs)
    
    # Inject exceptions
    attrs['DoesNotExist'] = exceptions.DoesNotExist
    attrs['MissingVersionMetadata'] = exceptions.MissingVersionMetadata
    attrs['MutexNotAcquired'] = exceptions.MutexNotAcquired
    
    # Merge metadata dictionary copying defaults from Document class
    m = { 'classname' : classname }
    m.update(Document.Meta.__dict__)
    m.update(attrs['Meta'].__dict__)
    del attrs['Meta']
    attrs['_meta'] = _meta = DocumentMetadata(metadata = m)
    
    # Pre-process fields
    from . import fields as db_fields
    fields = []
    for name, obj in attrs.items():
      if isinstance(obj, db_fields.Field):
        fields.append((name, obj))
        del attrs[name]
    
    # Include fields from parent abstract classes (if any)
    for base in bases:
      if issubclass(base, Document) and base != Document:
        fields += copy.deepcopy(base._meta.fields.items())
    
    # Create the actual type
    new_class = type.__new__(meta, classname, bases, attrs)
    if not _meta.abstract:
      if _meta.revisable:
        new_class._meta.revisions.ensure_index([("doc", pymongo.ASCENDING)])
      new_class._meta.collection.ensure_index([("_id", pymongo.ASCENDING), ("_version", pymongo.ASCENDING)])
    
    # Process document fields
    for name, obj in fields:
      obj.contribute_to_class(new_class, name)

    # Ensure that there is a primary key if none has been created
    if not _meta.abstract and not _meta.embedded:
      pkey_field = _meta.get_primary_key_field()
      if pkey_field is None:
        pkey_field = db_fields.SerialField(primary_key = True)
        pkey_field.contribute_to_class(new_class, "pk")
      else:
        setattr(new_class, "pk", pkey_field)
        _meta.add_field_alias(pkey_field, "pk")

    for name, obj in fields:
      obj.check_configuration()

    # Process composite indices
    if _meta.index_fields is not None and not _meta.abstract:
      for index_spec in _meta.index_fields:
        db_index_spec = []
        for field_path in index_spec:
          if field_path[0] == '-':
            order = Document.DESCENDING
            field_path = field_path[1:]
          else:
            order = Document.ASCENDING

          db_index_spec.append((".".join(_meta.resolve_subfield_hierarchy(field_path.split("."))), order))

        _meta.collection.ensure_index(db_index_spec)
    
    signals.document_prepared.send(sender = new_class)

    # Register the class in the document registry
    if not _meta.abstract:
      registry.document_registry.register(new_class)

    return new_class

class MetaEmbeddedDocument(type):
  """
  Meta class for generating embedded document classes.
  """
  def __new__(meta, classname, bases, attrs):
    """
    Constructs a new document type.
    """
    if classname == "EmbeddedDocument":
      return type.__new__(meta, classname, bases, attrs)
    
    attrs['_meta'] = DocumentMetadata(embedded = True)
    
    # Pre-process fields
    from . import fields as db_fields
    fields = []
    for name, obj in attrs.items():
      if isinstance(obj, db_fields.Field):
        fields.append((name, obj))
        del attrs[name]
    
    # Create the actual type
    new_class = type.__new__(meta, classname, bases, attrs)
    
    # Process document fields
    for name, obj in fields:
      obj.contribute_to_class(new_class, name)
    
    return new_class

class BaseDocument(object):
  """
  Abstract base document with common functionality for standalone and
  embedded documents.
  """
  # Sort order constants
  ASCENDING = 1
  DESCENDING = -1
  
  def __init__(self, **kwargs):
    """
    Class constructor.
    """
    self._values = {}
    self._reference_fields = {}

    # Handle additional arguments to constructor the same way as one would set attributes
    # on the document instance after it is instantiated
    for key, value in kwargs.iteritems():
      try:
        self._meta.get_field_by_name(key)
        setattr(self, key, value)
      except KeyError:
        raise KeyError("Field '%s' not found in document '%s'!" % (key, self.__class__.__name__))
  
  def __setattr__(self, name, value):
    """
    Raise errors when attempting to create new undefined attributes.
    """
    if name.startswith('_') or name in self._meta.fields:
      return super(BaseDocument, self).__setattr__(name, value)
    
    raise AttributeError("Field '{0}' does not exist!".format(name))
  
  def __getstate__(self):
    """
    Returns state for serialization.
    """
    values = {}
    for field, value in self._values.iteritems():
      values[field.name] = value
    
    return values
  
  def __setstate__(self, state):
    """
    Sets up state from serialized data.
    """
    self._values = {}
    self._reference_fields = {}
    
    values = state
    for name, value in values.iteritems():
      self._values[self._meta.fields.get(name)] = value
  
  def _set_from_db(self, data):
    """
    Sets up this document by populating it with data obtained from
    MongoDB database.
    
    @param data: Data dictionary
    """
    self._values.clear()
    self._reference_fields.clear()
    for key, value in data.iteritems():
      field = self._meta.get_field_by_db_name(key)
      if field is not None and value is not None:
        self._values[field] = field.from_store(value, self)
  
  def _set_from_search(self, data):
    """
    Sets up this document by populating it with data obtained from
    Elastic Search.
    
    @param data: Data dictionary
    """
    self._values.clear()
    for key, value in data.iteritems():
      field = self._meta.fields.get(key)
      if field is not None:
        self._values[field] = field.from_search(value, self)
  
  def _db_prepare(self, fields = None, db_names = True, update = False):
    """
    Prepares the document for saving into the database.
    
    @param fields: Subset of fields to prepare for
    """
    document = {}
    
    for name, field in self._meta.fields.iteritems():
      # Skip fields that are not meant to be saved into the database; they might
      # be virtual or only be meant for search indices
      if field.virtual:
        continue
      
      # If only a subset of fields is desired check that this one is in
      if fields is not None and name not in fields:
        continue

      value = self._values.get(field)
      if not field.no_pre_save:
        value = field.pre_save(value, self, update = update)
      field._validate(value, self)
      self._values[field] = value
      
      fname = field.db_name if db_names else field.name
      if value is not None:
        document[fname] = field.to_store(value, self)
      else:
        document[fname] = None
    
    return document
  
  def _search_prepare(self):
    """
    Prepares the document for saving into the search index.
    """
    document = {}
    
    for name, field in self._meta.fields.iteritems():
      if not field.searchable:
        continue
      
      value = self._values.get(field)
      if value is not None or field.virtual:
        document[field.name] = field.to_search(value, self)
    
    return document

  def _db_post_save(self):
    """
    Performs post-save actions on the document.
    """
    for name, field in self._meta.fields.iteritems():
      value = self._values.get(field)
      if value is not None:
        field.post_save(value, self)
  
  def get_top_level_document(self):
    """
    Returns the top-level document.
    """
    if hasattr(self, '_parent'):
      return self._parent.get_top_level_document()
    
    return self

class Document(BaseDocument):
  """
  Abstract document combines MongoDB models and Elastic Search schema
  definitions with a revision system.
  """
  __metaclass__ = MetaDocument
  
  # Default metadata
  class Meta:
    # MongoDB collection name containing the model
    collection = None
    
    # Additional indexes to create
    index_fields = None
    
    # Is this document an abstract one
    abstract = False
    
    # Should this document be made searchable
    searchable = True

    # Should this document have revisions
    revisable = True
  
  def __init__(self, **kwargs):
    """
    Class constructor.
    """
    if self._meta.abstract:
      raise ValueError("Unable to instantiate an abstract document '{0}'!".format(self.__class__.__name__))
    
    super(Document, self).__init__(**kwargs)

    # Initialize version to None, as this is a new document
    self._version = None

  def __getstate__(self):
    """
    Returns state for serialization.
    """
    super_state = super(Document, self).__getstate__()
    return self.pk, self._version, super_state
  
  def __setstate__(self, state):
    """
    Sets up state from serialized data.
    """
    self.pk, self._version, super_state = state
    super(Document, self).__setstate__(super_state)
    self._document_source = DocumentSource.Db
  
  def _set_from_db(self, data):
    """
    Sets up this document by populating it with data obtained from
    MongoDB database.
    
    @param data: Data dictionary
    """
    try:
      self._version = data['_version']
    except KeyError:
      raise exceptions.MissingVersionMetadata
    
    super(Document, self)._set_from_db(data)
    self._document_source = DocumentSource.Db
  
  def _set_from_search(self, data, highlight = None):
    """
    Sets up this document by populating it with data obtained from
    Elastic Search. The data obtained in this way may be incomplete, since
    some fields may not be indexed or have been transformed for search
    purpuses.
    
    Access to database instance is available by calling `refresh`.
    
    @param data: Data dictionary
    @param highlight: Optional highlight metadata
    """
    super(Document, self)._set_from_search(data)
    self._document_source = DocumentSource.Search
    self._highlight = highlight

  def get_highlighting(self):
    """
    Returns highlighting metadata for search results.
    """
    return self._highlight
  
  def is_persistent(self):
    """
    Returns true if this document is persisted into the database (has a set
    primary key). This does not mean that the document actually exists at this
    moment, just that it did when it was fetched.
    """
    return self.pk is not None

  def is_search_result(self):
    """
    Returns true if this document is a search result.
    """
    return self._document_source == DocumentSource.Search

  def refresh(self):
    """
    Refreshes this object from the database. This will reset any
    modification made to it.
    """
    document = self._meta.collection.find_one({ "_id" : self._pk_for_db() })
    if document is None:
      raise exceptions.DoesNotExist
    
    self._set_from_db(document)
  
  def save(self, snapshot = True, tasks = None, author = None, target = DocumentSource.Db):
    """
    Saves the document, potentially creating a new revision.

    @param snapshot: True if a snapshot should be made (when the document is revisable)
    @param tasks: None for default tasks, False for no tasks and dictionary for selective tasks
    @param author: Author metadata
    @param target: Where to save the document (storage, search)
    """
    _tasks = copy.deepcopy(DOCUMENT_DEFAULT_TASKS)
    if tasks is None:
      tasks = _tasks
    elif tasks is False:
      tasks = {}
    else:
      _tasks.update(tasks)
      tasks = _tasks

    if target == DocumentSource.Db:
      self._save_to_db(snapshot, tasks, author)
    elif target == DocumentSource.Search:
      self._save_to_search()
  
  def _modified_fields(self, old_document, document):
    """
    Returns names of all fields that have been modified between versions and
    are valid fields.
    """
    fields = set()
    for field in self._meta.fields.values():
      if document.get(field.db_name) != old_document.get(field.db_name):
        fields.add(field.name)
    
    return fields
  
  def _save_to_db(self, snapshot, tasks, author):
    """
    Saves the document into MongoDB, creating a new document revision.
    
    @param snapshot: Should a snapshot of the current version be saved
    @param tasks: Tasks that should be invoked
    @param author: Author metadata
    """
    if not self._values and self.pk is not None:
      return

    is_update = self._version is not None
    document = self._db_prepare(update = is_update)
    if not document:
      return
    
    if is_update:
      # Remove _id from document
      del document["_id"]

      # An existing document is being updated, first create a snapshot and
      # acquire the document update mutex
      old_document = self._lock(snapshot)
      
      # Commit the document, incrementing version and releasing the update mutex
      document = { '$set' : document }
      document['$inc'] = { '_version' : 1 }
      document['$set']['_mutex'] = datetime.datetime.utcnow() - datetime.timedelta(hours = 1)
      document['$set']['_last_update'] = datetime.datetime.utcnow()
      document['$set']['_last_author'] = author
      self._meta.collection.update(
        { "_id" : self._pk_for_db() },
        document,
        safe = True
      )
      self._version += 1
      
      # Dispatch update tasks
      self.dispatch_update_tasks(tasks, self._modified_fields(old_document, document['$set']))
    else:
      # A new document is being inserted
      document['_version'] = 1
      document['_mutex'] = datetime.datetime.utcnow() - datetime.timedelta(hours = 1)
      document['_last_update'] = datetime.datetime.utcnow()
      document['_last_author'] = author
      new_pk = self._meta.collection.insert(document, safe = True)
      if new_pk is not None:
        self.pk = self._meta.get_primary_key_field().from_store(new_pk, self)
      self._version = 1
    
      # Dispatch update tasks
      tasks.update({ 'reference_cache' : False })
      self.dispatch_update_tasks(tasks, [])
    
    self._document_source = DocumentSource.Db
    self._db_post_save()
  
  def _lock(self, snapshot = True):
    """
    Creates a snapshot of the current document and places it into a
    new revision. This operation will also acquire the editorial mutex
    on the document and will fail when such a mutex cannot be acquired.
    
    @param snapshot: True to create a snapshot, False to just acquire a mutex
    @return: Current version of the document
    """
    pk = self._pk_for_db()

    # Fetch existing document from database and acquire the edit mutex
    now = datetime.datetime.utcnow()
    document = self._meta.collection.find_and_modify(
      { "_id" : pk, "_mutex" : { "$lt" : now }, "_version" : self._version },
      { "$set" : { "_mutex" : now + datetime.timedelta(seconds = 30) } }
    )
    if not document:
      raise exceptions.MutexNotAcquired
    
    if snapshot and self._meta.revisable:
      # Only copy revisable fields to our document revision
      snapshot = {}
      for name, field in self._meta.fields.iteritems():
        if field.revisable and document.get(field.db_name):
          snapshot[field.db_name] = field.to_revision(document[field.db_name], self)
      
      # Create a new revision for the specified version
      self._meta.revisions.update(
        { "_id" : "{0}.{1}".format(self.pk, self._version) },
        {
          "_id" : "{0}.{1}".format(self.pk, self._version),
          "doc" : pk,
          "version" : self._version,
          "created" : document['_last_update'],
          "author" : document['_last_author'],
          "document" : snapshot,
        },
        upsert = True,
        safe = True
      )
    
    return document
  
  def dispatch_update_tasks(self, tasks, modified_fields):
    """
    Dispatches tasks that will update external documents and search indices in
    the background.
    
    @param tasks: Which tasks should be invoked
    @param modified_fields: Fields that have been modified
    """
    if tasks.get('reference_cache', False):
      # Dispatch task for syncing the cached references
      common_tasks.cache_spawn_syncers.delay(self.__class__, self.pk, modified_fields)
    
    if tasks.get('search_indices', False) and self._meta.searchable:
      # Dispatch task for updating search indices
      common_tasks.search_index_update.delay(self.__class__, self.pk)
  
  def revert(self, version, author = None):
    """
    Reverts to a previous version of this document.
    
    @param version: Version to revert to
    @param author: Author metadata
    """
    if not self._meta.revisable:
      return

    # TODO
    pass

  def get_search_boost(self):
    """
    This method may be overriden to compute a boosting value for this
    document.
    """
    return 1.0
  
  def _save_to_search(self):
    """
    Saves the document into Elastic Search.
    """
    if self.pk is None:
      raise exceptions.DocumentNotSaved
    
    if not self._meta.searchable:
      return
    
    if self._document_source != DocumentSource.Db:
      self.refresh()

    document = self._search_prepare()
    document['_id'] = document["pk"]
    document['_version'] = self._version
    document['_boost'] = float(self.get_search_boost())

    self._meta.search_engine.index(document)

  def _pk_for_db(self, search = False):
    """
    Returns a properly encoded primary key so that it can be used directly with
    MongoDB (or Elastic Search) driver.

    @param search: Should the field be formatted for ES instead
    """
    if self.pk is None:
      return None

    if not search:
      return self._meta.get_primary_key_field().to_store(self.pk, self)
    else:
      return self._meta.get_primary_key_field().to_search(self.pk, self)

  def _check_delete_restrict(self):
    """
    Check if there are any reverse references that prevent deletion of this
    document.
    """
    cascade_documents = []
    for doc_class, field_path, field in self._meta.reverse_references:
      documents = doc_class.find(**{ field_path.replace('.', '__') : self.pk })
      if field.on_delete == RESTRICT and documents.count() > 0:
        raise exceptions.DeleteRestrictedByReference
      elif field.on_delete == CASCADE:
        for document in documents:
          document._check_delete_restrict()
          cascade_documents.append(document)
    
    return cascade_documents
  
  def delete(self):
    """
    Deletes this document.
    """
    cascade_documents = self._check_delete_restrict()
    pk = self._pk_for_db()
    
    # Acquire the editorial mutex before deleting this document
    self._lock(False)
    self._meta.collection.remove(pk, safe = True)
    if self._meta.revisable:
      self._meta.revisions.remove({ "doc" : pk }, safe = True)
    if self._meta.searchable:
      common_tasks.search_index_remove.delay(self.__class__, self._pk_for_db(search = True))
    
    # Delete all referenced documents
    for document in cascade_documents:
      document.delete()

  @classmethod
  def find(cls, **criteria):
    """
    Used to search the document collection.
    """
    return DbResultSet(cls, criteria)

  @classmethod
  def get(cls, **criteria):
    """
    Retrieves a single document matching some criteria.
    """
    return cls.find(**criteria).one()

  @classmethod
  def get_or_create(cls, **criteria):
    """
    Retrieves a document matching some criteria or creates a new one.
    """
    try:
      return cls.find(**criteria).one()
    except cls.DoesNotExist:
      doc = cls(**criteria)
      return doc
  
  @classmethod
  def find_es(cls, query, **kwargs):
    """
    Used to search via Elastic Search.
    
    @param query: pyes.Query instance
    """
    return SearchResultSet(cls, query, **kwargs)
  
  def sync_reference_field(self, path, document):
    """
    Syncs a referenced document field that is identified by its path in
    the embedded document hierarchy.
    """
    for ref in self._reference_fields.get("{0}/{1}".format(path, document.pk), []):
      ref.sync(document)
  
  def get_reverse_references(self, modified_fields):
    """
    Returns a dictionary of reverse referenced document identifiers.
    """
    refs = {}
    
    for doc_class, field_path, field in self._meta.reverse_references:
      # Check if any fields for this reference have actually changed
      if modified_fields is not None and not set(field.dependencies).intersection(modified_fields):
        continue
      
      # Attempt to find referencing documents for the given identifier
      for doc_id in doc_class.find(**{ field_path.replace('.', '__') : self.pk }).ids():
        refs.setdefault((doc_class, doc_id), []).append(field_path)
    
    return refs

class EmbeddedDocument(BaseDocument):
  """
  Abstract embedded document.
  """
  __metaclass__ = MetaEmbeddedDocument

