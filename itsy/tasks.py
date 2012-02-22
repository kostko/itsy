from celery.task import task as celery_task

@celery_task(max_retries = 3)
def cache_resync(source_doc, doc_class, doc_id, fields):
  """
  Performs cache resync for fields that need updating.
  
  @param source_doc: Source document
  @param doc_class: Destination document class
  @param doc_id: Destination document identifier
  @param fields: Fields that need updating
  """
  logger = cache_resync.get_logger()
  
  # Fetch the appropriate document so we can sync it
  try:
    doc = doc_class(pk = doc_id)
  except doc_class.DoesNotExist:
    logger.warning("Failed to find '{0}' with PK '{1}'!".format(doc_class.__name__, doc_id))
    return False
  
  # Resync all fields
  for field_path in fields:
    doc.sync_reference_field(field_path, source_doc)
  
  # Save the document but don't create a snapshot and don't invoke tasks for cached
  # references
  try:
    doc.save(snapshot = False, tasks = { 'reference_cache' : False })
  except doc_class.MutexNotAcquired, e:
    cache_resync.retry(exc = e)

@celery_task()
def cache_spawn_syncers(document, modified_fields):
  """
  A task that is responsible for spawning multiple tasks for syncing
  cached reference fields in individual documents.
  
  @param document: Source document
  @param modified_fields: Fields that have been modified
  """
  for (doc_class, doc_id), fields in document.get_reverse_references(modified_fields).iteritems():
    cache_resync.delay(document, doc_class, doc_id, fields)

@celery_task(max_retries = 3)
def search_index_update(document):
  """
  Updates the search index for the given document.
  
  @param document: Source document
  """
  from .document import DocumentSource
  
  try:
    document.save(target = DocumentSource.Search)
  except Exception, e:
    search_index_update.retry(exc = e)

@celery_task(max_retries = 3)
def search_index_remove(document):
  """
  Removes a document from the search index.
  
  @param document: Document to remove
  """
  try:
    document._meta.search_engine.delete(document._id)
  except Exception, e:
    search_index_remove.retry(exc = e)

@celery_task()
def search_index_reindex(document_cls, offset = 0):
  """
  Performs a complete reindex of documents in the database.

  @param document_cls: Document class to reindex
  @param offset: Document offset
  """
  from .document import DocumentSource

  for document in document_cls.find().order_by("pk").skip(offset):
    try:
      # Reindex the document
      document.save(target = DocumentSource.Search)
    except:
      pass
