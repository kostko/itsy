import time

from celery.task import task as celery_task

@celery_task(max_retries = 3)
def cache_resync(source_doc_class, source_doc_id, doc_class, doc_id, fields):
  """
  Performs cache resync for fields that need updating.
  
  @param source_doc_class: Source document class
  @param source_doc_id: Source document class
  @param doc_class: Destination document class
  @param doc_id: Destination document identifier
  @param fields: Fields that need updating
  """
  try:
    source_doc = source_doc_class(pk = source_doc_id)
  except source_doc_class.DoesNotExist:
    return

  try:
    doc = doc_class(pk = doc_id)
  except doc_class.DoesNotExist:
    return

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
def cache_spawn_syncers(doc_class, doc_id, modified_fields):
  """
  A task that is responsible for spawning multiple tasks for syncing
  cached reference fields in individual documents.
  
  @param document: Source document
  @param modified_fields: Fields that have been modified
  """
  try:
    document = doc_class(pk = doc_id)
  except doc_class.DoesNotExist:
    return

  for (d_class, d_id), fields in document.get_reverse_references(modified_fields).iteritems():
    cache_resync.delay(doc_class, doc_id, d_class, d_id, fields)

@celery_task(max_retries = 3)
def search_index_update(doc_class, doc_id):
  """
  Updates the search index for the given document.
  
  @param document: Source document
  """
  from .document import DocumentSource

  try:
    document = doc_class(pk = doc_id)
    document.save(target = DocumentSource.Search)
  except doc_class.DoesNotExist:
    return
  except Exception, e:
    search_index_update.retry(exc = e)

@celery_task(max_retries = 3)
def search_index_remove(doc_class, doc_id):
  """
  Removes a document from the search index.
  
  @param document: Document to remove
  """
  try:
    doc_class._meta.search_engine.delete(doc_id)
  except Exception, e:
    search_index_remove.retry(exc = e)

@celery_task()
def search_index_reindex(document_cls, offset = 0, batch_size = 1000):
  """
  Performs a complete reindex of documents in the database.

  @param document_cls: Document class to reindex
  @param offset: Starting document offset
  """
  while True:
    try:
      count = 0
      for document in document_cls.find().only("pk").order_by("pk").skip(offset).limit(batch_size):
        search_index_update.delay(document_cls, document.pk)
        count += 1
        time.sleep(0.1)

      offset += batch_size
      if count < batch_size:
        break
    except:
      time.sleep(1.0)
