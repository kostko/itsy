from __future__ import absolute_import

from . import signals
from .document import Document

pending_callbacks = {}
registered_documents = {}

def track(field, document, callback):
  """
  Registers a callback that is invoked when the passed in document has been
  loaded.
  
  @param field: Field calling this function
  @param document: Document class or string containing the class name
  @param callback: Callback to be invoked
  """
  if isinstance(document, str):
    # Lazy tracking, wait for the document to be imported
    if document == 'self':
      callback(field.cls)
    elif document in registered_documents:
      callback(registered_documents[document])
    else:
      pending_callbacks.setdefault(document, []).append(callback)
  elif issubclass(document, Document):
    # Document is already resolved, invoke the callback
    callback(document)

def _dispatch_callbacks(sender, **kwargs):
  """
  Dispatches any pending callbacks for the specified document
  """
  for callback in pending_callbacks.pop(sender.__name__, []):
    callback(sender)
  
  registered_documents[sender.__name__] = sender

signals.document_prepared.connect(_dispatch_callbacks)

