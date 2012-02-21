from __future__ import absolute_import

from django.conf import settings

from .store import DocumentStore
from .search import DocumentSearch

# Create a default document store connection
store = DocumentStore(
  settings.ITSY_MONGODB_HOST,
  settings.ITSY_MONGODB_PORT,
  settings.ITSY_MONGODB_DB
)

# Create a default documentsearch connection
search = DocumentSearch(
  settings.ITSY_ELASTICSEARCH_HOST,
  settings.ITSY_ELASTICSEARCH_PORT,
  settings.ITSY_ELASTICSEARCH_INDEX
)

