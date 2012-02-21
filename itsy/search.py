from __future__ import absolute_import

import pyes

class DocumentSearchIndex(object):
  """
  An Elastic Search index object wrapper.
  """
  def __init__(self, es, index, typ):
    """
    Class constructor.
    
    @param es: Elastic Search handle
    @param index: Index name
    @param typ: Document type
    """
    self._es = es
    self._index = index
    self._type = typ
  
  def index(self, document):
    """
    Indexes a given document.
    """
    self._es.index(document, self._index, self._type, document['_id'])
  
  def refresh(self):
    """
    Refreshes the index.
    """
    self._es.refresh([self._index])
  
  def delete(self, doc_id):
    """
    Deletes a document from the index.
    """
    self._es.delete(self._index, self._type, doc_id)
  
  def search(self, query, **kwargs):
    """
    Performs a search over this index.
    """
    return self._es.search_raw(
      query,
      indices = [self._index],
      doc_types = [self._type],
      **kwargs
    )

  def set_mapping(self, mapping):
    """
    Sets up the field type mapping for this index.
    """
    self._es.create_index_if_missing(self._index)
    self._es.put_mapping(self._type, mapping, [self._index])

class DocumentSearch(object):
  """
  A container for Elastic Search connections.
  """
  def __init__(self, host, port, index_prefix):
    """
    Class constructor.
    
    @param host: Hostname of Elastic Search server
    @param port: Port of Elastic Search server
    @param index_prefix: Index prefix
    """
    self._es = pyes.ES("{0}:{1}".format(host, port))
    self._index_prefix = index_prefix
  
  def index(self, name, typ):
    """
    Returns a wrapper for performing Elastic Search operations on a
    specific index.
    
    @param name: Index name
    @param typ: Document type
    @return: Elastic Search operations wrapper
    """
    name = "{0}.{1}".format(self._index_prefix, name)
    return DocumentSearchIndex(self._es, name, typ)

