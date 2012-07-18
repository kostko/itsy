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

  def drop(self):
    """
    Drops the index and removes all data.
    """
    self._es.delete_index_if_exists(self._index)
  
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

  def optimize(self):
    """
    Optimizes the search index.
    """
    return self._es.optimize(indices = [self._index])

  def set_mapping(self, mapping):
    """
    Sets up the field type mapping for this index.
    """
    self._es.create_index_if_missing(self._index)
    self._es.put_mapping(self._type, mapping, [self._index])

  def set_configuration(self, config):
    """
    Sets up the index configuration.
    """
    self._es.create_index_if_missing(self._index)
    try:
      self._es.close_index(self._index)
      self._es.update_settings(self._index, config)
    finally:
      self._es.open_index(self._index)

class DocumentSearch(object):
  """
  A container for Elastic Search connections.
  """
  def __init__(self, servers, index_prefix):
    """
    Class constructor.
    
    @param servers: A list of Elastic Search servers
    @param index_prefix: Index prefix
    """
    self._es = pyes.ES(servers)
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

