from __future__ import absolute_import

import pymongo

class DbResultSet(object):
  """
  Wrapper for lazy evaluation of MongoDB result sets.
  """
  _has_limit = False
  _has_skip = False
  
  def __init__(self, document, spec, cursor = None):
    """
    Class constructor.
    
    @param document: The document class
    @param spec: Filter specification
    @param cursor: Optional existing cursor
    """
    self.document = document
    
    if cursor is not None:
      self.spec = spec
      self.query = cursor
    else:
      self.spec = self._parse_spec(spec)
      self.query = document._meta.collection.find(self.spec)
  
  def _parse_spec(self, spec):
    """
    Parse and transform a query specification.
    """
    from . import fields as db_fields
    
    new_spec = {}
    for key, value in spec.iteritems():
      elements = key.split('__')
      if len(elements) > 1:
        # Process fields through embedded document hierarchy
        rkey = []
        document = self.document
        for element in elements[:-1]:
          if document is not None:
            field = document._meta.get_field_by_name(element)
            rkey.append(field.db_name)
            
            if isinstance(field, db_fields.EmbeddedDocumentField):
              document = field.embedded
            else:
              # We have crossed a non-embedded field so the structure from here
              # on is undefined and database-dependent
              document = None
          else:
            rkey.append(element)
        
        # Discover a potential modifier
        modifier = elements[-1]
        rkey = ".".join(rkey)
        
        if modifier == 'all':
          new_spec[rkey] = { "$all" : value }
        elif modifier == 'in':
          new_spec[rkey] = { "$in" : value }
        else:
          # Not a known modifier, treat as a normal field
          if document is not None:
            modifier = document._meta.get_field_by_name(modifier).db_name
          new_spec["{0}.{1}".format(rkey, modifier)] = value
      else:
        self.document._meta.field_to_data(key, value, new_spec)
    
    return new_spec
  
  def limit(self, limit):
    """
    Limits this result set to some amount of entries.
    
    @param limit: Number of entries to limit to
    """
    if limit is None:
      return self

    self.query = self.query.limit(limit)
    self._has_limit = True
    return self
  
  def skip(self, skip):
    """
    Skips the first entries of this result set.
    
    @param skip: Number of entries to skip
    """
    self.query = self.query.skip(skip)
    self._has_skip = True
    return self
  
  def count(self):
    """
    Returns the number of documents returned by this query.
    """
    return self.query.count(with_limit_and_skip = True)
  
  def _parse_order_spec(self, spec):
    """
    Converts string-based sort order specification into one that
    can be used directly by pymongo.
    """
    if not isinstance(spec, (list, tuple)):
      spec = [spec]
    
    res = []
    for field in spec:
      direction = pymongo.ASCENDING
      if field.startswith('-'):
        direction = pymongo.DESCENDING
        field = field[1:]
      
      field = self.document._meta.get_field_by_name(field).db_name
      res.append((field, direction))
    
    return res
  
  def order_by(self, *fields):
    """
    Orders the result set by a specific field or fields.
    """
    self.query = self.query.sort(self._parse_order_spec(fields))
    return self 
  
  def all(self):
    """
    Clones this result set and returns it.
    """
    rs = DbResultSet(self.model, self.spec, self.query.clone())
    rs._has_limit = self._has_limit
    rs._has_skip = self._has_skip
    return rs
  
  def one(self):
    """
    Returns a single document.
    """
    try:
      return self[0]
    except IndexError:
      raise self.document.DoesNotExist
  
  def ids(self):
    """
    Limits the result to only return identifiers instead of documents.
    """
    return (x["_id"] for x in self.document._meta.collection.find(self.spec, fields = ("_id",)))
  
  def _to_document(self, document):
    """
    Converts a pymongo document dictionary into a valid document object
    instance.
    
    @param document: Document dictionary
    """
    obj = self.document()
    obj._set_from_db(document)
    return obj
  
  def __len__(self):
    """
    An alias for count().
    """
    return self.count()
  
  def __getitem__(self, key):
    """
    Evaluates this result set and returns the specified item.
    """
    if isinstance(key, slice):
      return [self._to_document(x) for x in self.query[key]]
    elif isinstance(key, int):
      return self._to_document(self.query[key])
    else:
      raise TypeError("Indices must be integers or slices!")
  
  def __iter__(self):
    """
    Evaluates this result set.
    """
    for document in self.query:
      yield self._to_document(document)

class SearchResultSet(object):
  """
  Wrapper for Elastic Search result sets.
  """
  def __init__(self, document, query, offset = 0, limit = 35, min_score = None):
    """
    Class constructor.
    
    @param document: Document class
    @param query: A valid pyes.Query
    @param offset: Optional offset in search results
    @param limit: Optional limit in search results
    @param min_score: Optional minimum score
    """
    self.document = document
    query = {
      'query' : query.serialize()
    }
    
    if min_score is not None:
      query['min_score'] = min_score
    
    self._results = document._meta.search_engine.search(
      query,
      **{
        'from'  : offset,
        'size'  : limit
      }
    )
  
  @property
  def total(self):
    """
    Returns the total number of hits.
    """
    return self._results['hits']['total']
  
  def _to_document(self, hit):
    """
    Converts a search result into a document.
    """
    obj = self.document()
    obj._set_from_search(hit)
    return obj
  
  def __iter__(self):
    """
    Iterates over the results.
    """
    for hit in self._results['hits']['hits']:
      yield self._to_document(hit['_source'])

