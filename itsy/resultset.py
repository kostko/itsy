from __future__ import absolute_import

import pymongo

class DbResultSet(object):
  """
  Wrapper for lazy evaluation of MongoDB result sets.
  """
  _has_limit = False
  _has_skip = False
  _only_fields = None
  
  def __init__(self, document, spec, cursor = None):
    """
    Class constructor.
    
    @param document: The document class
    @param spec: Filter specification
    @param cursor: Optional existing cursor
    """
    self.document = document
    self._only_fields = set()
    
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
    operators = ('ne', 'gt', 'gte', 'lt', 'lte', 'in', 'nin', 'mod', 'all', 'size', 'exists', 'not')
    
    new_spec = {}
    for key, value in spec.iteritems():
      elements = key.split('__')
      op = None
      if elements[-1] in operators:
        op = elements.pop()

      field_spec, last_field = self.document._meta.resolve_subfield_hierarchy(elements, get_field = True)
      if last_field is not None:
        # TODO value should be properly prepared
        value = last_field.to_query(value)

      # TODO 

      if op is not None:
        value = { "$%s" % op : value }

      new_spec[".".join(field_spec)] = value
    
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
    rs._only_fields = self._only_fields
    return rs
  
  def one(self):
    """
    Returns a single document.
    """
    try:
      return self[0]
    except IndexError:
      raise self.document.DoesNotExist

  def only(self, *fields):
    """
    Selects a subset of fields to be fetched.
    """
    for field in fields:
      path = ".".join(self.document._meta.resolve_subfield_hierarchy(field.split(".")))
      self._only_fields.add(path)
      if self.query._Cursor__fields is None:
        # Identifier and version fields must always be included
        self.query._Cursor__fields = { "_id" : 1, "_version" : 1 }

      self.query._Cursor__fields.update({ path : 1 })

    return self

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
  def __init__(self, document, query):
    """
    Class constructor.
    
    @param document: Document class
    @param query: A valid pyes.Query
    """
    self._document = document
    self._query = query
    self._offset = None
    self._limit = None
    self._min_score = None
    self._order = None
    self._highlight = None
    self._evaluated = False
    self._results = None

  def all(self):
    """
    Clones this result set.
    """
    rs = SearchResultSet(self._document, self._query)
    rs._offset = self._offset
    rs._limit = self._limit
    rs._min_score = self._min_score
    rs._highlight = self._highlight
    rs._order = self._order
    rs._evaluated = self._evaluated
    rs._results = self._results
    return rs

  def _evaluate(self):
    """
    Evaluates this result set if it hasn't yet been evaluated.
    """
    if not self._evaluated:
      query = { 'query' : self._query.serialize() }
      if self._min_score is not None:
        query['min_score'] = self._min_score
      if self._highlight is not None:
        query['highlight'] = self._highlight
      if self._order is not None:
        query['sort'] = self._order

      params = {}
      if self._offset is not None:
        params['from'] = int(self._offset)
      if self._limit is not None:
        params['size'] = int(self._limit)

      self._results = self._document._meta.search_engine.search(
        query,
        **params
      )
      self._evaluated = True

    return self._results

  def limit(self, limit):
    """
    Limits this result set to some amount of entries.

    @param limit: Number of entries to limit to
    """
    self._evaluated = False
    self._limit = limit
    return self

  def skip(self, skip):
    """
    Skips the first entries of this result set.

    @param skip: Number of entries to skip
    """
    self._evaluated = False
    self._offset = skip
    return self

  def min_score(self, score):
    """
    Sets up the minimum score for the documents to be included in the result
    set.

    @param score: Minimum score
    """
    self._evaluated = False
    self._min_score = score
    return self

  def highlight(self, **highlight):
    """
    Sets up the highlight descriptor.
    """
    self._evaluated = False
    self._highlight = highlight
    return self

  def order_by(self, *fields):
    """
    Orders the result set by a specific field or fields.
    """
    self._evaluated = False
    if self._order is None:
      self._order = []

    for field in fields:
      direction = "asc"
      if field.startswith('-'):
        direction = "desc"
        field = field[1:]

      self._order.append({ field : direction })

    return self

  @property
  def total(self):
    """
    Returns the total number of hits.
    """
    return self._evaluate()['hits']['total']

  def _to_document(self, hit):
    """
    Converts a search result into a document.
    """
    obj = self._document()
    obj._set_from_search(hit['_source'], hit.get('highlight'))
    return obj
  
  def __iter__(self):
    """
    Iterates over the results.
    """
    for hit in self._evaluate()['hits']['hits']:
      yield self._to_document(hit)

