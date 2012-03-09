import re

from .base import Field
from ..search.analyzer import ExactTermAnalyzer

__all__ = [
  "SearchCompositeField",
  "SearchCopyField",
  "SearchExactField"
]

class SearchCompositeField(Field):
  """
  Field that enables composition of other fields into text strings for
  purpuses of search persistance.
  """
  def __init__(self, composition, **kwargs):
    """
    Class constructor.
    """
    self.composition = composition
    kwargs['virtual'] = True
    kwargs['revisable'] = False
    kwargs['searchable'] = True
    super(SearchCompositeField, self).__init__(**kwargs)

  def from_search(self, value, document):
    """
    Converts value from Elastic Search store.
    """
    return value

  def __get__(self, obj, typ):
    """
    Override descriptor's get method to always return a precomposed value.
    """
    return self.composition.format(**{ 'self' : obj })

  def to_search(self, value, document):
    """
    Converts value to Elastic Search store.
    """
    return self.__get__(document, None)

  def get_search_mapping(self):
    """
    Returns field mapping for Elastic Search.
    """
    mapping = super(SearchCompositeField, self).get_search_mapping()
    mapping.update(dict(
      type = "string",
      index = "analyzed" if self.search_index.get("analyzed", True) else "not_analyzed",
      ))

    if self.search_index.get("analyzer", None) is not None:
      analyzer = self.search_index["analyzer"]
      mapping["analyzer"] = analyzer.get_unique_id()
      mapping.analyzers.add(analyzer)

    return mapping

class SearchCopyField(Field):
  """
  A virtual field that stores a copy of an existing (string) field.
  """
  def __init__(self, copy_from, **kwargs):
    """
    Class constructor.
    """
    self.copy_from = copy_from
    kwargs['virtual'] = True
    kwargs['revisable'] = False
    kwargs['searchable'] = True
    super(SearchCopyField, self).__init__(**kwargs)

  def from_search(self, value, document):
    """
    Converts value from Elastic Search store.
    """
    return value

  def __get__(self, obj, typ):
    """
    Override descriptor's get method to always return a precomposed value.
    """
    return unicode(getattr(obj, self.copy_from))

  def to_search(self, value, document):
    """
    Converts value to Elastic Search store.
    """
    return self.__get__(document, None)

  def get_search_mapping(self):
    """
    Returns field mapping for Elastic Search.
    """
    mapping = super(SearchCopyField, self).get_search_mapping()
    mapping.update(dict(
      type = "string",
      index = "analyzed" if self.search_index.get("analyzed", True) else "not_analyzed",
    ))

    if self.search_index.get("analyzer", None) is not None:
      analyzer = self.search_index["analyzer"]
      mapping["analyzer"] = analyzer.get_unique_id()
      mapping.analyzers.add(analyzer)

    return mapping

class SearchExactField(SearchCopyField):
  """
  A virtual field that stores a copy of an existing (string) field analyzed using
  the ExactTermAnalyzer.
  """
  def __init__(self, copy_from, **kwargs):
    """
    Class constructor.
    """
    search_index = dict(
      analyzed = True,
      analyzer = ExactTermAnalyzer()
    )
    search_index.update(kwargs.get('search_index', {}))
    super(SearchExactField, self).__init__(copy_from, search_index = search_index, **kwargs)
