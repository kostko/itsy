import hashlib

class SearchAnalyzer(object):
  """
  An abstract search analyzer declaration class.
  """
  def __init__(self):
    """
    Class constructor.
    """
    self.name = self.__class__.__name__.lower()

  def get_properties(self):
    """
    Returns a tuple of properties that uniquely identify this analyzer.
    """
    return (self.name,)

  def get_unique_id(self):
    """
    Returns an identifier that uniquely identifies this analyzer.
    """
    h = hashlib.md5()
    for p in self.get_properties():
      h.update(str(p))
    return "itsy_" + h.hexdigest()

  def __hash__(self):
    """
    Should return a hash value that identifies this analyzer and all of its
    arguments.
    """
    return hash(self.get_properties())

  def __eq__(self, other):
    """
    Should return true when two analyzers are considered equal.
    """
    return self.get_properties() == other.get_properties()

  def serialize(self):
    """
    Serializes this analyzer declaration into a form suitable for
    configuring the Elastic Search index.
    """
    return None

class PatternAnalyzer(SearchAnalyzer):
  """
  The Elastic Search pattern analyzer.
  """
  def __init__(self, pattern, **kwargs):
    """
    Class constructor.

    @param pattern: Pattern to use
    """
    self.pattern = pattern
    super(PatternAnalyzer, self).__init__(**kwargs)

  def get_properties(self):
    """
    Returns a tuple of properties that uniquely identify this analyzer.
    """
    return super(PatternAnalyzer, self).get_properties() + (self.pattern,)

  def serialize(self):
    """
    Serializes this analyzer declaration into a form suitable for
    configuring the Elastic Search index.
    """
    return dict(
      type = "pattern",
      pattern = self.pattern
    )

class CamelCaseAnalyzer(PatternAnalyzer):
  """
  An analyzer that tokenizes CamelCase words.
  """
  def __init__(self):
    """
    Class constructor.
    """
    super(CamelCaseAnalyzer, self).__init__(
      pattern = r"([^\p{L}\d]+)|(?<=\D)(?=\d)|(?<=\d)(?=\D)|(?<=[\p{L}&&[^\p{Lu}]])" \
        r"(?=\p{Lu})|(?<=\p{Lu})(?=\p{Lu}[\p{L}&&[^\p{Lu}]])"
    )
