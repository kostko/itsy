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
    self.filters = {}
    self.tokenizers = {}

  def add_filter(self, **kwargs):
    """
    Creates a new filter declaration and adds it as a dependency of
    this analyzer.
    """
    name = self.get_unique_id(typ = "filter%d" % len(self.filters))
    self.filters[name] = kwargs
    return name

  def get_filters(self):
    """
    Returns filter dependencies.
    """
    return self.filters

  def add_tokenizer(self, **kwargs):
    """
    Creates a new tokenizer declaration and adds it as a dependency of
    this analyzer.
    """
    name = self.get_unique_id(typ = "tokenizer%d" % len(self.tokenizers))
    self.tokenizers[name] = kwargs
    return name

  def get_tokenizers(self):
    """
    Returns tokenizer dependencies.
    """
    return self.tokenizers

  def get_properties(self):
    """
    Returns a tuple of properties that uniquely identify this analyzer.
    """
    return (self.name,)

  def get_unique_id(self, typ = ""):
    """
    Returns an identifier that uniquely identifies this analyzer.
    """
    h = hashlib.md5()
    h.update(typ)
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

class ExactTermAnalyzer(SearchAnalyzer):
  """
  An analyzer that can be used for exact matches. It emits the whole field
  content as a single token and performs some transformations (lowercasing,
  ASCII folding, non-word character replacement) on it.
  """
  def serialize(self):
    """
    Serializes this analyzer declaration into a form suitable for
    configuring the Elastic Search index.
    """
    return dict(
      type = "custom",
      tokenizer = "keyword",
      filter = [
        # Lowercase the string
        "lowercase",

        # Perform ASCII folding of unicode characters
        "asciifolding",

        # Replace non-word characters with spaces
        self.add_filter(
          type = "pattern_replace",
          pattern = r"[^\w\s]",
          replacement = " "
        ),

        # Replace multiple spaces with a single space
        self.add_filter(
          type = "pattern_replace",
          pattern = r"\s+",
          replacement = " "
        )
      ]
    )
