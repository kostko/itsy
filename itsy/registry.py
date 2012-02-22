
class DocumentRegistry(object):
  def __init__(self):
    self._documents = set()

  def register(self, cls):
    self._documents.add(cls)

  def __iter__(self):
    return iter(self._documents)

document_registry = DocumentRegistry()
