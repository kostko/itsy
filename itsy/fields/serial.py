from .base import IntegerField
from ..connection import store as db_store

__all__ = [
  "SerialField",
]

class SerialField(IntegerField):
  """
  An integer field that automatically generates monotonically incrementing
  numbers for new documents. Can be used as a primary key.
  """
  def __init__(self, counters_collection = "counters", **kwargs):
    """
    Class constructor.

    @param counters_collection: Optional name of the collection holding the counters
    """
    self.counters_collection = counters_collection
    super(SerialField, self).__init__(**kwargs)

  def pre_save(self, value, document, update = False):
    """
    Generates a new unique sequence value.
    """
    if update:
      return

    # Allocate a new identifier
    return db_store.collection(self.counters_collection).find_and_modify(
      { "_id" : "{0}.{1}".format(self.cls._meta.collection_base, self.name) },
      { "$inc" : { "next" : 1 } },
      new = True,
      upsert = True
    )["next"]

