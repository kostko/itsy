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
  class ManualValue(object):
    """
    A wrapper that ignores the validation with manually setting a value for
    a SerialField. Should be used in the following way:

      >>> doc.pk = SerialField.ManualValue(123)

    This is done in this way to prevent accidental overwrites of serial
    values.
    """
    def __init__(self, pk):
      """
      Class constructor.

      @param pk: Manual value for the serial field
      """
      self.pk = int(pk)

  def __init__(self, counters_collection = "counters", **kwargs):
    """
    Class constructor.

    @param counters_collection: Optional name of the collection holding the counters
    """
    self.counters_collection_name = counters_collection
    self.counters_collection = db_store.collection(counters_collection)
    super(SerialField, self).__init__(**kwargs)

  def set_counter(self, value):
    """
    Sets the collection counter for this serial field to a specific value.
    """
    self.counters_collection.update(
      { "_id" : "{0}.{1}".format(self.cls._meta.collection_base, self.name) },
      { "$set" : { "next" : int(value) } },
      upsert = True
    )

  def pre_save(self, value, document, update = False):
    """
    Generates a new unique sequence value.
    """
    if update:
      return value
    elif value is not None:
      if not isinstance(value, SerialField.ManualValue):
        raise ValueError("Manually setting the value of a SerialField on insert may cause conflicts in the "
          "future! Use of IntegerField is recommended, or wrap value in SerialField.ManualValue!")

      return value.pk

    # Allocate a new identifier
    return self.counters_collection.find_and_modify(
      { "_id" : "{0}.{1}".format(self.cls._meta.collection_base, self.name) },
      { "$inc" : { "next" : 1 } },
      new = True,
      upsert = True
    )["next"]

