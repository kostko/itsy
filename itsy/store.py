from __future__ import absolute_import

import pymongo
import types

def _find_and_modify(self, query = {}, update = None, upsert = False, **kwargs):
  """
  Backported find_and_modify mixin from pymongo 1.10.
  """
  if (not update and not kwargs.get('remove', None)):
    raise ValueError("Must either update or remove")

  if (update and kwargs.get('remove', None)):
    raise ValueError("Can't do both update and remove")

  # No need to include empty args
  if query: kwargs['query'] = query
  if update: kwargs['update'] = update
  if upsert: kwargs['upsert'] = upsert

  no_obj_error = "No matching object found"
  
  out = self._Collection__database.command(
    "findAndModify", self._Collection__name, allowable_errors = [no_obj_error], **kwargs
  )

  if not out['ok']:
    if out["errmsg"] == no_obj_error:
      return None
    else:
      # Should never get here b/c of allowable_errors
      raise ValueError("Unexpected Error: %s"%out)

  return out['value']

class DocumentStore(object):
  """
  A container for MongoDB connections.
  """
  def __init__(self, host, port, database):
    """
    Class constructor.
    
    @param host: Hostname of MongoDB server
    @param port: Port of MongoDB server
    @param database: Database name
    """
    self._db = getattr(pymongo.Connection(host, port), database)

  def collection(self, name, **kwargs):
    """
    Returns the specified MongoDB collection.
    
    @param name: Collection name
    @return: A pymongo.Collection instance
    """
    output = pymongo.collection.Collection(
      self._db,
      name,
      create = False,
      **kwargs
    )
    
    # Mixin backported find_and_modify
    if isinstance(output, pymongo.collection.Collection):
      output.__dict__['find_and_modify'] = types.MethodType(_find_and_modify, output, output.__class__)
    
    return output

