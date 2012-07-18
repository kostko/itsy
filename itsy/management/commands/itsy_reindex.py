import optparse
import traceback

from django.core.management import base as management_base
from django.utils import importlib

from ... import document as itsy_document
from ... import tasks as itsy_tasks

class Command(management_base.BaseCommand):
  args = "class_path"
  help = "Performs a reindex of the given document class."
  requires_model_validation = True
  option_list = management_base.BaseCommand.option_list + (
    optparse.make_option('--background', action = 'store_true', dest = 'background', default = False,
      help = "Should the reindexing be performed by background workers."),

    optparse.make_option('--recreate-index', action = 'store_true', dest = 'recreate-index', default = False,
      help = "Should the index be dropped and recreated. THIS WILL ERASE ALL DATA!"),

    optparse.make_option('--start-pk', dest = 'start-pk', default = "0",
      help = "Start with the specified primary key instead of the first one.")
  )

  def handle(self, *args, **options):
    """
    Performs a reindex of the given document class.
    """
    if len(args) != 1:
      raise management_base.CommandError("Reindex command takes exactly one argument!")

    # Load the specified document class
    class_path = args[0]
    module_name = class_path[:class_path.rfind(".")]
    class_name = class_path[class_path.rfind(".") + 1:]
    module = importlib.import_module(module_name)
    document_class = getattr(module, class_name)

    if not issubclass(document_class, itsy_document.Document):
      raise management_base.CommandError("Specified class is not a valid Document!")

    if not document_class._meta.searchable or document_class._meta.abstract or document_class._meta.embedded:
      raise management_base.CommandError("Specified document is not searchable!")

    if options.get("recreate-index"):
      # Drop the index and recreate it
      self.stdout.write("Recreating index...\n")
      document_class._meta.search_engine.drop()
      document_class._meta.emit_search_mappings()

    if options.get("background"):
      # Spawn the reindex task
      itsy_tasks.search_index_reindex.delay(document_class)

      # Notify the user that the reindex has started in the background
      self.stdout.write("Reindex of %s has been initiated in the background.\n" % class_path)
    else:
      self.stdout.write("Performing foreground reindex of %s...\n" % class_path)
      
      # Modify configuration for bulk indexing (disable index refresh)
      document_class._meta.search_engine.set_configuration({
        "index" : { "refresh_interval" : "-1" } })

      try:
        num_indexed = 0
        last_pk = int(options.get("start-pk", "0"))
        batch_size = 10000
        while True:
          # Assume that primary keys are monotonically incrementing
          self.stdout.write("Starting batch %d at pk=%s.\n" % (num_indexed // batch_size + 1, last_pk))
          old_last_pk = last_pk
          for document in document_class.find(pk__gt = last_pk).order_by("pk").limit(batch_size):
            try:
              document.save(target = itsy_document.DocumentSource.Search)
            except KeyboardInterrupt:
              self.stdout.write("ERROR: Aborted by user.\n")
              raise
            except:
              # Print the exception and continue reindexing
              traceback.print_exc()

            last_pk = document.pk
            num_indexed += 1
            if num_indexed % 100 == 0:
              self.stdout.write("Indexed %d documents.\n" % num_indexed)

          if old_last_pk == last_pk:
            self.stdout.write("Index finished at pk=%s.\n" % last_pk)
      except KeyboardInterrupt:
        pass
      finally:
        # Restore index configuration after indexing
        document_class._meta.search_engine.set_configuration({
          "index" : { "refresh_interval" : "1s" } })

        # Perform index optimization
        self.stdout.write("Optimizing index...\n")
        document_class._meta.search_engine.optimize()

      self.stdout.write("Reindex done.\n")
