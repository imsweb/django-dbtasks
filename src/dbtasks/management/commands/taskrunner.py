from django.core.management import BaseCommand, CommandParser
from django.tasks import DEFAULT_TASK_BACKEND_ALIAS

from dbtasks.runner import Runner


class Command(BaseCommand):
    help = "Runs the task runner."

    def add_arguments(self, parser: CommandParser):
        parser.add_argument("--workers", default=4, type=int)
        parser.add_argument("--worker-id", default=None)
        parser.add_argument("--backend", default=DEFAULT_TASK_BACKEND_ALIAS)

    def handle(self, *args, **options):
        Runner(
            workers=options["workers"],
            worker_id=options["worker_id"],
            backend=options["backend"],
        ).run()
