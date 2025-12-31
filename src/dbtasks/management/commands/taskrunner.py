import os
import platform

from django.core.management import BaseCommand, CommandParser
from django.tasks import DEFAULT_TASK_BACKEND_ALIAS

from dbtasks.runner import Runner


def cpus() -> int:
    return os.cpu_count() or 4


class Command(BaseCommand):
    help = "Runs the task runner."

    def add_arguments(self, parser: CommandParser):
        default_cpus = max(1, cpus() - 1)
        default_node = platform.node() or "taskrunner"
        parser.add_argument(
            "-w",
            "--workers",
            type=int,
            default=default_cpus,
            help=f"Number of worker threads [default={default_cpus}]",
        )
        parser.add_argument(
            "-i",
            "--worker-id",
            default=default_node,
            help=f"Name of the worker node [default=`{default_node}`]",
        )
        parser.add_argument(
            "--backend",
            default=DEFAULT_TASK_BACKEND_ALIAS,
            help=f"Task backend to use [default=`{DEFAULT_TASK_BACKEND_ALIAS}`]",
        )
        parser.add_argument(
            "--delay",
            type=float,
            default=1.0,
            help="Loop delay [default=1.0]",
        )
        parser.add_argument(
            "--no-periodic",
            action="store_false",
            default=True,
            dest="periodic",
        )

    def handle(self, *args, **options):
        Runner(
            workers=options["workers"],
            worker_id=options["worker_id"],
            backend=options["backend"],
            loop_delay=options["delay"],
            init_periodic=options["periodic"],
        ).run()
