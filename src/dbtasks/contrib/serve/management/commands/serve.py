import logging
import os
import platform
import sys
import threading

from django.conf import settings
from django.core.management import BaseCommand
from django.tasks import DEFAULT_TASK_BACKEND_ALIAS

try:
    from granian import Granian
    from granian.constants import Interfaces
except ImportError:
    print("The `serve` command requires Granian. Please install `dbtasks[serve]`.")
    sys.exit(1)

logger = logging.getLogger(__name__)


def cpus() -> int:
    return os.cpu_count() or 4


class Command(BaseCommand):
    help = "Web server and task runner."

    def add_arguments(self, parser):
        default_workers = int(os.getenv("GRANIAN_WORKERS", 1))
        default_threads = int(os.getenv("GRANIAN_BLOCKING_THREADS", max(1, cpus())))
        default_node = platform.node() or "taskrunner"
        default_task_threads = default_threads // 2
        # TODO: can probably be smarter about this...
        default_reload_path = "./src"

        parser.add_argument(
            "-r",
            "--reload",
            nargs="?",
            type=str,
            const=default_reload_path,
            default="",
            help="Reload on changes in specified directory [default=off]",
        )
        parser.add_argument(
            "-w",
            "--workers",
            type=int,
            default=default_workers,
            help=f"Number of worker processes [default={default_workers}]",
        )
        parser.add_argument(
            "-t",
            "--threads",
            type=int,
            default=default_threads,
            help=f"Number of worker threads [default={default_threads}]",
        )
        parser.add_argument(
            "-k",
            "--tasks",
            nargs="?",
            type=int,
            const=cpus() // 2,
            default=0,
            help=f"Number of task runner threads [default={default_task_threads}]",
        )
        parser.add_argument(
            "-i",
            "--worker-id",
            default=default_node,
            help=f"Name of the task runner node [default=`{default_node}`]",
        )
        parser.add_argument(
            "-b",
            "--backend",
            default=DEFAULT_TASK_BACKEND_ALIAS,
            help=f"Task backend to use [default=`{DEFAULT_TASK_BACKEND_ALIAS}`]",
        )
        parser.add_argument(
            "--delay",
            type=float,
            default=0.5,
            help="Task runner loop delay [default=0.5]",
        )
        parser.add_argument(
            "--no-periodic",
            action="store_false",
            default=True,
            dest="periodic",
            help="Do not schedule periodic tasks",
        )
        parser.add_argument(
            "addrport",
            nargs="?",
            default="127.0.0.1:8000",
            help="Optional port number, or ipaddr:port [default=`127.0.0.1:8000`]",
        )

    def on_startup(self):
        if self.runner:
            threading.Thread(target=self.runner.run).start()

    def on_reload(self):
        if self.runner:
            self.runner.reload()

    def on_shutdown(self):
        if self.runner:
            self.runner.stop()

    def handle(self, *args, **options):
        self.runner = None
        if workers := options["tasks"]:
            from dbtasks.runner import Runner

            self.runner = Runner(
                workers=workers,
                worker_id=options["worker_id"],
                backend=options["backend"],
                loop_delay=options["delay"],
                init_periodic=options["periodic"],
            )

        # With no argument, bind to 127.0.0.1 to match runserver, gunicorn, etc.
        # If specifying a port (but no address), bind to 0.0.0.0.
        address = "127.0.0.1"
        port = 8000
        if options["addrport"].isdigit():
            address = "0.0.0.0"
            port = int(options["addrport"])
        elif ":" in options["addrport"]:
            a, p = options["addrport"].rsplit(":", 1)
            address = a or "0.0.0.0"
            port = int(p)
        else:
            address = options["addrport"]

        reload_paths = []
        if path := options["reload"].strip():
            reload_paths.append(path)
            # Granian doesn't log this, AFAICT
            logger.info(f"Watching {path} for changes...")

        server = Granian(
            ":".join(settings.WSGI_APPLICATION.rsplit(".", 1)),
            address=address,
            port=port,
            interface=Interfaces.WSGI,
            workers=options["workers"],
            blocking_threads=options["threads"],
            log_access=True,
            reload=bool(reload_paths),
            reload_paths=reload_paths,
            websockets=False,
        )
        server.on_startup(self.on_startup)
        server.on_reload(self.on_reload)
        server.on_shutdown(self.on_shutdown)
        server.serve()
