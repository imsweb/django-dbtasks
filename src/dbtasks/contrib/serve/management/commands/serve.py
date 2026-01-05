import logging
import os
import platform
import sys
import threading

from django.conf import settings
from django.core.management import BaseCommand
from django.tasks import DEFAULT_TASK_BACKEND_ALIAS

from dbtasks.defaults import DEFAULT_RUNNER_LOOP_DELAY

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
            const=default_task_threads,
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
            default=DEFAULT_RUNNER_LOOP_DELAY,
            help=f"Task runner loop delay [default={DEFAULT_RUNNER_LOOP_DELAY}]",
        )
        parser.add_argument(
            "--no-periodic",
            action="store_false",
            default=True,
            dest="periodic",
            help="Do not schedule periodic tasks",
        )
        parser.add_argument(
            "-a",
            "--address",
            default=None,
            help="IP address to bind to [default=`127.0.0.1`]",
        )
        parser.add_argument(
            "-p",
            "--port",
            type=int,
            default=None,
            help="Port to listen on [default=8000]",
        )
        parser.add_argument(
            "addrport",
            nargs="?",
            default="",
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

        # With no argument, bind to 127.0.0.1:8000 to match runserver, gunicorn, etc.
        address = "127.0.0.1"
        port = 8000
        # Default to the GRANIAN_ environment variables if set.
        if p := os.getenv("GRANIAN_PORT"):
            # Match gunicorn's behavior of binding to 0.0.0.0 when port is in the env.
            address = "0.0.0.0"
            port = int(p)
        if a := os.getenv("GRANIAN_HOST"):
            address = a

        # Then check to see if an address/port was specified on the command line.
        if options["addrport"].isdigit():
            # If specifying a port (but no address), bind to 0.0.0.0.
            address = "0.0.0.0"
            port = int(options["addrport"])
        elif ":" in options["addrport"]:
            a, p = options["addrport"].rsplit(":", 1)
            address = a or "0.0.0.0"
            port = int(p)
        elif options["addrport"]:
            address = options["addrport"]

        # Finally, override with --address/--port if specified. Necessary because
        # `serve -k 9000` will start 9000 workers instead of binding to port 9000.
        if a := options["address"]:
            address = a
        if p := options["port"]:
            port = p

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
