# django-dbtasks

Database backend and runner for [Django tasks](https://docs.djangoproject.com/en/dev/topics/tasks/) (new in 6.0).


## Quickstart

Install the `django-dbtasks` package from PyPI, and configure your [TASKS setting](https://docs.djangoproject.com/en/dev/ref/settings/#std-setting-TASKS) as follows:

```python
TASKS = {
    "default": {
        "BACKEND": "dbtasks.backend.DatabaseBackend",
        "OPTIONS": {
            # Set this to True to execute tasks immediately (no need for a runner).
            "immediate": False,
            # How long to retain ScheduleTasks in the database. Forever if not set.
            "retain": datetime.timedelta(days=7),
            # Tasks to run periodically.
            "periodic": {
                # Runs at 3:30am every Monday through Friday.
                "myproject.tasks.maintenance": "30 3 * * 1-5",
            },
        },
    },
}
```

## Runner

`django-dbtasks` includes a dedicated `taskrunner` management command:

```
usage: manage.py taskrunner [-h] [-w WORKERS] [-i WORKER_ID] [--backend BACKEND] [--delay DELAY]
```

It is also straightforward to run the runner in a thread of its own:

```python
runner = Runner(workers=4, worker_id="in-process")
t = threading.Thread(target=runner.run)
t.start()
...
runner.stop()
t.join()
```

`django-dbtasks` itself is tested on free-threading builds of Python 3.13 and 3.14, but compatibility will depend on your database driver and other packages.


## Periodic Tasks

As shown in the [quickstart](#quickstart), periodic tasks are specified as a mapping in the backend `OPTIONS` under the `periodic` key. The keys of the mapping should be dotted paths to the tasks, and the values should either be a string in [crontab format](https://crontab.guru), or an instance of `dbtasks.Periodic`. Using a `dbtasks.Periodic` allows you to specify `args` and `kwargs` (as values or callables) that will be passed to the task. For example, the `Runner` registers a periodic task to remove old tasks past the retention window, in a manner similar to:

```python
# Convert the `timedelta` to seconds, so as to be JSON-serializable.
retain_secs = int(self.backend.options["retain"].total_seconds())
# Clear old tasks every hour, on a random minute.
self.periodic["dbtasks.runner.cleanup"] = Periodic("~ * * * *", args=[retain_secs])
```

_Note that this allows you to specify a custom `Periodic` for the `dbtasks.runner.cleanup` task in your `TASKS` setting if necessary._


## Logging

Be sure to add a `dbtasks` logger to your `LOGGING` setting:

```python
LOGGING = {
    ...
    "loggers": {
        "dbtasks": {
            "handlers": ["console"],
            "level": "INFO",
        },
    },
}
```

## Testing

There is a `RunnerTestCase` that starts a runner for the duration of a test suite. See [test_tasks.py](tests/tests/test_tasks.py) for example usage.
