# django-dbtasks

Database backend and runner for [Django tasks](https://docs.djangoproject.com/en/dev/topics/tasks/) (new in 6.0).

`django-dbtasks` is tested on PostgreSQL, SQLite, and MySQL for versions of Python back to 3.12, including the free-threading builds.

_Note when using SQLite it is recommended to set `OPTIONS["transaction_mode"] = "IMMEDIATE"` - see https://forum.djangoproject.com/t/sqlite-and-database-is-locked-error/26994 for more information._


## Quickstart

Install the `django-dbtasks` package from PyPI, and configure your [TASKS setting](https://docs.djangoproject.com/en/dev/ref/settings/#std-setting-TASKS) as follows:

```python
TASKS = {
    "default": {
        "BACKEND": "dbtasks.backend.DatabaseBackend",
        "OPTIONS": {
            # Set this to True to execute tasks immediately (no need for a runner).
            "immediate": False,
            # Whether to send `task_enqueued`, `task_started`, and `task_finished`.
            "signals": True,
            # How long to retain ScheduledTasks in the database. Forever if not set.
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
usage: manage.py taskrunner [-h] [-w WORKERS] [-i WORKER_ID] [--backend BACKEND]
                            [--delay DELAY] [--no-periodic]
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

As shown in the [quickstart](#quickstart), periodic tasks are specified as a mapping in the backend `OPTIONS` under the `periodic` key. The keys of the mapping should be dotted paths to the tasks, and the values should either be a string in [crontab format](https://crontab.guru), or an instance of `dbtasks.Periodic`. Using a `dbtasks.Periodic` allows you to specify `args` and `kwargs` (as values or callables) that will be passed to the task, along with a custom `retain` duration.


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


## Extras

`django-dbtasks` comes with a number of optional features for integration into various environments.


### `dbtasks.contrib.serve`

This is a Django app you can add to `INSTALLED_APPS` to enable a `serve` management command that runs your site in a [Granian](https://github.com/emmett-framework/granian) server, with options to also start an integrated task runner. For instance, the following command will serve your site on `127.0.0.1:8000`, start a runner, and reload your server (and tasks!) on code changes:

```
manage.py serve -k -r
```

Running an integrated task runner (with `-k/--tasks`) is not a great idea in production, but is excellent for development. Granian is a production-caliber server, so you can also use `serve` in production alongside a separate [taskrunner command](#runner).

You can install the `django-dbtasks[serve]` extra to include Granian automatically.


### uWSGI mule

You can run a [uWSGI mule](https://uwsgi-docs.readthedocs.io/en/latest/Mules.html) that starts a task runner by passing `--mule=dbtasks.contrib.mule:taskrunner` or specifying `<mule>dbtasks.contrib.mule:taskrunner</mule>` in your XML config.
