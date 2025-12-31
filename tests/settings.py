import os

INSTALLED_APPS = ["dbtasks", "tests"]

match os.getenv("TEST_ENGINE"):
    case "postgres":
        DATABASES = {
            "default": {
                "ENGINE": "django.db.backends.postgresql",
                "NAME": os.getenv("POSTGRES_DB", "dbtasks"),
                "USER": os.getenv("POSTGRES_USER", "postgres"),
                "PASSWORD": os.getenv("POSTGRES_PASSWORD", ""),
                "HOST": os.getenv("POSTGRES_HOST", "localhost"),
                "PORT": os.getenv("POSTGRES_PORT", 5432),
            }
        }
    case "mysql":
        DATABASES = {
            "default": {
                "ENGINE": "mysql.connector.django",
                "NAME": os.getenv("MYSQL_DB", "dbtasks"),
                "USER": os.getenv("MYSQL_USER", "root"),
                "PASSWORD": os.getenv("MYSQL_PASSWORD", ""),
                "HOST": os.getenv("MYSQL_HOST", "localhost"),
                "PORT": os.getenv("MYSQL_PORT", 3306),
            }
        }
    case _:
        DATABASES = {
            "default": {
                "ENGINE": "django.db.backends.sqlite3",
                "NAME": ":memory:",
                "TEST": {
                    "NAME": "testdb.sqlite3",
                },
                "OPTIONS": {
                    "transaction_mode": "IMMEDIATE",
                },
            }
        }

TASKS = {
    "default": {
        "BACKEND": "dbtasks.backend.DatabaseBackend",
        "OPTIONS": {
            "immediate": False,
            "retain": {
                "tests.tasks.send_mail": 0,
            },
            "periodic": {
                "tests.tasks.maintenance": "1h",
            },
        },
    },
}

LOGGING = {
    "version": 1,
    "disable_existing_loggers": True,
    "handlers": {
        "tests": {
            "class": "tests.loggers.TestLogHandler",
            "level": "DEBUG",
        },
    },
    "root": {
        "handlers": ["tests"],
        "level": "DEBUG",
    },
}

TIME_ZONE = "America/New_York"
