import datetime
import os

INSTALLED_APPS = ["dbtasks", "tests"]

if os.getenv("TEST_ENGINE") == "postgres":
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
else:
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
            "retain": datetime.timedelta(days=7),
            "periodic": {},
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
