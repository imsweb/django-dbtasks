import logging

from django.tasks import task

logger = logging.getLogger(__name__)


@task
def send_mail(to: str, message: str):
    logger.info(f"Sending mail to {to}: {message}")
    return {"sent": True}


@task
def maintenance():
    pass


@task
def kaboom(msg: str):
    raise ValueError(msg)
