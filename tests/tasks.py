import asyncio
import logging
from typing import Any

from django.tasks import TaskContext, task

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


@task(takes_context=True)
async def async_task(context: TaskContext, arg: Any):
    await asyncio.sleep(0.01)
    return {"id": context.task_result.id, "arg": arg}
