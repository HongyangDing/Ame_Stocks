"""Celery application skeleton; persistent run orchestration begins in Step 2."""

from __future__ import annotations

import os

from celery import Celery

celery_app = Celery(
    "ame_stocks",
    broker=os.getenv("CELERY_BROKER_URL", "redis://127.0.0.1:6379/0"),
    backend=os.getenv("CELERY_RESULT_BACKEND", "redis://127.0.0.1:6379/1"),
)
celery_app.conf.update(
    accept_content=["json"],
    enable_utc=True,
    result_serializer="json",
    task_serializer="json",
    timezone="UTC",
)


@celery_app.task(name="ame_stocks.system.health")
def health() -> dict[str, str]:
    """Side-effect-free task used to verify a future worker connection."""

    return {"service": "ame-stocks-worker", "status": "ok"}
