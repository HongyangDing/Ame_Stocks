from ame_stocks_worker.celery_app import celery_app, health


def test_worker_skeleton_is_registered_without_contacting_broker() -> None:
    assert celery_app.main == "ame_stocks"
    assert "ame_stocks.system.health" in celery_app.tasks
    assert health.run() == {"service": "ame-stocks-worker", "status": "ok"}
