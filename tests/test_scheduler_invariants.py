from datetime import datetime, timezone


def test_compute_next_interval_run_dt_uses_last_sent_anchor(monkeypatch):
    import bot.scheduler as scheduler

    monkeypatch.setattr(scheduler.time_mod, "time", lambda: 1_000)
    rule = {"interval_minutes": 2, "last_sent_at_ts": 940, "created_at_ts": 1}
    dt = scheduler._compute_next_interval_run_dt(rule)
    assert dt == datetime.fromtimestamp(1_060, tz=timezone.utc)


def test_compute_next_interval_run_dt_uses_created_at_when_never_sent(monkeypatch):
    import bot.scheduler as scheduler

    monkeypatch.setattr(scheduler.time_mod, "time", lambda: 1_000)
    rule = {"interval_minutes": 5, "last_sent_at_ts": None, "created_at_ts": 900}
    dt = scheduler._compute_next_interval_run_dt(rule)
    assert dt == datetime.fromtimestamp(1_200, tz=timezone.utc)


def test_job_generation_gates_stale_jobs():
    import bot.scheduler as scheduler

    class DummyApp:
        def __init__(self):
            self.bot_data = {}

    app = DummyApp()
    gen = scheduler._bump_rule_generation(app, chat_id=1, rule_id=2, job_kind=scheduler.JOB_KIND_RULE)
    assert gen == 1

    assert scheduler._job_is_stale(app, chat_id=1, rule_id=2, job_kind=scheduler.JOB_KIND_RULE, job_generation=0) is True
    assert scheduler._job_is_stale(app, chat_id=1, rule_id=2, job_kind=scheduler.JOB_KIND_RULE, job_generation=1) is False


def test_job_generation_unknown_treated_as_current():
    import bot.scheduler as scheduler

    class DummyApp:
        def __init__(self):
            self.bot_data = {}

    app = DummyApp()
    assert scheduler._job_is_stale(app, chat_id=1, rule_id=2, job_kind=scheduler.JOB_KIND_RULE, job_generation=0) is False

