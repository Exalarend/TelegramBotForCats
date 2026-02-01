def test_state_expired_draft_is_cleared(monkeypatch):
    from bot.handlers import state as flow_state

    class DummyContext:
        def __init__(self):
            self.user_data = {}

    ctx = DummyContext()
    # Freeze time.
    monkeypatch.setattr(flow_state.time, "time", lambda: 1_000)
    ctx.user_data["draft_rule"] = {"chat_id": 1, "actor_user_id": 10, "expires_at_ts": 999}

    assert flow_state.get_draft(ctx, chat_id=1, actor_user_id=10) is None
    assert "draft_rule" not in ctx.user_data


def test_state_actor_mismatch_does_not_clear(monkeypatch):
    from bot.handlers import state as flow_state

    class DummyContext:
        def __init__(self):
            self.user_data = {}

    ctx = DummyContext()
    monkeypatch.setattr(flow_state.time, "time", lambda: 1_000)
    ctx.user_data["draft_rule"] = {"chat_id": 1, "actor_user_id": 10, "expires_at_ts": 2_000}

    assert flow_state.get_draft(ctx, chat_id=1, actor_user_id=11) is None
    assert "draft_rule" in ctx.user_data

