def test_system_rule_insert_sets_created_at_ts(tmp_path):
    from bot.db import repo
    from bot.db.schema import _conn, ensure_schema

    db_path = tmp_path / "test.db"
    ensure_schema(db_path=str(db_path), default_timezone="Europe/Moscow")

    chat_id = 123
    images = [{"ref": "file_id_1", "ref_type": "file_id", "weight": 1.0, "texts": [("hi", 1.0)]}]
    rid_w = repo.ensure_system_rule_weekly(
        chat_id=chat_id, system_key="sys_weekly", title="T", days=[0], time_hhmm="09:00", images=images
    )
    rid_i = repo.ensure_system_rule_interval(
        chat_id=chat_id, system_key="sys_interval", title="T2", interval_minutes=60, images=images
    )

    with _conn() as con:
        r_w = con.execute("SELECT created_at_ts FROM rules WHERE id = ?", (rid_w,)).fetchone()
        r_i = con.execute("SELECT created_at_ts FROM rules WHERE id = ?", (rid_i,)).fetchone()

    assert int(r_w["created_at_ts"]) > 0
    assert int(r_i["created_at_ts"]) > 0

