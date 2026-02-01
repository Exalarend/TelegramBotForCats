def fmt_rule_schedule(rule: dict) -> str:
    if rule.get("kind") == "weekly":
        labels = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
        days = [labels[d] for d in (rule.get("days") or [])]
        return f"{', '.join(days)} в {rule.get('time_hhmm')}"
    if rule.get("kind") == "interval":
        return f"Каждые {rule.get('interval_minutes')} мин"
    return "Правило"


def fmt_rule_name(rule: dict) -> str:
    name = str(rule.get("title") or "").strip()
    return name if name else fmt_rule_schedule(rule)

