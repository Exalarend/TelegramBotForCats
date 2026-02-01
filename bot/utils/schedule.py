def python_weekday_to_jobqueue(day: int) -> int:
    """
    Storage uses Python weekday: Mon=0..Sun=6.
    PTB JobQueue.run_daily expects: Sun=0..Sat=6 (PTB v20+).
    """
    return (int(day) + 1) % 7

