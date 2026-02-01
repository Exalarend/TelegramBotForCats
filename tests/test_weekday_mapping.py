from bot.utils.schedule import python_weekday_to_jobqueue


def test_python_weekday_to_jobqueue_mapping():
    # Python: Mon=0..Sun=6
    # PTB JobQueue: Sun=0..Sat=6
    assert python_weekday_to_jobqueue(0) == 1  # Mon -> 1
    assert python_weekday_to_jobqueue(1) == 2  # Tue -> 2
    assert python_weekday_to_jobqueue(5) == 6  # Sat -> 6
    assert python_weekday_to_jobqueue(6) == 0  # Sun -> 0

