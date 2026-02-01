from bot.utils.retry import compute_retry_delay_s


def test_compute_retry_delay_s():
    assert compute_retry_delay_s(1) == 60
    assert compute_retry_delay_s(2) == 120
    assert compute_retry_delay_s(3) == 300
    assert compute_retry_delay_s(4) == 300

