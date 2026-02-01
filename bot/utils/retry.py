def compute_retry_delay_s(attempt: int) -> int:
    """
    attempt=1 -> 60s
    attempt=2 -> 120s
    attempt>=3 -> 300s
    """
    if attempt <= 1:
        return 60
    if attempt == 2:
        return 120
    return 300

