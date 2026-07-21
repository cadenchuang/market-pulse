from inference.consumer import resolve_worker_count


def test_explicit_count_wins():
    assert resolve_worker_count(4, cpu_count=8) == 4


def test_zero_means_auto_cpu_count():
    assert resolve_worker_count(0, cpu_count=6) == 6


def test_negative_means_auto():
    assert resolve_worker_count(-1, cpu_count=2) == 2


def test_never_less_than_one():
    assert resolve_worker_count(0, cpu_count=0) == 1
