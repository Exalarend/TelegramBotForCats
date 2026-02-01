from bot.notify.picker import weighted_choice


def test_weighted_choice_respects_zero_weights():
    opts = [{"id": 1, "weight": 0}, {"id": 2, "weight": 10}]
    picked = weighted_choice(opts, weight_key="weight")
    assert picked["id"] == 2


def test_weighted_choice_single_option():
    opts = [{"id": 7, "weight": 1}]
    picked = weighted_choice(opts, weight_key="weight")
    assert picked["id"] == 7

