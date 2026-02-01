import random

from bot.notify.picker import pick_system_content


def test_system_picker_picks_text_from_selected_image():
    random.seed(123)
    rule = {"id": 1}
    image_options = [
        {"id": 10, "ref": "FILE_ID_A", "ref_type": "file_id", "weight": 1},
        {"id": 11, "ref": "FILE_ID_B", "ref_type": "file_id", "weight": 0},
    ]
    text_options = [
        {"id": 1, "image_option_id": 10, "text": "TEXT_A1", "weight": 1},
        {"id": 2, "image_option_id": 10, "text": "TEXT_A2", "weight": 0},
        {"id": 3, "image_option_id": 11, "text": "TEXT_B", "weight": 1},
    ]

    picked = pick_system_content(rule=rule, text_options=text_options, image_options=image_options)
    assert picked.image_option_id == 10
    assert picked.image_ref == "FILE_ID_A"
    assert picked.text == "TEXT_A1"

