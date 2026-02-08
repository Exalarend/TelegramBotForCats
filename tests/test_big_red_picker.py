import random

from bot.notify.picker import pick_big_red_content
from bot.system.big_red_loader import BigRedNode
from bot.system.config_loader import SystemImage, SystemImageText


def test_pick_big_red_content():
    node = BigRedNode(
        key="test",
        title="Test",
        children=None,
        images=[
            SystemImage(
                ref="assets/system/Garfield_1.jpeg",
                ref_type="path",
                weight=1.0,
                texts=[
                    SystemImageText(text="Mew", weight=1),
                    SystemImageText(text="Mrr", weight=2),
                ],
            ),
        ],
    )
    random.seed(42)
    picked = pick_big_red_content(node)
    assert picked.image_ref == "assets/system/Garfield_1.jpeg"
    assert picked.image_ref_type == "path"
    assert picked.text in ("Mew", "Mrr")
