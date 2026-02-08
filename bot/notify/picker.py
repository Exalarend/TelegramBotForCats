import os
import random
from dataclasses import dataclass


@dataclass(frozen=True)
class PickedContent:
    text: str
    image_ref: str | None
    image_ref_type: str | None
    image_option_id: int | None


def weighted_choice(options: list[dict], *, weight_key: str) -> dict | None:
    if not options:
        return None
    total = 0.0
    for o in options:
        w = float(o.get(weight_key, 1.0))
        if w > 0:
            total += w
    if total <= 0:
        return options[0]
    r = random.random() * total
    upto = 0.0
    for o in options:
        w = float(o.get(weight_key, 1.0))
        if w <= 0:
            continue
        upto += w
        if upto >= r:
            return o
    return options[-1]


def pick_system_content(
    *,
    rule: dict,
    text_options: list[dict],
    image_options: list[dict],
) -> PickedContent:
    """
    Implements: pick image (weighted) -> pick text from that image's set (weighted).
    """
    images = [i for i in image_options if _is_image_option_available(str(i.get("ref")), str(i.get("ref_type")))]

    picked_image = weighted_choice(images, weight_key="weight") if images else None

    image_ref = None
    image_ref_type = None
    image_option_id = None
    if picked_image:
        image_ref = str(picked_image.get("ref"))
        image_ref_type = str(picked_image.get("ref_type") or "file_id")
        image_option_id = int(picked_image.get("id"))

    candidate_texts: list[dict] = []
    if image_option_id is not None:
        candidate_texts = [t for t in text_options if t.get("image_option_id") == image_option_id]
    if not candidate_texts:
        candidate_texts = [t for t in text_options if t.get("image_option_id") is None] or text_options
    picked_text = weighted_choice(candidate_texts, weight_key="weight") if candidate_texts else None
    text = str((picked_text or {}).get("text") or "")

    return PickedContent(
        text=text,
        image_ref=image_ref,
        image_ref_type=image_ref_type,
        image_option_id=image_option_id,
    )


def pick_big_red_content(button) -> PickedContent:
    """
    Pick random image (weighted) then random text from that image (weighted).
    Works with BigRedButton from bot.system.big_red_loader.
    """
    images = [
        {"ref": img.ref, "ref_type": img.ref_type, "weight": img.weight, "texts": img.texts}
        for img in button.images
        if _is_image_option_available(img.ref, img.ref_type)
    ]
    picked_image_dict = weighted_choice(images, weight_key="weight") if images else None

    image_ref = None
    image_ref_type = None
    candidate_texts: list[dict] = []
    if picked_image_dict:
        image_ref = str(picked_image_dict.get("ref"))
        image_ref_type = str(picked_image_dict.get("ref_type") or "file_id")
        texts_list = picked_image_dict.get("texts") or []
        for t in texts_list:
            text_val = getattr(t, "text", None) if hasattr(t, "text") else (t.get("text") if isinstance(t, dict) else None)
            weight_val = getattr(t, "weight", 1.0) if hasattr(t, "weight") else (t.get("weight", 1.0) if isinstance(t, dict) else 1.0)
            if text_val:
                candidate_texts.append({"text": str(text_val), "weight": float(weight_val)})

    picked_text = weighted_choice(candidate_texts, weight_key="weight") if candidate_texts else None
    text = str((picked_text or {}).get("text") or "")

    return PickedContent(
        text=text,
        image_ref=image_ref,
        image_ref_type=image_ref_type,
        image_option_id=None,
    )


def _is_image_option_available(ref: str, ref_type: str) -> bool:
    if not ref:
        return False
    if ref_type in {"file_id", "url"}:
        return True
    if ref_type == "path":
        repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
        abs_path = os.path.abspath(os.path.join(repo_root, ref))
        return os.path.exists(abs_path)
    return True

