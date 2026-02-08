"""Loader for Big Red Button config (config/big_red_button.yaml).
Supports tree structure: nodes with `children` = submenu, nodes with `images` = leaf (sends content).
"""

from __future__ import annotations

import os
from dataclasses import dataclass

from bot.system.config_loader import SystemImage, SystemImageText


@dataclass
class BigRedNode:
    """Node in Big Red Button tree. Either folder (children) or leaf (images)."""
    key: str
    title: str
    children: list["BigRedNode"] | None = None
    images: list[SystemImage] | None = None

    def is_folder(self) -> bool:
        return bool(self.children)

    def is_leaf(self) -> bool:
        return bool(self.images)


def _parse_node(b: dict) -> BigRedNode | None:
    if not isinstance(b, dict):
        return None
    key = str(b.get("key") or "").strip()
    if not key:
        return None
    title = str(b.get("title") or "").strip() or key

    children_raw = b.get("children")
    children: list[BigRedNode] = []
    if isinstance(children_raw, list) and children_raw:
        for c in children_raw:
            child = _parse_node(c)
            if child:
                children.append(child)

    images: list[SystemImage] = []
    images_raw = b.get("images") or []
    if isinstance(images_raw, list) and images_raw:
        for img in images_raw:
            if not isinstance(img, dict):
                continue
            ref = str(img.get("ref") or "").strip()
            ref_type = str(img.get("ref_type") or "").strip()
            if not ref or ref_type not in {"file_id", "url", "path"}:
                continue
            weight = float(img.get("weight", 1.0))
            texts_raw = img.get("texts") or []
            if not isinstance(texts_raw, list) or not texts_raw:
                continue
            texts_list: list[SystemImageText] = []
            for t in texts_raw:
                if not isinstance(t, dict):
                    continue
                text = str(t.get("text") or "").strip()
                if not text:
                    continue
                tw = float(t.get("weight", 1.0))
                texts_list.append(SystemImageText(text=text, weight=tw))
            if texts_list:
                images.append(SystemImage(ref=ref, ref_type=ref_type, weight=weight, texts=texts_list))

    if not children and not images:
        return None

    return BigRedNode(key=key, title=title, children=children if children else None, images=images if images else None)


def load_big_red_buttons(yaml_path: str) -> list[BigRedNode]:
    try:
        import yaml  # type: ignore
    except ModuleNotFoundError as e:
        raise ModuleNotFoundError("PyYAML is required. Install it via `pip install PyYAML`.") from e

    if not os.path.exists(yaml_path):
        return []

    with open(yaml_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    buttons_raw = data.get("buttons") or []
    if not isinstance(buttons_raw, list):
        return []

    nodes: list[BigRedNode] = []
    for b in buttons_raw:
        node = _parse_node(b)
        if node:
            nodes.append(node)
    return nodes


def get_nodes_at_path(root_nodes: list[BigRedNode], path: str) -> list[BigRedNode]:
    """Return children to display at given path. path="" = root, path="key1.key2" = nested."""
    if not path:
        return root_nodes
    parts = path.split(".")
    current: list[BigRedNode] = root_nodes
    for part in parts:
        part = part.strip()
        if not part:
            continue
        found = next((n for n in current if n.key == part), None)
        if not found or not found.children:
            return []
        current = found.children
    return current


def find_node_by_path(root_nodes: list[BigRedNode], path: str) -> BigRedNode | None:
    """Find node by path. path="key1" or path="key1.key2"."""
    if not path:
        return None
    parts = path.split(".")
    current: list[BigRedNode] = root_nodes
    node: BigRedNode | None = None
    for part in parts:
        part = part.strip()
        if not part:
            continue
        found = next((n for n in current if n.key == part), None)
        if not found:
            return None
        node = found
        current = found.children or []
    return node
