import textwrap

import pytest

from bot.system.config_loader import load_system_rules

pytest.importorskip("yaml")


def test_yaml_validation_rejects_missing_rules(tmp_path):
    p = tmp_path / "sys.yaml"
    p.write_text("{}", encoding="utf-8")
    with pytest.raises(ValueError):
        load_system_rules(str(p))


def test_yaml_validation_accepts_minimal_valid(tmp_path):
    p = tmp_path / "sys.yaml"
    p.write_text(
        textwrap.dedent(
            """
            rules:
              - system_key: test_rule
                kind: weekly
                enabled_by_default: true
                schedule:
                  days: [0]
                  time_hhmm: "09:00"
                images:
                  - ref: "FILE_ID"
                    ref_type: file_id
                    weight: 1
                    texts:
                      - text: "Hello"
                        weight: 1
            """
        ).strip(),
        encoding="utf-8",
    )
    rules = load_system_rules(str(p))
    assert len(rules) == 1
    assert rules[0].system_key == "test_rule"

