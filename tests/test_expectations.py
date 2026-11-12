import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from quality.expectations import build_suites  # noqa: E402


def test_every_expectation_has_a_severity():
    suites = build_suites("2026-06-05", ["A0001"], (70, 130))
    for table, exps in suites.items():
        for e in exps:
            assert e.meta["severity"] in {"critical", "warning"}, (table, e)


def test_volume_check_is_warning_not_blocking():
    suites = build_suites("2026-06-05", ["A0001"], (70, 130))
    vol = [e for e in suites["silver_usage"] if type(e).__name__ == "ExpectTableRowCountToBeBetween"]
    assert vol and vol[0].meta["severity"] == "warning"


def test_core_money_rules_are_critical():
    suites = build_suites("2026-06-05", ["A0001"], (70, 130))
    crit = {(type(e).__name__, getattr(e, "column", None)) for e in suites["silver_subscriptions"] if e.meta["severity"] == "critical"}
    assert ("ExpectColumnValuesToBeBetween", "mrr") in crit
    assert ("ExpectColumnValuesToBeInSet", "currency") in crit
