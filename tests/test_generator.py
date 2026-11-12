import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from generator.generate_daily_drops import DEFECTS, World, inject  # noqa: E402


def test_world_is_deterministic():
    a, b = World(seed=5), World(seed=5)
    from datetime import date
    da, db = a.day(date(2026, 6, 1), True), b.day(date(2026, 6, 1), True)
    assert [s["subscription_id"] for s in da["subscriptions"]] == [s["subscription_id"] for s in db["subscriptions"]]


def test_inject_negative_mrr_creates_bad_row():
    import random
    from datetime import date
    w = World(seed=1)
    data = w.day(date(2026, 6, 1), True)
    rng = random.Random(0)
    rng.random = lambda: 0.0  # force every defect
    hit = inject(data, rng, date(2026, 6, 1))
    assert set(hit) == set(DEFECTS)
    assert any(s["mrr"] < 0 for s in data["subscriptions"])
    assert any(s["currency"] == "XYZ" for s in data["subscriptions"])
    assert any(u["account_id"] is None for u in data["usage"])
    ids = [u["event_id"] for u in data["usage"]]
    assert len(ids) != len(set(ids))  # duplicates present


def test_usage_never_exceeds_active_subscriptions():
    from datetime import date
    w = World(seed=3)
    d = w.day(date(2026, 6, 1), True)
    active = {s["subscription_id"] for s in w.subs.values() if s["status"] == "active"}
    assert {u["subscription_id"] for u in d["usage"]} <= active
