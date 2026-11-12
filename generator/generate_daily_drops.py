"""Simulate daily file drops from three siloed revenue systems.

landing/
  accounts/      day=YYYY-MM-DD/accounts.csv           (CRM accounts, full daily snapshot)
  opportunities/ day=YYYY-MM-DD/opportunities.csv      (CRM deals created or changed that day)
  subscriptions/ day=YYYY-MM-DD/subscriptions.jsonl    (billing CDC feed: every change to a subscription)
  usage/         day=YYYY-MM-DD/usage.jsonl            (product metering: API calls per account per day)

Real feeds are messy. On some days the generator injects one or more of these defects:
  null_account_usage   usage rows with no account_id
  negative_mrr         subscription change with negative MRR
  unknown_currency     subscription in a currency finance does not support
  duplicate_events     the same usage event_id sent twice
  future_usage         usage dated in the future (clock bug)
  orphan_opportunity   deal pointing to an account that does not exist
Which defects hit which day is recorded in landing/_defect_log.json (used only for evaluation).

Usage:
    python generator/generate_daily_drops.py --days 10            # land all days
    python generator/generate_daily_drops.py --days 10 --only 3   # land just day 3
"""

from __future__ import annotations

import argparse
import csv
import json
import random
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LANDING = ROOT / "data" / "landing"
START = date(2026, 6, 1)
PRODUCTS = {  # product: (list MRR per seat USD, included API units per seat per month, overage USD per 1k units)
    "Rate Intelligence": (1800, 300_000, 2.0),
    "Channel Manager": (1200, 150_000, 1.5),
    "Demand Forecasting": (2400, 500_000, 2.5),
}
SEGMENTS = {"Enterprise": (5, 12), "Mid-Market": (2, 5), "SMB": (1, 2)}
DEFECTS = ["null_account_usage", "negative_mrr", "unknown_currency", "duplicate_events", "future_usage", "orphan_opportunity"]
DEFECT_RATE = 0.06


def ts(d: date, hour: int = 9) -> str:
    return datetime(d.year, d.month, d.day, hour, tzinfo=timezone.utc).isoformat()


class World:
    def __init__(self, seed: int = 11):
        self.rng = random.Random(seed)
        self.accounts: list[dict] = []
        self.subs: dict[str, dict] = {}
        self.opps: dict[str, dict] = {}
        r = self.rng
        for i in range(1, 121):
            seg = r.choices(list(SEGMENTS), weights=[2, 4, 5])[0]
            self.accounts.append({
                "account_id": f"A{i:04d}",
                "account_name": f"{r.choice(['Azure','Coral','Summit','Harbor','Lotus','Atlas','Meridian','Saffron','Nimbus','Sterling'])} "
                                f"{r.choice(['Hotels','Resorts','Travels','Airways','Stays','Hospitality'])} {i}",
                "segment": seg,
                "region": r.choice(["APAC", "EMEA", "AMER"]),
                "csm_owner": r.choice(["Ira", "Noah", "Zara", "Kabir", "Elena"]),
            })
            if r.random() < 0.8:  # 80% are paying customers at the start
                self._new_sub(self.accounts[-1], START - timedelta(days=r.randint(30, 400)))

    def _new_sub(self, acc: dict, start: date, op_day: date | None = None) -> dict:
        r = self.rng
        product = r.choice(list(PRODUCTS))
        seats = r.randint(*SEGMENTS[acc["segment"]])
        price, units, _ = PRODUCTS[product]
        sub = {
            "subscription_id": f"S{uuid.UUID(int=r.getrandbits(128)).hex[:10]}",
            "account_id": acc["account_id"], "product": product, "seats": seats,
            "mrr": round(price * seats * r.uniform(0.85, 1.0), 2), "currency": "USD",
            "included_units": units * seats, "status": "active",
            "start_date": start.isoformat(), "end_date": None,
            "updated_at": ts(op_day or start), "op": "UPSERT",
        }
        self.subs[sub["subscription_id"]] = sub
        return sub

    def day(self, d: date, first: bool) -> dict[str, list[dict]]:
        r = self.rng
        sub_changes = [dict(s) for s in self.subs.values()] if first else []
        if not first:
            for s in list(self.subs.values()):
                if s["status"] != "active":
                    continue
                x = r.random()
                if x < 0.02:      # expansion: more seats
                    s["seats"] += r.randint(1, 3)
                    s["mrr"] = round(PRODUCTS[s["product"]][0] * s["seats"] * r.uniform(0.85, 1.0), 2)
                    s["included_units"] = PRODUCTS[s["product"]][1] * s["seats"]
                elif x < 0.03:    # contraction
                    s["seats"] = max(1, s["seats"] - 1)
                    s["mrr"] = round(PRODUCTS[s["product"]][0] * s["seats"] * r.uniform(0.85, 1.0), 2)
                    s["included_units"] = PRODUCTS[s["product"]][1] * s["seats"]
                elif x < 0.037:   # churn
                    s["status"], s["end_date"] = "cancelled", d.isoformat()
                else:
                    continue
                s["updated_at"] = ts(d, r.randint(8, 18))
                sub_changes.append(dict(s))
            for acc in r.sample(self.accounts, 2):  # new logos / cross-sell
                sub_changes.append(dict(self._new_sub(acc, d, d)))

        usage = []
        for s in self.subs.values():
            if s["status"] != "active" or date.fromisoformat(s["start_date"]) > d:
                continue
            daily = s["included_units"] / 30 * r.uniform(0.5, 1.6)
            usage.append({"event_id": str(uuid.UUID(int=r.getrandbits(128))), "account_id": s["account_id"],
                          "subscription_id": s["subscription_id"], "product": s["product"],
                          "usage_date": d.isoformat(), "api_units": int(daily), "received_at": ts(d, 23)})

        opps = []
        for _ in range(r.randint(3, 8)):
            acc = r.choice(self.accounts)
            oid = f"O{uuid.UUID(int=r.getrandbits(128)).hex[:10]}"
            product = r.choice(list(PRODUCTS))
            self.opps[oid] = {"opportunity_id": oid, "account_id": acc["account_id"], "product": product,
                              "type": r.choice(["New Business", "Expansion", "Renewal"]), "stage": "Prospecting",
                              "amount_usd": round(PRODUCTS[product][0] * 12 * r.randint(1, 6), 2),
                              "close_date": (d + timedelta(days=r.randint(20, 120))).isoformat(),
                              "updated_at": ts(d)}
            opps.append(dict(self.opps[oid]))
        for o in r.sample(list(self.opps.values()), min(4, len(self.opps))):
            if o["stage"] in ("Closed Won", "Closed Lost"):
                continue
            o["stage"] = r.choice(["Qualification", "Proposal", "Negotiation", "Closed Won", "Closed Lost"])
            o["updated_at"] = ts(d, 15)
            opps.append(dict(o))
        return {"accounts": [dict(a) for a in self.accounts], "opportunities": opps,
                "subscriptions": sub_changes, "usage": usage}


def inject(day_data: dict, rng: random.Random, d: date) -> list[str]:
    hit = [x for x in DEFECTS if rng.random() < DEFECT_RATE]
    for defect in hit:
        u, s, o = day_data["usage"], day_data["subscriptions"], day_data["opportunities"]
        if defect == "null_account_usage" and u:
            for row in rng.sample(u, min(5, len(u))):
                row["account_id"] = None
        elif defect == "negative_mrr" and s:
            bad = dict(rng.choice(s), mrr=-abs(rng.choice(s)["mrr"]), updated_at=ts(d, 20))
            s.append(bad)
        elif defect == "unknown_currency" and s:
            s.append(dict(rng.choice(s), currency="XYZ", updated_at=ts(d, 21)))
        elif defect == "duplicate_events" and u:
            u.extend(dict(x) for x in rng.sample(u, min(8, len(u))))
        elif defect == "future_usage" and u:
            for row in rng.sample(u, min(3, len(u))):
                row["usage_date"] = (d + timedelta(days=400)).isoformat()
        elif defect == "orphan_opportunity" and o:
            o.append(dict(rng.choice(o), opportunity_id=f"O{rng.getrandbits(40):x}", account_id="A9999"))
    return hit


def write(day_dir: Path, name: str, rows: list[dict], kind: str):
    day_dir.mkdir(parents=True, exist_ok=True)
    path = day_dir / f"{name}.{kind}"
    if kind == "csv":
        with path.open("w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0]))
            w.writeheader()
            w.writerows(rows)
    else:
        path.write_text("".join(json.dumps(r) + "\n" for r in rows))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=10)
    ap.add_argument("--only", type=int, help="land only this day number (1-based)")
    a = ap.parse_args()

    world = World()
    defect_rng = random.Random(99)
    log_path = LANDING / "_defect_log.json"
    log = json.loads(log_path.read_text()) if log_path.exists() and a.only else {}
    for n in range(1, a.days + 1):
        d = START + timedelta(days=n - 1)
        data = world.day(d, first=(n == 1))
        defects = inject(data, defect_rng, d)
        if a.only and n != a.only:
            continue
        for name, kind in [("accounts", "csv"), ("opportunities", "csv"), ("subscriptions", "jsonl"), ("usage", "jsonl")]:
            if data[name]:
                write(LANDING / name / f"day={d.isoformat()}", name, data[name], kind)
        log[d.isoformat()] = defects
        print(f"landed {d}: subs={len(data['subscriptions'])} usage={len(data['usage'])} opps={len(data['opportunities'])} defects={defects or 'none'}")
    LANDING.mkdir(parents=True, exist_ok=True)
    log_path.write_text(json.dumps(log, indent=2))


if __name__ == "__main__":
    main()
