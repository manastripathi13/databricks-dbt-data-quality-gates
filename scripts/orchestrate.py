"""Local stand-in for the Azure Data Factory pipeline (adf/pl_revenue_daily.json).

For each business day:
  1. land        new files arrive in the landing zone              (ADF: Copy activity)
  2. pipeline    Bronze -> Silver with quarantine                  (ADF: run Databricks Lakeflow pipeline)
  3. gate        Great Expectations on Silver; critical failure    (ADF: Databricks job + If Condition)
                 means Gold is NOT refreshed and dashboards keep yesterday's correct numbers
  4. gold        dbt build: Gold models + tests                    (ADF: Databricks job, dbt task)
After all days, the same checks run on the raw drops to measure the "no gates" baseline.
Every step is logged to data/run_log.jsonl.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LOG = ROOT / "data" / "run_log.jsonl"
ENV = {**os.environ, "SPARK_CONF_DIR": str(ROOT / "conf"), "GX_ANALYTICS_ENABLED": "False"}


def step(name: str, cmd: list[str], cwd: Path = ROOT) -> tuple[bool, float, str]:
    t = time.time()
    p = subprocess.run(cmd, cwd=cwd, env=ENV, capture_output=True, text=True)
    out = (p.stdout + p.stderr).replace("\r", "\n")
    keep = [ln for ln in out.splitlines() if ln.startswith(("[gate]", "       CRITICAL")) or "Done. PASS=" in ln or "Run is" in ln]
    return p.returncode == 0, round(time.time() - t, 1), "\n".join(keep)


def log(entry: dict) -> None:
    with LOG.open("a") as f:
        f.write(json.dumps(entry) + "\n")


def main(days: int) -> None:
    LOG.parent.mkdir(parents=True, exist_ok=True)
    LOG.unlink(missing_ok=True)
    start = date(2026, 6, 1)
    for n in range(1, days + 1):
        d = (start + timedelta(days=n - 1)).isoformat()
        print(f"\n=== business day {d}")
        ok, t, _ = step("land", ["uv", "run", "python", "generator/generate_daily_drops.py", "--days", str(days), "--only", str(n)])
        print(f"  land      {'ok' if ok else 'FAILED'} ({t}s)")
        ok_p, t, out = step("pipeline", ["./scripts/run_pipeline.sh"])
        print(f"  pipeline  {'ok' if ok_p else 'FAILED'} ({t}s)")
        ok_g, t, out = step("gate", ["uv", "run", "python", "quality/run_gate.py", "--run-date", d])
        print(f"  gate      {'PASS' if ok_g else 'BLOCK'} ({t}s)\n" + "\n".join("    " + x for x in out.splitlines()))
        gold = None
        if ok_p and ok_g:
            gold, t, out = step("gold", ["uv", "run", "dbt", "build", "--profiles-dir", "."], cwd=ROOT / "dbt")
            print(f"  gold      {'refreshed' if gold else 'FAILED'} ({t}s) {out.strip()}")
        else:
            print("  gold      skipped: dashboards keep the last good data")
        log({"run_date": d, "pipeline_ok": ok_p, "gate_passed": ok_g, "gold_refreshed": bool(gold)})

    print("\n=== baseline: same checks on the raw drops (what dashboards got with no gates)")
    dates = [(start + timedelta(days=i)).isoformat() for i in range(days)]
    _, t, out = step("baseline", ["uv", "run", "python", "quality/run_gate.py", "--raw", "--run-date", *dates])
    print("\n".join("  " + x for x in out.splitlines()))
    print("\n=== final dbt build so the data quality scorecard includes the baseline")
    ok, t, out = step("gold", ["uv", "run", "dbt", "build", "--profiles-dir", "."], cwd=ROOT / "dbt")
    print(f"  {out.strip()}")


if __name__ == "__main__":
    main(int(sys.argv[1]) if len(sys.argv) > 1 else 10)
