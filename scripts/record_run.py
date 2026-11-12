"""Append one line per business day to data/run_log.jsonl (used by the report)."""
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
run_date, pipeline_ok, gate_passed, gold_refreshed = sys.argv[1], *(a == "success" for a in sys.argv[2:5])
log = ROOT / "data" / "run_log.jsonl"
rows = [json.loads(x) for x in log.read_text().splitlines()] if log.exists() else []
rows = [r for r in rows if r["run_date"] != run_date] + [
    {"run_date": run_date, "pipeline_ok": pipeline_ok, "gate_passed": gate_passed, "gold_refreshed": gold_refreshed}]
log.write_text("".join(json.dumps(r) + "\n" for r in sorted(rows, key=lambda r: r["run_date"])))
print(rows[-1])
