"""Great Expectations suites for the Silver -> Gold quality gate.

Every expectation carries a severity in its `meta`:
  critical  a failure blocks the Gold refresh (dashboards keep yesterday's correct numbers)
  warning   recorded and reported, but Gold still refreshes

The same suites can be pointed at the *raw* daily drop to measure what dashboards would
have received without any gates (the "before" baseline).
"""

from __future__ import annotations

from datetime import date

import great_expectations as gx
import great_expectations.expectations as gxe

PRODUCTS = ["Rate Intelligence", "Channel Manager", "Demand Forecasting"]
CURRENCIES = ["USD", "EUR", "GBP", "INR", "AED", "SGD"]
STAGES = ["Prospecting", "Qualification", "Proposal", "Negotiation", "Closed Won", "Closed Lost"]


def _c(exp):
    exp.meta = {"severity": "critical"}
    return exp


def _w(exp):
    exp.meta = {"severity": "warning"}
    return exp


def build_suites(run_date: str, known_accounts: list[str], expected_usage_rows: tuple[int, int]) -> dict[str, list]:
    """Expectations per Silver table. Some depend on today's context (date, known accounts, volume)."""
    lo, hi = expected_usage_rows
    return {
        "silver_usage": [
            _c(gxe.ExpectColumnValuesToNotBeNull(column="event_id")),
            _c(gxe.ExpectColumnValuesToBeUnique(column="event_id")),
            _c(gxe.ExpectColumnValuesToNotBeNull(column="account_id")),
            _c(gxe.ExpectColumnValuesToBeInSet(column="account_id", value_set=known_accounts)),
            _c(gxe.ExpectColumnValuesToBeBetween(column="api_units", min_value=0, max_value=50_000_000)),
            _c(gxe.ExpectColumnValuesToBeBetween(column="usage_date", max_value=date.fromisoformat(run_date))),
            _c(gxe.ExpectColumnValuesToBeInSet(column="product", value_set=PRODUCTS)),
            _w(gxe.ExpectTableRowCountToBeBetween(min_value=lo, max_value=hi)),  # volume anomaly check
        ],
        "silver_subscriptions": [
            _c(gxe.ExpectColumnValuesToBeUnique(column="subscription_id")),
            _c(gxe.ExpectColumnValuesToNotBeNull(column="account_id")),
            _c(gxe.ExpectColumnValuesToBeBetween(column="mrr", min_value=0)),
            _c(gxe.ExpectColumnValuesToBeInSet(column="currency", value_set=CURRENCIES)),
            _c(gxe.ExpectColumnValuesToBeInSet(column="status", value_set=["active", "cancelled"])),
            _w(gxe.ExpectColumnValuesToBeBetween(column="seats", min_value=1, max_value=100)),
        ],
        "silver_opportunities": [
            _c(gxe.ExpectColumnValuesToBeUnique(column="opportunity_id")),
            _c(gxe.ExpectColumnValuesToBeInSet(column="account_id", value_set=known_accounts)),
            _c(gxe.ExpectColumnValuesToBeBetween(column="amount_usd", min_value=0, strict_min=True)),
            _c(gxe.ExpectColumnValuesToBeInSet(column="stage", value_set=STAGES)),
        ],
        "silver_accounts": [
            _c(gxe.ExpectColumnValuesToBeUnique(column="account_id")),
            _c(gxe.ExpectColumnValuesToNotBeNull(column="account_id")),
            _w(gxe.ExpectColumnValuesToBeInSet(column="segment", value_set=["Enterprise", "Mid-Market", "SMB"])),
        ],
    }


def suite_from(name: str, expectations: list) -> gx.ExpectationSuite:
    suite = gx.ExpectationSuite(name=name)
    for e in expectations:
        suite.add_expectation(e)
    return suite
