-- Sanity check: retention numbers must be possible. GRR is never above 100%, NRR is never negative.
select * from {{ ref('rpt_net_revenue_retention') }}
where gross_revenue_retention_pct > 100 or net_revenue_retention_pct < 0
