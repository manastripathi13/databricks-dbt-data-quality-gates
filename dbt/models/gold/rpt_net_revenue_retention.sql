-- Net and gross revenue retention for customers who were paying on the first day of the period.
-- NRR = (start MRR + expansion - contraction - churn) / start MRR. New customers are excluded.
with by_customer as (
    select account_id, as_of_date, sum(mrr_usd) as mrr
    from {{ ref('fct_mrr_daily') }}
    group by 1, 2
),
bounds as (select min(as_of_date) as d0, max(as_of_date) as d1 from by_customer),
cohort as (
    select s.account_id, s.mrr as start_mrr, coalesce(e.mrr, 0) as end_mrr
    from by_customer s
    cross join bounds b
    left join by_customer e on e.account_id = s.account_id and e.as_of_date = b.d1
    where s.as_of_date = b.d0 and s.mrr > 0
)
select
    (select d0 from bounds)                                                    as period_start,
    (select d1 from bounds)                                                    as period_end,
    count(*)                                                                   as starting_customers,
    round(sum(start_mrr), 2)                                                   as starting_mrr_usd,
    round(sum(case when end_mrr > start_mrr then end_mrr - start_mrr else 0 end), 2)             as expansion_usd,
    round(sum(case when end_mrr < start_mrr and end_mrr > 0 then start_mrr - end_mrr else 0 end), 2) as contraction_usd,
    round(sum(case when end_mrr = 0 then start_mrr else 0 end), 2)             as churned_usd,
    count(case when end_mrr = 0 then 1 end)                                    as churned_customers,
    round(100 * sum(end_mrr) / sum(start_mrr), 2)                              as net_revenue_retention_pct,
    round(100 * sum(least(end_mrr, start_mrr)) / sum(start_mrr), 2)            as gross_revenue_retention_pct
from cohort
