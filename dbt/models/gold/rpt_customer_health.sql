-- Simple, explainable health score for Customer Success (0-100).
with util as (
    select account_id, avg(utilisation_pct) as avg_utilisation_pct, sum(overage_revenue_usd) as overage_usd
    from {{ ref('rpt_usage_overage') }} group by 1
),
mrr_change as (
    select account_id,
           sum(case when as_of_date = (select max(as_of_date) from {{ ref('fct_mrr_daily') }}) then mrr_usd end)
         - sum(case when as_of_date = (select min(as_of_date) from {{ ref('fct_mrr_daily') }}) then mrr_usd end) as mrr_change_usd
    from {{ ref('fct_mrr_daily') }} group by 1
),
open_opps as (
    select account_id, count(*) as open_opportunities
    from {{ source('silver', 'silver_opportunities') }}
    where stage not in ('Closed Won', 'Closed Lost') group by 1
)
select
    c.account_id, c.account_name, c.segment, c.csm_owner, c.current_mrr_usd,
    round(u.avg_utilisation_pct, 1)            as avg_utilisation_pct,
    coalesce(m.mrr_change_usd, 0)              as mrr_change_usd,
    coalesce(o.open_opportunities, 0)          as open_opportunities,
    least(100, greatest(0,
        50
        + case when u.avg_utilisation_pct >= 80 then 25 when u.avg_utilisation_pct >= 50 then 10 else -20 end
        + case when coalesce(m.mrr_change_usd, 0) > 0 then 15 when coalesce(m.mrr_change_usd, 0) < 0 then -25 else 0 end
        + case when coalesce(o.open_opportunities, 0) > 0 then 10 else 0 end
    ))                                         as health_score,
    case
        when u.avg_utilisation_pct < 50 or coalesce(m.mrr_change_usd, 0) < 0 then 'at risk'
        when u.avg_utilisation_pct >= 100 then 'upsell candidate'
        else 'healthy'
    end                                        as health_status
from {{ ref('dim_customer') }} c
left join util u on u.account_id = c.account_id
left join mrr_change m on m.account_id = c.account_id
left join open_opps o on o.account_id = c.account_id
where c.customer_status = 'active'
