-- Daily ARR by product and segment: the headline revenue chart.
select
    m.as_of_date,
    m.product,
    c.segment,
    count(distinct case when m.mrr_usd > 0 then m.account_id end) as paying_customers,
    round(sum(m.mrr_usd), 2)                                       as mrr_usd,
    round(sum(m.mrr_usd) * 12, 2)                                  as arr_usd
from {{ ref('fct_mrr_daily') }} m
join {{ ref('dim_customer') }} c on c.account_id = m.account_id
group by 1, 2, 3
