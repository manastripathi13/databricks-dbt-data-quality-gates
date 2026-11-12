-- Month-to-date usage vs entitlement per active subscription, and the overage revenue it implies.
with usage as (
    select subscription_id, sum(api_units) as units_mtd, count(distinct usage_date) as days_with_usage,
           max(usage_date) as last_usage_date
    from {{ source('silver', 'silver_usage') }}
    where usage_date >= to_date('{{ var("period_start") }}')
    group by 1
),
price as (
    select * from values
        ('Rate Intelligence', 2.0), ('Channel Manager', 1.5), ('Demand Forecasting', 2.5)
        as t(product, overage_usd_per_1k_units)
)
select
    s.subscription_id,
    s.account_id,
    s.product,
    s.included_units                                                       as included_units_per_month,
    round(s.included_units * u.days_with_usage / 30.0)                     as prorated_entitlement,
    u.units_mtd,
    round(100.0 * u.units_mtd / nullif(s.included_units * u.days_with_usage / 30.0, 0), 1) as utilisation_pct,
    greatest(u.units_mtd - s.included_units * u.days_with_usage / 30.0, 0)  as overage_units,
    round(greatest(u.units_mtd - s.included_units * u.days_with_usage / 30.0, 0) / 1000 * p.overage_usd_per_1k_units, 2)
                                                                           as overage_revenue_usd
from {{ source('silver', 'silver_subscriptions') }} s
join usage u on u.subscription_id = s.subscription_id
join price p on p.product = s.product
where s.status = 'active'
