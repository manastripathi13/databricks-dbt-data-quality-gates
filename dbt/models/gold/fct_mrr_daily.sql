-- MRR per subscription per day, reconstructed from SCD Type 2 history:
-- for every day we pick the subscription version that was valid at the end of that day.
with days as (
    -- from the period start to the latest business day seen in any feed
    -- (on a quiet day no subscription changes, so the history alone would end too early)
    select explode(sequence(to_date('{{ var("period_start") }}'),
                            greatest((select max(to_date(valid_from)) from {{ source('silver', 'silver_subscriptions_history') }}),
                                     (select max(usage_date) from {{ source('silver', 'silver_usage') }})),
                            interval 1 day)) as as_of_date
),
versions as (
    select * from {{ source('silver', 'silver_subscriptions_history') }}
)
select
    d.as_of_date,
    v.subscription_id,
    v.account_id,
    v.product,
    v.seats,
    case when v.status = 'active' then v.mrr else 0 end as mrr_usd,
    v.status
from days d
join versions v
  on v.valid_from < d.as_of_date + interval 1 day
 and (v.valid_to is null or v.valid_to >= d.as_of_date + interval 1 day)
