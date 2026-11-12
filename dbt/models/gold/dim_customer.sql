-- One row per customer with current commercial status.
with subs as (
    select account_id,
           sum(case when status = 'active' then mrr else 0 end)                     as current_mrr_usd,
           count(case when status = 'active' then 1 end)                            as active_subscriptions,
           concat_ws(', ', sort_array(collect_set(case when status = 'active' then product end))) as active_products,
           min(start_date)                                                          as customer_since
    from {{ source('silver', 'silver_subscriptions') }}
    group by account_id
)
select
    a.account_id,
    a.account_name,
    a.segment,
    a.region,
    a.csm_owner,
    coalesce(s.current_mrr_usd, 0)            as current_mrr_usd,
    coalesce(s.current_mrr_usd, 0) * 12       as current_arr_usd,
    coalesce(s.active_subscriptions, 0)       as active_subscriptions,
    s.active_products,
    s.customer_since,
    case
        when s.active_subscriptions > 0 then 'active'
        when s.account_id is not null then 'churned'
        else 'prospect'
    end                                       as customer_status
from {{ source('silver', 'silver_accounts') }} a
left join subs s on s.account_id = a.account_id
