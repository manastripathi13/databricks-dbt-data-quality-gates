-- Sales pipeline: one row per opportunity with stage weighting.
select
    o.opportunity_id, o.account_id, c.segment, o.product, o.type, o.stage, o.amount_usd,
    case o.stage when 'Prospecting' then 0.1 when 'Qualification' then 0.2 when 'Proposal' then 0.4
                 when 'Negotiation' then 0.7 when 'Closed Won' then 1.0 else 0 end      as win_probability,
    round(o.amount_usd * case o.stage when 'Prospecting' then 0.1 when 'Qualification' then 0.2
          when 'Proposal' then 0.4 when 'Negotiation' then 0.7 when 'Closed Won' then 1.0 else 0 end, 2) as weighted_amount_usd,
    o.stage not in ('Closed Won', 'Closed Lost')                                            as is_open,
    o.close_date
from {{ source('silver', 'silver_opportunities') }} o
join {{ ref('dim_customer') }} c on c.account_id = o.account_id
