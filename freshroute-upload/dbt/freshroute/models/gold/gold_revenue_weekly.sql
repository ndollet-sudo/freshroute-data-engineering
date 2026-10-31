-- UC3 - CFO: billed delivery fees and revenue lost on failed attempts, in GBP and EUR (group reporting).
-- Grain: week_start x depot_id. Billed = fee of delivered parcels; lost = fee of refused / damaged / failed.
-- EUR uses the average ECB GBP->EUR rate of the week.
with weekly_fx as (
    select date_trunc('week', rate_date) as week_start, avg(gbp_to_eur) as gbp_to_eur
    from {{ ref('stg_fx_rates') }}
    group by 1
),

weekly as (
    select
        date_trunc('week', business_date)                              as week_start,
        depot_id,
        any_value(city)                                                as city,
        count(*)                                                       as deliveries_due,
        sum(iff(is_delivered, delivery_fee_gbp, 0))                    as delivery_fee_gbp,
        sum(iff(is_delivered, 0, delivery_fee_gbp))                    as lost_fee_gbp
    from {{ ref('fct_deliveries') }}
    group by 1, 2
)

select
    w.*,
    round(w.delivery_fee_gbp * fx.gbp_to_eur, 2)   as delivery_fee_eur,
    round(w.lost_fee_gbp * fx.gbp_to_eur, 2)       as lost_fee_eur,
    fx.gbp_to_eur
from weekly w
left join weekly_fx fx on fx.week_start = w.week_start
