-- Every bronze delivery, typed, with the list of problems found on the row.
-- silver_deliveries keeps the good rows, deliveries_quarantine the others: nothing is lost.
{{ config(materialized='view') }}

with typed as (
    select
        d.*,
        try_to_number({{ clean_text('d.delivery_id') }})   as delivery_id_n,
        try_to_number({{ clean_text('d.route_id') }})      as route_id_n,
        upper({{ clean_text('d.customer_ref') }})          as customer_ref_c,
        {{ clean_ts('d.promised_by') }}                    as promised_ts,
        {{ clean_ts('d.delivered_at') }}                   as delivered_ts,
        {{ clean_float('d.parcel_kg') }}                   as parcel_kg_n,
        {{ clean_money('regexp_replace(d.delivery_fee_gbp, \'[^0-9,.-]\', \'\')') }} as fee_raw,
        m.clean_value                                      as outcome_c
    from {{ source('bronze', 'deliveries') }} d
    left join {{ ref('category_mapping') }} m
           on m.domain = 'outcome' and m.raw_value = {{ normalise_category('d.outcome') }}
)

select
    x.*,
    array_construct_compact(
        iff(delivery_id_n is null,                         'missing delivery_id', null),
        iff(r.route_id is null,                            'unknown route_id (orphan)', null),
        iff(outcome_c is null,                             'outcome missing or unknown', null),
        iff(promised_ts is null,                           'promised_by not readable', null),
        iff(outcome_c = 'delivered' and delivered_ts is null, 'delivered without delivered_at', null),
        iff(fee_raw is null,                               'delivery fee not readable', null)
    ) as problems
from typed x
left join {{ ref('stg_routes') }} r on r.route_id = x.route_id_n
