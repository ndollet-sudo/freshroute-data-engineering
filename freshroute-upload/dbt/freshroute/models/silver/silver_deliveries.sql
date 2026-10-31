-- Clean deliveries. Grain: one row per delivery_id.
-- Which version wins? The LATEST BATCH (a later batch is an update of the delivery), then the latest pull,
-- then deterministic tie-breakers so that two runs on the same data always give the same result.
select
    delivery_id_n                                               as delivery_id,
    route_id_n                                                  as route_id,
    customer_ref_c                                              as customer_ref,
    promised_ts                                                 as promised_by,
    delivered_ts                                                as delivered_at,
    iff(parcel_kg_n > 0 and parcel_kg_n <= 100, parcel_kg_n, null) as parcel_kg,    -- not used in a KPI: NULL, not quarantine
    iff(abs(fee_raw) > 50, abs(fee_raw) / 100, abs(fee_raw))    as delivery_fee_gbp, -- pence typed as pounds (x100)
    fee_raw < 0                                                 as fee_sign_error,
    outcome_c                                                   as outcome,
    coalesce({{ clean_text('source_system') }}, 'legacy')       as source_system,
    _batch,
    _run_id,
    _loaded_at
from {{ ref('int_deliveries_checked') }}
where array_size(problems) = 0
qualify row_number() over (
    partition by delivery_id_n
    order by _batch desc, _pg_loaded_at desc, iff(abs(fee_raw) between 0.5 and 50, 0, 1), fee_raw, delivered_ts
) = 1
