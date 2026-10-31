-- Rows that would make a business number wrong, kept with the reason (never silently dropped).
select
    delivery_id, route_id, promised_by, delivered_at, delivery_fee_gbp, outcome,
    _batch, _run_id, _loaded_at, problems
from {{ ref('int_deliveries_checked') }}
where array_size(problems) > 0
