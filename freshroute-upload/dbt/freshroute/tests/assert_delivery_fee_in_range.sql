-- Business rule: after repair (pence -> pounds, sign), a home grocery delivery fee is between 0 and 50 GBP.
select delivery_id, delivery_fee_gbp
from {{ ref('silver_deliveries') }}
where delivery_fee_gbp < 0 or delivery_fee_gbp > 50
