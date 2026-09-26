select
    customer_id,
    campaign_id,
    snapshot_date,
    campaign_status,
    budget_id,
    budget_name,
    budget_period,
    budget_amount_micros,
    budget_amount,
    budget_is_shared,
    _loaded_at as loaded_at
from {{ source('google_ads', 'campaign_budget_snapshot') }}
