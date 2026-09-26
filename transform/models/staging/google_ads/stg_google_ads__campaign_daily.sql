select
    customer_id,
    campaign_id,
    {{ adapter.quote('date') }} as date_day,
    campaign_name,
    campaign_status,
    advertising_channel_type,
    impressions,
    clicks,
    cost_micros,
    cost,
    conversions,
    conversions_value,
    _loaded_at as loaded_at
from {{ source('google_ads', 'campaign_daily') }}
