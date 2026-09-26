select
    customer_id,
    campaign_id,
    ad_group_id,
    {{ adapter.quote('date') }} as date_day,
    ad_group_name,
    ad_group_status,
    ad_group_type,
    impressions,
    clicks,
    cost_micros,
    cost,
    conversions,
    conversions_value,
    _loaded_at as loaded_at
from {{ source('google_ads', 'ad_group_daily') }}
