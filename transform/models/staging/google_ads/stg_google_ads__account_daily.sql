select
    customer_id,
    {{ adapter.quote('date') }} as date_day,
    account_name,
    currency_code,
    time_zone,
    impressions,
    clicks,
    cost_micros,
    cost,
    conversions,
    conversions_value,
    _loaded_at as loaded_at
from {{ source('google_ads', 'account_daily') }}
