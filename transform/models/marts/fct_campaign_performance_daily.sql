{{
    config(
        partition_by={'field': 'date_day', 'data_type': 'date'} if target.type == 'bigquery' else none,
        cluster_by=['customer_id', 'campaign_id'] if target.type == 'bigquery' else none,
    )
}}

with campaigns as (
    select * from {{ ref('stg_google_ads__campaign_daily') }}
),

-- Latest known name, currency and time zone of each account.
accounts as (
    select customer_id, account_name, currency_code, time_zone
    from (
        select
            customer_id,
            account_name,
            currency_code,
            time_zone,
            row_number() over (partition by customer_id order by date_day desc) as recency
        from {{ ref('stg_google_ads__account_daily') }}
    ) as ranked
    where recency = 1
)

select
    campaigns.date_day,
    campaigns.customer_id,
    accounts.account_name,
    accounts.currency_code,
    campaigns.campaign_id,
    campaigns.campaign_name,
    campaigns.campaign_status,
    campaigns.advertising_channel_type,
    campaigns.impressions,
    campaigns.clicks,
    campaigns.cost,
    campaigns.conversions,
    campaigns.conversions_value,
    campaigns.clicks / nullif(campaigns.impressions, 0) as ctr,
    campaigns.cost / nullif(campaigns.clicks, 0) as cpc,
    campaigns.cost / nullif(campaigns.conversions, 0) as cost_per_conversion,
    campaigns.conversions_value / nullif(campaigns.cost, 0) as roas
from campaigns
left join accounts
    on accounts.customer_id = campaigns.customer_id
