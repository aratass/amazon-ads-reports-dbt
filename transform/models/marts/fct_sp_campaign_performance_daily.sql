{{
    config(
        partition_by={'field': 'date_day', 'data_type': 'date'} if target.type == 'bigquery' else none,
        cluster_by=['profile_id', 'campaign_id'] if target.type == 'bigquery' else none,
    )
}}

with campaigns as (
    select * from {{ ref('stg_amazon_ads__sp_campaign_daily') }}
),

profiles as (
    select * from {{ ref('stg_amazon_ads__profiles') }}
)

select
    campaigns.date_day,
    campaigns.profile_id,
    profiles.country_code,
    profiles.marketplace_id,
    profiles.currency_code,
    campaigns.campaign_id,
    campaigns.campaign_name,
    campaigns.campaign_status,
    campaigns.campaign_budget_amount,
    campaigns.impressions,
    campaigns.clicks,
    campaigns.cost,
    campaigns.purchases_7d,
    campaigns.sales_7d,
    campaigns.units_sold_7d,
    campaigns.purchases_14d,
    campaigns.sales_14d,
    campaigns.clicks / nullif(campaigns.impressions, 0) as ctr,
    campaigns.cost / nullif(campaigns.clicks, 0) as cpc,
    campaigns.purchases_7d / nullif(campaigns.clicks, 0) as conversion_rate_7d,
    campaigns.cost / nullif(campaigns.sales_7d, 0) as acos_7d,
    campaigns.sales_7d / nullif(campaigns.cost, 0) as roas_7d,
    campaigns.cost / nullif(campaigns.campaign_budget_amount, 0) as budget_utilization
from campaigns
left join profiles
    on profiles.profile_id = campaigns.profile_id
