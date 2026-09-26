select
    profile_id,
    campaign_id,
    {{ adapter.quote('date') }} as date_day,
    campaign_name,
    upper(campaign_status) as campaign_status,
    campaign_budget_amount,
    impressions,
    clicks,
    cost,
    purchases_7d,
    sales_7d,
    units_sold_7d,
    purchases_14d,
    sales_14d,
    _loaded_at as loaded_at
from {{ source('amazon_ads', 'sp_campaign_daily') }}
