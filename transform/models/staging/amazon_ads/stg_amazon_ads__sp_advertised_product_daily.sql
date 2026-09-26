select
    profile_id,
    campaign_id,
    ad_group_id,
    ad_id,
    {{ adapter.quote('date') }} as date_day,
    ad_group_name,
    advertised_asin,
    advertised_sku,
    impressions,
    clicks,
    cost,
    purchases_7d,
    sales_7d,
    units_sold_7d,
    purchases_14d,
    sales_14d,
    _loaded_at as loaded_at
from {{ source('amazon_ads', 'sp_advertised_product_daily') }}
