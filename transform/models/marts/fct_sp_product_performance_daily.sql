{{
    config(
        partition_by={'field': 'date_day', 'data_type': 'date'} if target.type == 'bigquery' else none,
        cluster_by=['profile_id', 'advertised_asin'] if target.type == 'bigquery' else none,
    )
}}

-- One row per advertised ASIN per day. The same ASIN is often advertised from several
-- campaigns and ad groups; this adds them up.

with products as (
    select * from {{ ref('stg_amazon_ads__sp_advertised_product_daily') }}
),

profiles as (
    select * from {{ ref('stg_amazon_ads__profiles') }}
)

select
    products.date_day,
    products.profile_id,
    products.advertised_asin,
    profiles.currency_code,
    count(distinct products.ad_id) as ads,
    count(distinct products.campaign_id) as campaigns,
    sum(products.impressions) as impressions,
    sum(products.clicks) as clicks,
    sum(products.cost) as cost,
    sum(products.purchases_7d) as purchases_7d,
    sum(products.sales_7d) as sales_7d,
    sum(products.units_sold_7d) as units_sold_7d,
    sum(products.cost) / nullif(sum(products.sales_7d), 0) as acos_7d,
    sum(products.sales_7d) / nullif(sum(products.cost), 0) as roas_7d
from products
left join profiles
    on profiles.profile_id = products.profile_id
group by
    products.date_day,
    products.profile_id,
    products.advertised_asin,
    profiles.currency_code
