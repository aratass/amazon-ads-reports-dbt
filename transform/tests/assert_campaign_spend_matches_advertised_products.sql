{#-
    The spCampaigns report and the spAdvertisedProduct report are generated separately.
    For every profile, campaign and day, campaign spend must equal the sum of that
    campaign's product ads, within `spend_reconciliation_tolerance` (0.5%) or one cent.

    Fails on a missing or partially loaded report window, a campaign present in only one
    report, or a join in the mart that duplicates or drops rows.
-#}

{%- set tolerance = var('spend_reconciliation_tolerance') -%}

with campaign_spend as (
    select profile_id, campaign_id, date_day, cost as campaign_cost
    from {{ ref('fct_sp_campaign_performance_daily') }}
),

product_spend as (
    select profile_id, campaign_id, date_day, sum(cost) as product_cost
    from {{ ref('stg_amazon_ads__sp_advertised_product_daily') }}
    group by profile_id, campaign_id, date_day
),

compared as (
    select
        coalesce(campaign_spend.profile_id, product_spend.profile_id) as profile_id,
        coalesce(campaign_spend.campaign_id, product_spend.campaign_id) as campaign_id,
        coalesce(campaign_spend.date_day, product_spend.date_day) as date_day,
        coalesce(campaign_spend.campaign_cost, 0) as campaign_cost,
        coalesce(product_spend.product_cost, 0) as product_cost
    from campaign_spend
    full outer join product_spend
        on product_spend.profile_id = campaign_spend.profile_id
        and product_spend.campaign_id = campaign_spend.campaign_id
        and product_spend.date_day = campaign_spend.date_day
)

select
    profile_id,
    campaign_id,
    date_day,
    campaign_cost,
    product_cost,
    product_cost - campaign_cost as difference
from compared
where abs(product_cost - campaign_cost) > greatest(0.01, {{ tolerance }} * abs(campaign_cost))
