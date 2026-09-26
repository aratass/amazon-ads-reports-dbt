-- The latest known state of each advertiser profile.
select
    profile_id,
    region,
    country_code,
    currency_code,
    timezone,
    marketplace_id,
    account_id,
    account_type,
    account_name,
    snapshot_date,
    _loaded_at as loaded_at
from (
    select
        *,
        row_number() over (partition by profile_id order by snapshot_date desc) as recency
    from {{ source('amazon_ads', 'profiles') }}
) as ranked
where recency = 1
