-- Deal creation timestamp
SELECT
    deal_id,
    deal_change_ts,
    deal_change_ts AS deal_add_time
FROM {{ ref('stg_deal_changes') }}
WHERE deal_changed_field = 'add_time'