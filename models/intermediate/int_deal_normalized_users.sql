SELECT
    dc.deal_id,
    dc.deal_change_ts,
    u.user_id AS deal_user_id,
    u.user_name AS deal_user_name,
    u.user_email AS deal_user_email
FROM {{ ref('stg_deal_changes') }} AS dc
LEFT JOIN {{ ref("stg_users") }} AS u
    ON dc.deal_field_new_value = cast(u.user_id AS text)
WHERE deal_changed_field = 'user_id'
