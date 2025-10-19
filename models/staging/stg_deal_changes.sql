SELECT
    deal_id,
    change_time AS deal_change_ts,
    changed_field_key AS deal_changed_field,
    new_value AS deal_field_new_value
FROM {{ source('postgres_public', 'deal_changes') }}
