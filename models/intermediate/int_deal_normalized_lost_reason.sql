SELECT
    deal_id,
    deal_change_ts,
    deal_field_new_value::int AS deal_lost_reason,
    CASE
        WHEN deal_field_new_value = '1' THEN 'Customer Not Ready'
        WHEN deal_field_new_value = '2' THEN 'Pricing Issues'
        WHEN deal_field_new_value = '3' THEN 'Unreachable Customer'
        WHEN deal_field_new_value = '4' THEN 'Product Mismatch'
        WHEN deal_field_new_value = '5' THEN 'Duplicate Entry'
    END AS deal_lost_reason_name
FROM {{ ref('stg_deal_changes') }}
WHERE deal_changed_field = 'lost_reason'
