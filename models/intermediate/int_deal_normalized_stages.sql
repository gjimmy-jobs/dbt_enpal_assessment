SELECT
    dc.deal_id,
    ffs.stage_id,
    dc.deal_field_new_value AS stage_step,
    ffs.stage_kpi_name,
    dc.deal_change_ts AS stage_start_ts,
    DATE_TRUNC('month', dc.deal_change_ts) AS stage_month
FROM {{ ref('stg_deal_changes') }} AS dc
LEFT JOIN {{ ref('int_full_funnel_steps') }} AS ffs
    ON deal_field_new_value = ffs.stage_fk
WHERE deal_changed_field = 'stage_id'
