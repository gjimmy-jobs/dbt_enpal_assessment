-- Completed sales call activities (meeting, sc_2) linked to funnel steps
SELECT
    act.activity_id,
    act.deal_id,
    act.activity_due_date,
    act.activity_user,
    DATE_TRUNC('month', act.activity_due_date) AS activity_due_month,
    ffs.stage_step,
    ffs.stage_id,
    ffs.stage_kpi_name
FROM {{ ref('int_activities_normalized') }} AS act
LEFT JOIN {{ ref('int_full_funnel_steps') }} AS ffs
    ON act.activity_type = ffs.activity_fk
WHERE
    act.activity_type IN ('meeting', 'sc_2')
    AND act.is_activity_done
    AND act.is_activity_active