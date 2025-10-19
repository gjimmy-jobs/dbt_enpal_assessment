SELECT
    act.activity_id,
    lower(act_ty.is_activity_active) = 'yes' AS is_activity_active,
    act.activity_user,
    act.deal_id,
    act.is_activity_done,
    act.activity_due_date,
    act.activity_type
FROM {{ ref('stg_activities') }} AS act
LEFT JOIN {{ ref('stg_activity_types') }} AS act_ty
    ON act.activity_type = act_ty.activity_type
