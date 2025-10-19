SELECT
    activity_id,
    type AS activity_type,
    assigned_to_user AS activity_user,
    deal_id,
    done AS is_activity_done,
    due_to AS activity_due_date
FROM {{ source('postgres_public', 'activity') }}
