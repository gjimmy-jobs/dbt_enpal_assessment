SELECT
    id AS activity_type_id,
    name AS activity_type_name,
    active AS is_activity_active,
    type AS activity_type
FROM {{ source('postgres_public', 'activity_types') }}
