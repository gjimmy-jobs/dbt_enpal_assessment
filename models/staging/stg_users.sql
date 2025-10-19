SELECT
    id AS user_id,
    name AS user_name,
    email AS user_email,
    modified AS user_modified_ts
FROM {{ source('postgres_public', 'users') }}
