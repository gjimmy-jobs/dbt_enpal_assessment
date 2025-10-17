SELECT
    stage_id
    , stage_name
FROM {{ source('postgres_public', 'stages') }}