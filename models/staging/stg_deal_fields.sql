SELECT
    id AS field_id
    , field_key
    , name AS field_name
    , field_value_options
FROM {{ source('postgres_public', 'fields') }}