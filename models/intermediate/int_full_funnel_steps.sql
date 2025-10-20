-- Master funnel step reference combining stages and activity-based steps
WITH base_stages AS (
    SELECT
        stage_id,
        stage_name
    FROM {{ ref('stg_deal_stages') }}
),

with_steps AS (
    SELECT
        cast(stage_id AS text) AS stage_step,
        stage_name AS stage_kpi_name,
        stage_id AS stage_order,
        cast(stage_id AS text) AS stage_fk,
        '' AS activity_fk
    FROM base_stages
),

-- Add Sales Call 1 (meeting) and Sales Call 2 (sc_2) as activity-based steps
add_sales_calls AS (
    SELECT * FROM with_steps

    UNION ALL

    SELECT
        '2.1' AS stage_step,
        'Sales Call 1' AS stage_kpi_name,
        2.5 AS stage_order,
        '' AS stage_fk,
        'meeting' AS activity_fk

    UNION ALL

    SELECT
        '3.1' AS stage_step,
        'Sales Call 2' AS stage_kpi_name,
        3.5 AS stage_order,
        '' AS stage_fk,
        'sc_2' AS activity_fk
),

-- Renumber to create sequential stage_id
renumbered AS (
    SELECT
        stage_step,
        stage_kpi_name,
        row_number() OVER (ORDER BY stage_order) AS stage_order,
        stage_fk,
        activity_fk
    FROM add_sales_calls
)

SELECT
    stage_order AS stage_id,
    stage_step,
    stage_kpi_name,
    stage_fk,
    activity_fk
FROM renumbered
ORDER BY stage_id