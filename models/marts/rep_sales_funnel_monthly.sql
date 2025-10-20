-- Monthly sales funnel reporting: deal counts by funnel step and month
WITH stages AS (
    -- Aggregate stage-based funnel steps by month
    SELECT
        to_char(ds.stage_month, 'YYYY-MM') AS dt_month_string,
        ds.stage_kpi_name,
        ds.stage_id,
        count(DISTINCT ds.deal_id) AS cnt_deals
    FROM {{ ref('int_deal_normalized_stages') }} AS ds
    GROUP BY dt_month_string, ds.stage_kpi_name, ds.stage_id
),

activities AS (
    -- Aggregate activity-based funnel steps (Sales Call 1 & 2) by month
    SELECT
        to_char(act.activity_due_month, 'YYYY-MM') AS dt_month_string,
        act.stage_kpi_name,
        act.stage_id,
        count(DISTINCT act.deal_id) AS cnt_deals
    FROM {{ ref('int_activities_normalized_relevant') }} AS act
    GROUP BY dt_month_string, act.stage_kpi_name, act.stage_id
),

months_stages AS (
    -- Create complete spine of all month-stage combinations
    SELECT
        m.dt_month_string,
        s.stage_id
    FROM {{ ref('int_months') }} AS m
    CROSS JOIN
        (SELECT DISTINCT stage_id FROM {{ ref('int_full_funnel_steps') }}) AS s
)

SELECT
    ms.dt_month_string AS month,
    ffs.stage_kpi_name AS kpi_name,
    ffs.stage_step AS funnel_step,
    coalesce(s.cnt_deals, 0) + coalesce(act.cnt_deals, 0) AS deals_count
FROM months_stages AS ms
LEFT JOIN
    (
        SELECT stage_id, stage_kpi_name, stage_step
        FROM {{ ref('int_full_funnel_steps') }}
    ) AS ffs
    ON ms.stage_id = ffs.stage_id
LEFT JOIN stages AS s
    ON
        ms.dt_month_string = s.dt_month_string
        AND ms.stage_id = s.stage_id
LEFT JOIN activities AS act
    ON
        ms.dt_month_string = act.dt_month_string
        AND ms.stage_id = act.stage_id