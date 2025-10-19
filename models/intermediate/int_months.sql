WITH date_range AS (
    SELECT
        MIN(min_date) AS min_date,
        MAX(max_date) AS max_date
    FROM (
        SELECT
            MIN(activity_due_date) AS min_date,
            MAX(activity_due_date) AS max_date
        FROM {{ ref('stg_activities') }}

        UNION ALL

        SELECT
            MIN(deal_change_ts) AS min_date,
            MAX(deal_change_ts) AS max_date
        FROM {{ ref('stg_deal_changes') }}
    )
),

month_series AS (
    SELECT
        DATE_TRUNC(
            'month',
            date_range.min_date + (INTERVAL '1 month' * generate_subscripts)
        ) AS dt_month
    FROM date_range,
        LATERAL GENERATE_SERIES(
            0,
            EXTRACT(
                YEAR FROM AGE(
                    DATE_TRUNC('month', date_range.max_date),
                    DATE_TRUNC('month', date_range.min_date)
                )
            )
            * 12
            + EXTRACT(
                MONTH FROM AGE(
                    DATE_TRUNC('month', date_range.max_date),
                    DATE_TRUNC('month', date_range.min_date)
                )
            )::INT
        ) AS generate_subscripts
)

SELECT
    dt_month,
    DATE_PART('year', dt_month) AS dt_year,
    DATE_PART('month', dt_month) AS dt_month_num,
    TO_CHAR(dt_month, 'YYYY-MM') AS dt_month_string
FROM month_series
ORDER BY dt_month
