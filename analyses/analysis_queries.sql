-- Are there activity records assigned to non-existing users?
-- Expected: no orphaned activities
SELECT *
FROM public.activity AS act
LEFT JOIN public.users AS u
    ON act.assigned_to_user = u.id
WHERE coalesce(u.name, '') = '';

-- Are there stage changes assigned to non-existing users?
-- Expected: no orphaned stage changes
SELECT *
FROM public.deal_changes AS dc
LEFT JOIN public.users AS u
    ON dc.new_value = cast(u.id AS text)
WHERE
    coalesce(u.name, '') = ''
    AND dc.changed_field_key = 'user_id';

-- Are there duplicated names in users table?
-- Result: yes, 21 duplicated names - to be investigated
SELECT
    name,
    count(*) AS cnt
FROM public.users
GROUP BY name
HAVING count(*) > 1;  

-- Are there duplicated emails in users table?
-- Expected: yes, 4 duplicated emails - to be investigated
SELECT
    email,
    count(*) AS cnt
FROM public.users
GROUP BY email
HAVING count(*) > 1;  


-- Count distinct deals in deal stages
-- Result: 1995
SELECT count(DISTINCT deal_id) AS count_distinct_deals
FROM public_pipedrive_analytics.stg_deal_changes;

-- Count distinct deals in deal activities
-- Result: 4572
SELECT count(DISTINCT deal_id) AS count_distinct_deals
FROM public_pipedrive_analytics.stg_activities;

-- What is the overlap between activity and deal_changes in terms of deal_id?
-- Result: only 8 common deal_ids - significant data quality issue
WITH a AS (
    SELECT DISTINCT deal_id
    FROM public_pipedrive_analytics.stg_activities
)

SELECT
    CASE
        WHEN dc.deal_id IS NULL THEN 'Activity deal_id only'
        WHEN a.deal_id IS NULL THEN 'Stage change deal_id only'
        ELSE 'Both'
    END AS overlap_type,
    count(*) AS cnt
FROM (
    SELECT DISTINCT deal_id
    FROM public_pipedrive_analytics.stg_deal_changes
) AS dc
FULL OUTER JOIN a ON dc.deal_id = a.deal_id
GROUP BY 1;

-- What is the overlap between activity and deal_changes in terms of user_id?
WITH a AS (
    SELECT DISTINCT activity_user AS user_id
    FROM public_pipedrive_analytics.stg_activities
)

SELECT
    CASE
        WHEN dc.user_id IS NULL THEN 'Activity user_id only'
        WHEN a.user_id IS NULL THEN 'Stage change user_id only'
        ELSE 'Both'
    END AS overlap_type,
    count(*) AS cnt
FROM (
    SELECT DISTINCT cast(deal_field_new_value AS integer) AS user_id
    FROM public_pipedrive_analytics.stg_deal_changes
    WHERE deal_changed_field = 'user_id'
) AS dc
FULL OUTER JOIN a ON dc.user_id = a.user_id
GROUP BY overlap_type;


--=====================================
--    VALIDATE INTEGRITY OF STAGES   --
--=====================================

-- How many deals are there that have no stage?
-- Result: none
WITH deal_with_stages AS (
    SELECT DISTINCT deal_id
    FROM public_pipedrive_analytics.stg_deal_changes
    WHERE deal_changed_field = 'stage_id'
),

unique_deals AS (
    SELECT DISTINCT deal_id
    FROM public_pipedrive_analytics.stg_deal_changes
)

SELECT
    sum(CASE WHEN dws.deal_id IS NOT NULL THEN 1 ELSE 0 END)
        AS cnt_deals_with_stages,
    sum(CASE WHEN dws.deal_id IS NULL THEN 1 ELSE 0 END) AS cnt_orphan_deals
FROM unique_deals AS ud
LEFT JOIN deal_with_stages AS dws ON ud.deal_id = dws.deal_id;

-- How many deals started at stage 1?
-- Result: 1995
WITH first_stage AS (
    SELECT
        deal_id,
        deal_field_new_value,
        row_number()
            OVER (
                PARTITION BY deal_id, deal_changed_field
                ORDER BY deal_change_ts ASC
            )
            AS rn
    FROM public_pipedrive_analytics.stg_deal_changes
    WHERE deal_changed_field = 'stage_id'
)

SELECT
    deal_field_new_value AS initial_deal_stage,
    count(*) AS cnt
FROM first_stage
WHERE rn = 1
GROUP BY deal_field_new_value;

-- How many deals were lost?
-- Result: all of them
WITH lost_deals AS (
    SELECT DISTINCT deal_id
    FROM public_pipedrive_analytics.stg_deal_changes
    WHERE deal_changed_field = 'lost_reason'
),

unique_deals AS (
    SELECT DISTINCT deal_id
    FROM public_pipedrive_analytics.stg_deal_changes
)

SELECT
    sum(CASE WHEN ld.deal_id IS NOT NULL THEN 1 ELSE 0 END) AS cnt_lost_deals,
    sum(CASE WHEN ld.deal_id IS NULL THEN 1 ELSE 0 END) AS cnt_non_lost_deals
FROM unique_deals AS ud
LEFT JOIN lost_deals AS ld ON ud.deal_id = ld.deal_id;

-- What is the last change for every deal?
-- Result: lost_reason is the most common final change
SELECT
    deal_changed_field AS last_deal_change_key_field,
    count(*) AS cnt_deals
FROM public_pipedrive_analytics.stg_deal_changes
WHERE (deal_id, deal_change_ts) IN (
    SELECT deal_id, max(deal_change_ts)
    FROM public_pipedrive_analytics.stg_deal_changes
    GROUP BY deal_id
)
GROUP BY deal_changed_field;

-- Are there deals that have more than 1 lost_reason?
-- Result: 5 deals have multiple lost_reason records - these are duplicated deal_ids
SELECT
    deal_id,
    count(*) AS cnt
FROM public_pipedrive_analytics.stg_deal_changes
WHERE deal_changed_field = 'add_time'
GROUP BY deal_id
HAVING count(*) > 1;

-- Check if there are deals in deal_changes where stage_ids are not in chronological order
-- Result: yes, but only for duplicated deal_ids
SELECT
    deal_id,
    count(*) AS count_violations
FROM (
    SELECT
        deal_id,
        cast(deal_field_new_value AS int) AS current_stage,
        lag(cast(deal_field_new_value AS int))
            OVER (PARTITION BY deal_id ORDER BY deal_change_ts)
            AS prev_stage,
        deal_change_ts
    FROM public_pipedrive_analytics.stg_deal_changes
    WHERE deal_changed_field = 'stage_id'
) AS staged
WHERE current_stage < prev_stage
GROUP BY deal_id
HAVING count(*) > 0
ORDER BY count_violations DESC;

-- Check if user_ids are always assigned prior to stages
-- Result: true for all deals except those with duplicates (expected behavior)
WITH d2 AS (
    SELECT
        deal_id,
        min(deal_change_ts) AS min_non_add_time
    FROM public_pipedrive_analytics.stg_deal_changes
    WHERE deal_changed_field NOT IN ('user_id', 'add_time')
    GROUP BY deal_id
)

SELECT
    d1.deal_id,
    d1.max_add_time,
    d2.min_non_add_time,
    d1.max_add_time > d2.min_non_add_time AS add_time_comes_later
FROM (
    SELECT
        deal_id,
        max(deal_change_ts) AS max_add_time
    FROM public_pipedrive_analytics.stg_deal_changes
    WHERE deal_changed_field = 'user_id'
    GROUP BY deal_id
) AS d1
LEFT JOIN d2 ON d1.deal_id = d2.deal_id
WHERE d1.max_add_time > d2.min_non_add_time;
