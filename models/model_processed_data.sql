-- do all data preprocessing here
-- steps done:
-- 1. get distinct rows
-- 2. get latest user_id for each anonymous_id and assign to each row
-- 3. add pageview_id to identify page views
-- 4. add session_id to identify user sessions

-- get distinct rows
WITH data_distinct AS (
    SELECT 
        DISTINCT 
            timestamp
            ,user_id
            ,anonymous_id
        FROM IKAROS.IKAROS_SCHEMA.PAGES
        ORDER BY timestamp ASC
),
-- session id generation
-- session: successive page views
-- if 30 mins have passed without activity, current session will be ended and a new one will be spawned
anon_id_with_session AS (
    SELECT
        anonymous_id
        ,timestamp
        ,1 + sum(IFF(DATEDIFF('minute', lag, timestamp) > 30, 1, 0)) over ( partition by anonymous_id order by timestamp ) as session_id
    FROM (
        SELECT *,
            lag(timestamp) over ( partition by anonymous_id order by timestamp ) lag -- previous row event_time
        FROM data_distinct
    )
),
-- anonymous_id with max timestamp
anon_id_max_timestamp AS (
    SELECT
        anonymous_id
        ,MAX(timestamp) AS max_timestamp
    FROM data_distinct
    GROUP BY anonymous_id
),
-- user id at max timestamp
anon_id_with_latest_user_id AS (
   SELECT
       t1.anonymous_id AS anonymous_id
       ,t1.max_timestamp AS timestamp
       ,t2.user_id AS user_id
    FROM anon_id_max_timestamp t1
    LEFT JOIN data_distinct t2
    ON t1.anonymous_id = t2.anonymous_id
    AND t1.max_timestamp = t2.timestamp
),
-- rebuild inital data latest new user_id
data_with_latest_user_id AS (
    SELECT 
        t1.timestamp AS timestamp
        ,t2.user_id AS user_id
        ,t1.anonymous_id
    FROM data_distinct t1
    LEFT JOIN anon_id_with_latest_user_id t2
    ON t1.anonymous_id = t2.anonymous_id
),
-- distinct data with pageview_id with latest user_id
data_distinct_with_pageview_id AS (
    SELECT
        ROW_NUMBER() OVER (
            ORDER BY timestamp, user_id, anonymous_id
        ) AS pageview_id
        ,timestamp
        ,user_id
        ,anonymous_id
        --,CONCAT(COALESCE(user_id, 'NULL'),'_', COALESCE(anonymous_id, 'NULL')) AS concat_id
        ,CASE
            WHEN user_id IS NULL THEN anonymous_id
            ELSE user_id
        END
        AS final_id
    FROM data_with_latest_user_id
),
-- add session_id
data_distinct_with_session_id AS (
    SELECT
        t1.pageview_id
        ,t1.timestamp
        ,t1.user_id
        ,t1.anonymous_id
        ,t1.final_id
        ,t2.session_id
    FROM data_distinct_with_pageview_id t1
    LEFT JOIN anon_id_with_session t2
    ON t1.timestamp = t2.timestamp
    AND t2.anonymous_id = t2.anonymous_id
)

SELECT
    pageview_id
    ,timestamp
    ,user_id
    ,anonymous_id
    ,final_id
    ,session_id
FROM data_distinct_with_session_id
ORDER BY anonymous_id, session_id