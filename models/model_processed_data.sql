-- do all data preprocessing here
-- get distinct values
WITH data_distinct AS (
    SELECT 
        DISTINCT 
            timestamp
            ,user_id
            ,anonymous_id
        FROM IKAROS.IKAROS_SCHEMA.PAGES
        ORDER BY timestamp ASC
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
-- rebuild inital data with new timestamp
data_with_latest_user_id AS (
    SELECT 
        t1.timestamp AS timestamp
        ,t2.user_id AS user_id
        ,t1.anonymous_id
    FROM data_distinct t1
    LEFT JOIN anon_id_with_latest_user_id t2
    ON t1.anonymous_id = t2.anonymous_id
),
-- distinct data with ID
data_distinct_with_ids AS (
    SELECT
        ROW_NUMBER() OVER (
            ORDER BY timestamp, user_id, anonymous_id
        ) AS pageview_id
        ,timestamp
        ,user_id
        ,anonymous_id
        ,CONCAT(COALESCE(user_id, 'NULL'),'_', COALESCE(anonymous_id, 'NULL')) AS concat_id
        -- TODO add session ID
        
    FROM data_with_latest_user_id
)

SELECT
    pageview_id
    ,timestamp
    ,user_id
    ,anonymous_id
    ,concat_id
FROM data_distinct_with_ids
ORDER BY pageview_id