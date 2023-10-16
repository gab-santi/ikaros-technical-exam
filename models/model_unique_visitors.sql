-- unique visitors = number of unique users, which we get via counting the anon_id
-- 1358
SELECT 
    COUNT(DISTINCT anonymous_id) AS count_visitor
FROM {{ ref('model_processed_data') }}