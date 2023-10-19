-- unique visitors = number of unique users, which we get via counting the final_id
-- 1358
SELECT 
    COUNT(DISTINCT final_id) AS count_visitor
FROM {{ ref('model_processed_data') }}