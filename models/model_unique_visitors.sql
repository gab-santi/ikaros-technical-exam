-- unique visitors = number of unique users, which we get via concat_id (latest user_id+anon_id)
SELECT 
    COUNT(DISTINCT concat_id) AS count_visitor
FROM {{ ref('model_processed_data' )}}