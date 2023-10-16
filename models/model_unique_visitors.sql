SELECT 
    COUNT(DISTINCT concat_id) AS count_visitor
FROM {{ ref('model_processed_data' )}}