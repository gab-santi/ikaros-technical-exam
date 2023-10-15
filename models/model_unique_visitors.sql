SELECT 
    COUNT(DISTINCT concat_id) AS unique_visitor_count
FROM {{ ref('model_processed_data' )}}