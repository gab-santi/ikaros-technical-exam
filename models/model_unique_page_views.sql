SELECT
    COUNT(DISTINCT pageview_id) AS num_views
FROM {{ ref('model_processed_data') }}