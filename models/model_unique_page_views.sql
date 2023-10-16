SELECT
    COUNT(DISTINCT pageview_id) AS count_views
FROM {{ ref('model_processed_data') }}