-- unique page views = number of unique rows or pageview_ids in the data
SELECT
    COUNT(DISTINCT pageview_id) AS count_views
FROM {{ ref('model_processed_data') }}