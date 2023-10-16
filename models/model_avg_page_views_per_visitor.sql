-- avg page views per visitor = total page view count / total visitor count
WITH total_view_count AS (
    SELECT
        1 AS id
        ,count_views
    FROM {{ ref('model_unique_page_views') }}
),

total_visitor_count AS (
    SELECT
        1 AS id
        ,count_visitor
    FROM {{ ref('model_unique_visitors') }}
)

SELECT
    views.count_views/visitors.count_visitor AS avg_page_views_per_visitor
FROM total_view_count views
LEFT JOIN total_visitor_count visitors
ON views.id = visitors.id
