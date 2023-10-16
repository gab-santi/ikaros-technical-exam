-- avg sessions per visitor = total session count / total visitor count
WITH total_session_count AS (
    SELECT
        1 AS id
        ,count_sessions
    FROM {{ ref('model_unique_sessions') }}
),

total_visitor_count AS (
    SELECT
        1 AS id
        ,count_visitor
    FROM {{ ref('model_unique_visitors') }}
)

SELECT
    sessions.count_sessions/visitors.count_visitor AS avg_sessions_per_visitor
FROM total_session_count sessions
LEFT JOIN total_visitor_count visitors
ON sessions.id = visitors.id
