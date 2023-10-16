-- get unique number of sessions
-- get distinct combinations of anonymous_id and session_id
SELECT
    COUNT(DISTINCT anonymous_id, session_id) AS count_sessions
FROM {{ ref('model_processed_data') }}