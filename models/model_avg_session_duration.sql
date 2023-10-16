-- average session duration, in minutes
-- get all sessions per user
-- with min and max timestamps
WITH user_sessions_min_max AS (
    SELECT
        anonymous_id
        ,session_id
        ,MIN(timestamp) AS min_session_timestamp
        ,MAX(timestamp) AS max_session_timestamp
    FROM {{ ref('model_processed_data') }}
    GROUP BY anonymous_id, session_id
),

-- compute session durations
user_sessions_durations AS (
    SELECT
        anonymous_id
        ,session_id
        ,DATEDIFF('m', min_session_timestamp, max_session_timestamp) AS session_duration
    FROM user_sessions_min_max
)

-- get overall average
SELECT
    AVG(session_duration) AS avg_session_duration
FROM user_sessions_durations