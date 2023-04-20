WITH PLAYS AS ( 
SELECT 
    content_type,
    content_id,
    COUNT(DISTINCT event_id) total_plays
FROM 
    plays
WHERE 
    lower(referrer) = 'pushnotif'
    AND platform IN ('app-android', 'app-ios')
    AND ts >= {{date_start}}
    AND ts < {{date_end}}
    AND NOT is_yesterday
GROUP BY 1,2
),

EVENTS AS (
    SELECT 
        MIN(cast(TIME AS TIMESTAMP)) AS TIME,
        notif_title,
        notif_message,
        action,
        segment_name,
        content_type,
        content_id,
        COUNT(DISTINCT id) AS total_events,
        COUNT(DISTINCT visitor_id) AS total_visitors
    FROM 
        push_notif
    WHERE ts >=  {{date_start}}
        AND ts < {{date_end}}
        AND platform IN ('app-android', 'app-ios')
    GROUP BY 2,3,4,5,6,7
),

events_agg AS ( 
    SELECT min(TIME) AS time_wib,
    notif_title,
    notif_message,
    MAX(segment_name) segment_name,
    MAX(content_type) content_type,
    MAX(content_id) content_id,
    SUM(CASE WHEN action='sent user' THEN total_events END) total_sent,
    SUM(CASE WHEN action='received' THEN total_events END) total_received,
    SUM(CASE WHEN action='shown' THEN total_events END) total_shown,
    SUM(CASE WHEN action='open' THEN total_events END) total_open
FROM 
    EVENTS
GROUP BY 2,3
),

pn as (
SELECT ea.time_wib,
      ea.notif_title,
      ea.notif_message,
      ea.segment_name,
      ea.total_sent,
      ea.total_received,
      ea.total_shown,
      ea.total_open,
      p.total_plays
FROM 
    events as ea
LEFT JOIN 
    plays p ON ea.content_type = p.content_type
    AND ea.content_id = p.content_id
WHERE 
    ea.total_sent > 0 OR ea.total_open > 100
ORDER BY ea.time_wib DESC
),

result as (
SELECT
    *
FROM
    pn
WHERE
    NOT LOWER(pn.notif_title) LIKE '%test%' 
    AND NOT LOWER(pn.notif_title) LIKE '%pembayaran anda%'
    AND NOT LOWER(notif_message) LIKE '%film spesial untukmu%'
    AND NOT LOWER(segment_name) LIKE '%quiz:result%'
    AND NOT LOWER(segment_name) LIKE '%pushnotif-byw%'
    AND NOT segment_name in (
            'discovery_offering_day_0',
            'discovery_offering_day_1',
            'discovery_offering_day_3',
            'almost_avid_day_1',
            'almost_avid_day_3',
            'almost_avid_v2_date_21',
            'almost_avid_v2_date_26',
            'almost_avid_v2_date_30',
            'one_timer_date_6',
            'frequent_watcher_date_6'
            )
)

SELECT
    *
FROM
    result
[[WHERE UPPER(notif_title) LIKE UPPER(CONCAT('%', {{search}}, '%'))]]

