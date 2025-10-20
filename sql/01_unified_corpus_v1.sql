-- sql/01_unified_corpus_v1.sql
CREATE OR REPLACE VIEW `OriginBGEstuary.analytics.unified_corpus_v1` AS
WITH slack AS (
  SELECT
    CONCAT('slack:', channel_id, ':', CAST(ts AS STRING)) AS doc_id,
    'slack' AS source,
    channel_id,
    thread_ts,
    TIMESTAMP_SECONDS(CAST(ts AS INT64)) AS created_at,
    user AS author_id,
    REPLACE(REGEXP_REPLACE(COALESCE(text,''), r'\s+', ' '), '&nbsp;', ' ') AS text,
    CASE WHEN is_private THEN CONCAT('slack:private:', channel_id)
         ELSE CONCAT('slack:public:', channel_id) END AS permission_scope
  FROM `OriginBGEstuary.originslack.channel_messages`
  WHERE COALESCE(text,'') <> ''
),
hubspot AS (
  SELECT
    CONCAT('hubspot:', CAST(id AS STRING)) AS doc_id,
    'hubspot' AS source,
    NULL AS channel_id,
    NULL AS thread_ts,
    TIMESTAMP_MICROS(createdate) AS created_at,
    CAST(ownerid AS STRING) AS author_id,
    REPLACE(
      REGEXP_REPLACE(CONCAT(COALESCE(name,''), '\n', COALESCE(description,'')), r'\s+', ' '),
      '&nbsp;', ' '
    ) AS text,
    CONCAT('hubspot:pipeline:', COALESCE(pipeline, 'unknown')) AS permission_scope
  FROM `OriginBGEstuary.OriginCRM.hubspotrmsync`
  WHERE COALESCE(name,'') <> '' OR COALESCE(description,'') <> ''
)
SELECT * FROM slack
UNION ALL
SELECT * FROM hubspot;
