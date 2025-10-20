-- sql/02_unified_chunks_v1.sql
CREATE OR REPLACE TABLE `OriginBGEstuary.analytics.unified_chunks_v1` AS
WITH base AS (
  SELECT doc_id, source, created_at, author_id, permission_scope, text
  FROM `OriginBGEstuary.analytics.unified_corpus_v1`
),
split AS (
  SELECT
    doc_id, source, created_at, author_id, permission_scope,
    SPLIT(text, '\n\n') AS paras
  FROM base
),
flat AS (
  SELECT
    doc_id, source, created_at, author_id, permission_scope,
    para, OFFSET AS idx
  FROM split, UNNEST(paras) AS para WITH OFFSET
),
grouped AS (
  SELECT
    doc_id, source, created_at, author_id, permission_scope,
    CEIL((idx+1)/6.0) AS grp_id,
    STRING_AGG(para, '\n\n' ORDER BY idx) AS chunk_text
  FROM flat
  GROUP BY doc_id, source, created_at, author_id, permission_scope, CEIL((idx+1)/6.0)
)
SELECT
  CONCAT(doc_id, '#', CAST(grp_id AS STRING)) AS chunk_id,
  doc_id, source, created_at, author_id, permission_scope, chunk_text
FROM grouped;
