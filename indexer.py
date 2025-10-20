# indexer.py
import os
from typing import List
from tenacity import retry, stop_after_attempt, wait_exponential
from google.cloud import bigquery
from openai import OpenAI
from pinecone import Pinecone

GCP_PROJECT   = os.environ["GCP_PROJECT"]           # OriginBGEstuary
BQ_DATASET    = os.environ.get("BQ_DATASET","analytics")
CHUNK_TABLE   = f"{GCP_PROJECT}.{BQ_DATASET}.unified_chunks_v1"
STATE_TABLE   = f"{GCP_PROJECT}.{BQ_DATASET}.indexer_state"
PINECONE_INDEX= os.environ.get("PINECONE_INDEX","company-knowledge")
OPENAI_MODEL  = os.environ.get("EMB_MODEL","text-embedding-3-large")
BATCH_SIZE    = int(os.environ.get("BATCH_SIZE","100"))

bq = bigquery.Client(project=GCP_PROJECT)
pc = Pinecone(api_key=os.environ["PINECONE_API_KEY"])
index = pc.Index(PINECONE_INDEX)
oai = OpenAI(api_key=os.environ["OPENAI_API_KEY"])

def ensure_state():
    bq.query(f"""
      CREATE TABLE IF NOT EXISTS `{STATE_TABLE}` (
        id STRING,
        last_watermark TIMESTAMP
      )
    """).result()
    rows = list(bq.query(f"SELECT 1 FROM `{STATE_TABLE}` WHERE id='chunks'").result())
    if not rows:
        bq.query(f"INSERT `{STATE_TABLE}` (id, last_watermark) VALUES ('chunks', TIMESTAMP('1970-01-01'))").result()

def get_wm():
    return list(bq.query(f"SELECT last_watermark FROM `{STATE_TABLE}` WHERE id='chunks'").result())[0][0]

def set_wm(ts):
    bq.query(f"UPDATE `{STATE_TABLE}` SET last_watermark=TIMESTAMP('{ts}') WHERE id='chunks'").result()

def fetch(since, limit=5000):
    q = f"""
    SELECT chunk_id, doc_id, source, created_at, author_id, permission_scope, chunk_text, CURRENT_TIMESTAMP() AS updated_at
    FROM `{CHUNK_TABLE}`
    WHERE created_at > @since
    ORDER BY created_at ASC
    LIMIT {limit}
    """
    return list(bq.query(q, job_config=bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("since","TIMESTAMP", since)]
    )).result())

@retry(stop=stop_after_attempt(5), wait=wait_exponential(min=1,max=30))
def embed(texts: List[str]):
    resp = oai.embeddings.create(model=OPENAI_MODEL, input=texts)
    return [d.embedding for d in resp.data]

def run():
    ensure_state()
    wm = get_wm()
    rows = fetch(wm)
    if not rows: 
        print("No new rows."); return
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i:i+BATCH_SIZE]
        vecs = embed([r["chunk_text"] for r in batch])
        payload = []
        for r, v in zip(batch, vecs):
            preview = (r["chunk_text"][:500] + "…") if r["chunk_text"] and len(r["chunk_text"])>500 else r["chunk_text"]
            payload.append({
                "id": r["chunk_id"],
                "values": v,
                "metadata": {
                    "doc_id": r["doc_id"],
                    "source": r["source"],
                    "created_at": str(r["created_at"]),
                    "permission": r["permission_scope"],
                    "preview": preview  # used by app.py
                }
            })
            last_ts = r["created_at"]
        index.upsert(vectors=payload)
        set_wm(last_ts)
        print(f"Upserted {len(payload)}; wm={last_ts}")

if __name__ == "__main__":
    run()
