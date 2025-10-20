# Company RAG (BigQuery → OpenAI → Pinecone)

Hybrid RAG for internal knowledge using Slack + HubSpot data in BigQuery.

## Architecture
BigQuery (source of truth) → Chunk table → OpenAI embeddings → Pinecone ANN → FastAPI `/ask`.

## One-time setup
1. Run SQL:
   - `sql/01_unified_corpus_v1.sql`
   - `sql/02_unified_chunks_v1.sql`
2. Create Pinecone index:
   ```bash
   export PINECONE_API_KEY=...
   python create_index.py
