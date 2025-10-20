# app.py
import os
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Any
from openai import OpenAI
from pinecone import Pinecone

OPENAI_API_KEY     = os.environ["OPENAI_API_KEY"]
PINECONE_API_KEY   = os.environ["PINECONE_API_KEY"]
PINECONE_INDEX     = os.environ.get("PINECONE_INDEX", "company-knowledge")
CHAT_MODEL         = os.environ.get("CHAT_MODEL", "gpt-4.1-mini")
EMB_MODEL          = os.environ.get("EMB_MODEL", "text-embedding-3-large")

client_oai = OpenAI(api_key=OPENAI_API_KEY)
pc = Pinecone(api_key=PINECONE_API_KEY)
index = pc.Index(PINECONE_INDEX)

app = FastAPI()

class AskReq(BaseModel):
    query: str
    user_groups: List[str] = []   # e.g., ["slack:public:#general", "hubspot:pipeline:sales"]
    top_k: int = 8

SYSTEM = """You are the internal assistant. Be concise and cite sources as [doc_id].
Respect permissions: only use docs where permission intersects the user's groups.
If nothing relevant is found, say so."""

def embed_query(q: str):
    return client_oai.embeddings.create(model=EMB_MODEL, input=q).data[0].embedding

def pc_filter(groups: List[str]) -> Dict[str, Any]:
    return {"permission": {"$in": groups}} if groups else {}

@app.post("/ask")
def ask(req: AskReq):
    try:
        qvec = embed_query(req.query)
        res = index.query(
            vector=qvec, top_k=req.top_k, include_metadata=True,
            filter=pc_filter(req.user_groups)
        )
        matches = res.matches or []
        if not matches:
            return {"answer":"I couldn't find anything relevant.", "citations":[]}

        # Build lightweight context from metadata preview (or hydrate from BQ)
        contexts, citations = [], []
        for m in matches:
            md = m.metadata or {}
            doc_id = md.get("doc_id", m.id)
            preview = md.get("preview", "")  # optional; see indexer
            contexts.append(f"[{doc_id}] {preview}")
            citations.append(doc_id)

        messages = [
            {"role":"system","content": SYSTEM},
            {"role":"user","content": f"Question: {req.query}\n\nSources:\n" + "\n\n".join(contexts)}
        ]
        out = client_oai.chat.completions.create(model=CHAT_MODEL, messages=messages, temperature=0)
        return {"answer": out.choices[0].message.content, "citations": list(dict.fromkeys(citations))}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
