# create_index.py
import os
from pinecone import Pinecone, ServerlessSpec

pc = Pinecone(api_key=os.environ["PINECONE_API_KEY"])
name = os.environ.get("PINECONE_INDEX","company-knowledge")
if name not in [i.name for i in pc.list_indexes()]:
    pc.create_index(
        name, dimension=3072, metric="cosine",
        spec=ServerlessSpec(cloud="aws", region="us-east-1")
    )
print("Index OK")
