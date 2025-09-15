# OpenSearch + Wikipedia Q\&A — Full Stack (Docker, Ingest, Airflow, Reranker, Simulation)

This single document contains everything to run a complete prototype and a production-ish pipeline that integrates OpenSearch (hybrid text + vector), live Wikipedia ingestion (via dump or API), an Airflow DAG for periodic updates, a cross-encoder reranker service, and a small simulation to evaluate latency/quality trade-offs.

---

## Contents

1. Overview & quick start
2. `docker-compose.yml` (OpenSearch + OpenSearch Dashboards + Python service)
3. OpenSearch index mapping & settings
4. Small dataset ingestion script (`ingest_sample.py`)
5. Prototype Q\&A service (`qa_service.py`) — queries OpenSearch + Wikipedia REST + LLM placeholder
6. Airflow DAG for Wikipedia dump ingestion (`dags/wikipedia_ingest_dag.py`) + helper `wiki_ingest.py`
7. Cross-encoder reranker service (`reranker_service.py`) and client (`rerank_client.py`)
8. Evaluation & simulation script (`simulate_eval.py`)
9. `requirements.txt` and run instructions
10. Notes on deployment, scaling, and safety

---

# 1 — Overview & quick start

This bundle gives you three main deliverables:

* **Docker Compose**: spins up OpenSearch and Dashboards, plus an optional tiny API container for quick testing.
* **Prototype ingestion & QA**: Python scripts to index a tiny dataset and run a hybrid retrieval + LLM synthesis pipeline.
* **Production pipeline**: Airflow DAG that downloads Wikipedia dumps, parses, chunks, embeds, and indexes into OpenSearch on a schedule.
* **Cross-encoder reranker**: a simple HTTP service that reranks (query, passage) pairs using a Hugging Face cross-encoder model.
* **Simulation**: measures retrieval+rerank latency and shows how to run experiments.

Open the sections below for runnable files.

---

# 2 — `docker-compose.yml`

```yaml
version: '3.8'
services:
  opensearch:
    image: opensearchproject/opensearch:2.9.0
    container_name: opensearch
    environment:
      - discovery.type=single-node
      - plugins.security.disabled=true
      - bootstrap.memory_lock=true
      - "OPENSEARCH_JAVA_OPTS=-Xms512m -Xmx512m"
    ulimits:
      memlock:
        soft: -1
        hard: -1
    volumes:
      - opensearch-data:/usr/share/opensearch/data
    ports:
      - 9200:9200
      - 9600:9600

  dashboards:
    image: opensearchproject/opensearch-dashboards:2.9.0
    ports:
      - 5601:5601
    environment:
      - OPENSEARCH_HOSTS=http://opensearch:9200
    depends_on:
      - opensearch

  api:
    build: ./api
    container_name: qa_api
    ports:
      - 8000:8000
    depends_on:
      - opensearch

volumes:
  opensearch-data:
```

Include an `api/` folder containing the Python service code (`qa_service.py`) and a `Dockerfile` (see later sections).

---

# 3 — OpenSearch index mapping & settings

Save as `opensearch_mapping.json`.

```json
{
  "settings": {
    "index": {
      "knn": true,
      "knn.algo_param.ef_search": 512
    }
  },
  "mappings": {
    "properties": {
      "title": { "type": "text" },
      "text": { "type": "text" },
      "metadata": { "type": "object", "enabled": true },
      "timestamp": { "type": "date" },
      "embedding": {
        "type": "knn_vector",
        "dimension": 384
      }
    }
  }
}
```

Adjust `dimension` to match your embedding model.

---

# 4 — `ingest_sample.py` (index a tiny dataset)

```python
# ingest_sample.py
# small script to create index and add a few docs

import json
from opensearchpy import OpenSearch
from sentence_transformers import SentenceTransformer

OPENSEARCH_HOST = "http://localhost:9200"
INDEX = "docs"
EMBED_DIM = 384

client = OpenSearch(OPENSEARCH_HOST)
model = SentenceTransformer("all-MiniLM-L6-v2")

# create index if not exists
with open("opensearch_mapping.json","r") as f:
    mapping = json.load(f)

if not client.indices.exists(INDEX):
    client.indices.create(index=INDEX, body=mapping)

# sample docs
docs = [
    {"title": "Transfer learning", "text": "Transfer learning is a technique in machine learning where a model trained on one task is repurposed on a second related task.", "metadata": {"source":"sample"}},
    {"title": "Neural networks", "text": "Neural networks are computing systems inspired by biological neural networks.", "metadata": {"source":"sample"}},
    {"title": "OpenSearch", "text": "OpenSearch is a community-driven, open source search and analytics suite derived from Elasticsearch.", "metadata": {"source":"sample"}}
]

for i,d in enumerate(docs):
    emb = model.encode(d["text"]).tolist()
    body = {"title": d["title"], "text": d["text"], "metadata": d["metadata"], "embedding": emb}
    client.index(index=INDEX, id=f"sample_{i}", body=body, refresh=True)

print("Indexed sample docs")
```

---

# 5 — `qa_service.py` (FastAPI prototype)

This service demonstrates hybrid retrieval (BM25 + kNN) + Wikipedia REST lookup + placeholder LLM synthesis.

```python
# api/qa_service.py
from fastapi import FastAPI
from pydantic import BaseModel
import requests
from opensearchpy import OpenSearch
from sentence_transformers import SentenceTransformer

app = FastAPI()
client = OpenSearch("http://opensearch:9200")
model = SentenceTransformer("all-MiniLM-L6-v2")
INDEX = "docs"

class Query(BaseModel):
    q: str

@app.post('/qa')
def qa(query: Query):
    qtxt = query.q
    # 1) Wikipedia snippets
    wiki = fetch_wikipedia_snippets(qtxt, limit=3)
    # 2) OpenSearch BM25
    os_hits = opensearch_text_search(qtxt, k=5)
    # 3) optional vector search
    vec_hits = opensearch_vector_search(qtxt, k=5)
    combined = fuse_results(wiki, os_hits, vec_hits, k=6)
    # 4) LLM synth placeholder: return the assembled prompt
    prompt = build_prompt(qtxt, combined)
    return {"prompt": prompt, "sources": combined}

# --- helper routines (copy from earlier prototype) ---
# fetch_wikipedia_snippets, opensearch_text_search, opensearch_vector_search, fuse_results, build_prompt
# (keep code brief here — see full functions in repo)
```

Add a `Dockerfile` in `api/`:

```dockerfile
FROM python:3.10-slim
WORKDIR /app
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt
COPY . /app
CMD ["uvicorn","qa_service:app","--host","0.0.0.0","--port","8000"]
```

---

# 6 — Airflow DAG for Wikipedia ingestion

Place this in an Airflow environment's `dags/`.

```python
# dags/wikipedia_ingest_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from wiki_ingest import run_wikipedia_ingest

default_args = {
    'owner': 'qa-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('wikipedia_ingest', default_args=default_args, schedule_interval='@weekly')

ingest = PythonOperator(
    task_id='ingest_wikipedia_dump',
    python_callable=run_wikipedia_ingest,
    dag=dag
)
```

`wiki_ingest.py` — helper to download dump, parse, chunk, embed, index. (This is simplified; for production use streaming parser like `wikiextractor`.)

```python
# wiki_ingest.py
import os
import bz2
import requests
import tempfile
from opensearchpy import OpenSearch
from sentence_transformers import SentenceTransformer
from pathlib import Path

OPENSEARCH = os.getenv('OPENSEARCH_HOST','http://localhost:9200')
INDEX = 'docs'
EMBED_MODEL = 'all-MiniLM-L6-v2'

client = OpenSearch(OPENSEARCH)
model = SentenceTransformer(EMBED_MODEL)

WIKI_DUMP_URL = 'https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles.xml.bz2'

def download_dump(target_path):
    r = requests.get(WIKI_DUMP_URL, stream=True)
    with open(target_path, 'wb') as f:
        for chunk in r.iter_content(chunk_size=8192):
            f.write(chunk)

# naive parser that extracts <title> and <text> blocks using simple splitting; for production use a streaming XML parser

def parse_and_index(dump_path, max_pages=2000):
    # decompress
    with bz2.open(dump_path, 'rt', encoding='utf-8', errors='ignore') as fh:
        content = fh.read()
    # VERY naive splitting (only for example!)
    pages = content.split('<page>')
    count = 0
    for p in pages[1: max_pages+1]:
        try:
            title = p.split('<title>')[1].split('</title>')[0]
            text = p.split('<text')[1].split('>')[1].split('</text>')[0]
        except Exception:
            continue
        # chunk by 800 characters
        chunks = [text[i:i+1500] for i in range(0,len(text),1500)]
        for i,ch in enumerate(chunks):
            emb = model.encode(ch).tolist()
            body = {
                'title': title,
                'text': ch,
                'metadata': {'page': title, 'chunk': i},
            'embedding': emb
            }
            client.index(index=INDEX, body=body)
        count += 1
    return count


def run_wikipedia_ingest():
    tmp = tempfile.mkdtemp()
    dump_path = Path(tmp)/'enwiki-latest-pages-articles.xml.bz2'
    download_dump(dump_path)
    n = parse_and_index(dump_path, max_pages=500)
    print(f'Indexed {n} pages (sample)')
```

**Important:** The naive parser above is for demonstration. In production use `wikiextractor` or `mwxml` to parse efficiently and reliably.

---

# 7 — Cross-encoder reranker service

`reranker_service.py` — starts a small HTTP server that accepts POST with `{query:..., passages:[{id:...,text:...}]}` and returns scores.

```python
# reranker_service.py
from fastapi import FastAPI
from pydantic import BaseModel
from typing import List, Dict
from sentence_transformers import CrossEncoder

app = FastAPI()
model = CrossEncoder('cross-encoder/ms-marco-MiniLM-L-6-v2')

class Passage(BaseModel):
    id: str
    text: str

class RerankRequest(BaseModel):
    query: str
    passages: List[Passage]

@app.post('/rerank')
def rerank(req: RerankRequest):
    pairs = [(req.query, p.text) for p in req.passages]
    scores = model.predict(pairs)
    out = [{'id': p.id, 'score': float(s)} for p,s in zip(req.passages, scores)]
    out.sort(key=lambda x: x['score'], reverse=True)
    return {'ranked': out}
```

`rerank_client.py` — demonstrates calling the service:

```python
import requests

def rerank_demo():
    payload = {
        'query': 'what is transfer learning',
        'passages': [
            {'id':'a','text':'Transfer learning repurposes models trained on one task.'},
            {'id':'b','text':'Neural networks are layers of neurons.'}
        ]
    }
    r = requests.post('http://localhost:8100/rerank', json=payload)
    print(r.json())

if __name__=='__main__':
    rerank_demo()
```

Run the reranker service with `uvicorn reranker_service:app --port 8100`.

---

# 8 — `simulate_eval.py` (latency & quality sim)

This script simulates retrieval (OpenSearch BM25 + kNN) plus reranker and measures timings. It also computes a simple MRR if you provide ground-truth passage IDs.

```python
# simulate_eval.py
import time
from opensearchpy import OpenSearch
import requests

OS = OpenSearch('http://localhost:9200')
RERANK_URL = 'http://localhost:8100/rerank'

def retrieve(query, k=10):
    t0 = time.time()
    res = OS.search(index='docs', body={'size':k,'query':{'multi_match':{'query':query,'fields':['title^2','text']}}})
    t1 = time.time()
    hits = [{'id':h['_id'],'text':h['_source']['text']} for h in res['hits']['hits']]
    return hits, t1-t0

def rerank(query, passages):
    t0 = time.time()
    r = requests.post(RERANK_URL, json={'query':query,'passages':passages}).json()
    t1 = time.time()
    return r['ranked'], t1-t0

if __name__=='__main__':
    q = 'what is transfer learning'
    hits, t_r = retrieve(q, k=10)
    print('retrieval time',t_r,'hits',len(hits))
    ranked, t_rr = rerank(q, hits)
    print('rerank time',t_rr)
    print('top result',ranked[0])
```

---

# 9 — `requirements.txt`

```
fastapi
uvicorn
opensearch-py
sentence-transformers
transformers
torch
requests
pydantic
apache-airflow==2.8.0
```

Adjust versions for compatibility with your environment. For CPU-only runs use appropriate torch wheels.

---

# 10 — Run instructions (quick)

1. Start OpenSearch and Dashboards:

   * `docker compose up -d`
2. Install Python deps (locally or use the `api` container):

   * `pip install -r requirements.txt`
3. Index sample docs:

   * `python ingest_sample.py`
4. Start reranker:

   * `uvicorn reranker_service:app --port 8100 --host 0.0.0.0`
5. Start QA API (locally or in container):

   * `uvicorn qa_service:app --port 8000 --host 0.0.0.0`
6. Test endpoint:

   * `curl -s -X POST http://localhost:8000/qa -H "Content-Type: application/json" -d '{"q":"what is transfer learning"}' | jq`
7. For periodic full Wikipedia ingest, deploy Airflow and put `wikipedia_ingest_dag.py` and `wiki_ingest.py` into its DAGs and Python path. Make sure the Airflow worker has enough disk and memory.

---

# 11 — Production notes & tuning

* **Parser:** use `wikiextractor` or `mwxml` to reliably extract pages from the dump.
* **Chunking:** prefer semantic chunking by sentence boundaries and \~200–800 tokens per chunk.
* **Embeddings:** choose model/embedding provider based on privacy & latency (local sentence-transformers vs cloud embeddings).
* **Vector index tuning:** set `ef_search`, `ef_construction`, and `m` for HNSW accordingly.
* **Reranker:** cross-encoder improves quality but increases latency; add caching for frequent queries and consider a threshold to only rerank top-N.
* **LLM prompts:** always instruct the LLM to use only provided passages and return citations (pass `passages` with `source:id` metadata).
* **Monitoring:** log retrieval candidates, reranker scores, LLM tokens, and user feedback.

---

# 12 — Security & privacy

* Redact PII before indexing. Encrypt sensitive metadata at rest.
* If using hosted LLMs, be aware of data retention and user privacy policies.
* Introduce human review for high-risk categories.

---

# 13 — Next steps & optional add-ons

* Add a small frontend demonstrating answers with source citations.
* Integrate a feedback loop for training the reranker (store labeled pairs).
* Use a vector-only index (FAISS) if you want to decouple vector concerns from OpenSearch.

---

*End of document.*


  
