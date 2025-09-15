# requirements:
# pip install opensearch-py requests sentence-transformers transformers torch

import requests
from opensearchpy import OpenSearch
from sentence_transformers import SentenceTransformer
import numpy as np

# === CONFIG ===
OPENSEARCH_HOST = "http://localhost:9200"
INDEX = "docs"
EMBED_MODEL_NAME = "all-MiniLM-L6-v2"  # 384-dim
TOP_K = 5

# === clients ===
os_client = OpenSearch(OPENSEARCH_HOST, timeout=30)
embed_model = SentenceTransformer(EMBED_MODEL_NAME)

# === helper: fetch wikipedia summary (live) ===
def fetch_wikipedia_snippets(query, limit=3):
    s_url = "https://en.wikipedia.org/w/api.php"
    params = {
        "action": "query",
        "list": "search",
        "srsearch": query,
        "utf8": 1,
        "format": "json",
        "srlimit": limit
    }
    r = requests.get(s_url, params=params, timeout=10).json()
    results = []
    for item in r.get("query", {}).get("search", []):
        title = item["title"]
        # get page summary
        page = requests.get(
            "https://en.wikipedia.org/api/rest_v1/page/summary/" + title
        ).json()
        results.append({"source": "wikipedia", "title": title, "text": page.get("extract","")})
    return results

# === helper: search OpenSearch (BM25 + optional vector) ===
def opensearch_text_search(q, k=TOP_K):
    body = {
        "size": k,
        "query": {
            "multi_match": {
                "query": q,
                "fields": ["title^2", "text"]
            }
        }
    }
    res = os_client.search(index=INDEX, body=body)
    hits = []
    for hit in res["hits"]["hits"]:
        hits.append({
            "source": "opensearch",
            "id": hit["_id"],
            "score": float(hit["_score"]),
            "title": hit["_source"].get("title"),
            "text": hit["_source"].get("text"),
            "metadata": hit["_source"].get("metadata",{})
        })
    return hits

# === hybrid vector search example (if you have embeddings indexed) ===
def opensearch_vector_search(q, k=TOP_K):
    vec = embed_model.encode(q).tolist()
    body = {
      "size": k,
      "query": {
        "knn": {
          "embedding": {
            "vector": vec,
            "k": k
          }
        }
      }
    }
    res = os_client.search(index=INDEX, body=body)
    hits = []
    for hit in res["hits"]["hits"]:
        hits.append({
            "source":"opensearch_vector",
            "id": hit["_id"],
            "score": hit["_score"],
            "title": hit["_source"].get("title"),
            "text": hit["_source"].get("text")
        })
    return hits

# === fusion: simple union + rescoring by normalized scores (quick RRF-like) ===
def fuse_results(*lists, k=TOP_K):
    # simple approach: assign scores & sort
    combined = []
    for lst in lists:
        for i, item in enumerate(lst):
            # if item already present by id+source, skip
            combined.append(item)
    # naive dedupe by (source,id,text)
    seen = set()
    unique = []
    for it in combined:
        key = (it.get("source"), it.get("id") or it.get("title"), it.get("text")[:120])
        if key in seen: 
            continue
        seen.add(key)
        unique.append(it)
    # sort by available score if present, otherwise prefer wikipedia first
    unique.sort(key=lambda x: x.get("score", 0.0), reverse=True)
    return unique[:k]

# === placeholder LLM call (replace with whichever LLM you use) ===
def llm_synthesize_answer(question, passages):
    prompt = "Answer the question using ONLY the following passages. Include citations to the source.\n\n"
    for i,p in enumerate(passages):
        src = p.get("source")+" / "+(p.get("title") or p.get("id") or "doc")
        prompt += f"PASSAGE {i+1} (source: {src}):\n{p['text']}\n\n"
    prompt += f"QUESTION: {question}\n\nAnswer (with short citations):"
    # TODO: call your LLM API here and return the LLM response
    return prompt  # for now returning prompt to visualize

# === pipeline ===
def qa_pipeline(query):
    wiki = fetch_wikipedia_snippets(query, limit=3)
    os_text = opensearch_text_search(query, k=5)
    os_vec = []
    # if you have vector index enabled, use vector search too (uncomment)
    # os_vec = opensearch_vector_search(query, k=5)
    combined = fuse_results(wiki, os_text, os_vec, k=6)
    # synthesize with LLM
    answer = llm_synthesize_answer(query, combined)
    return {"answer": answer, "sources": combined}

if __name__ == "__main__":
    q = "What is transfer learning in machine learning?"
    out = qa_pipeline(q)
    print(out["answer"])
    print("SOURCES:")
    for s in out["sources"]:
        print("-", s.get("source"), s.get("title") or s.get("id"))
