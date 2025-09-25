import torch
import base64
import requests
import json
from sentence_transformers import SentenceTransformer

# -------------------------
# CONFIG
# -------------------------
OPENSEARCH_URL = "http://localhost:9200"   # Change if needed
MODEL_NAME = "all-MiniLM-L6-v2"
MODEL_PATH = f"{MODEL_NAME}.pt"  # TorchScript export path

# -------------------------
# STEP 1: Convert HuggingFace model → TorchScript
# -------------------------
def convert_model_to_torchscript():
    model = SentenceTransformer(MODEL_NAME)

    class EncoderWrapper(torch.nn.Module):
        def __init__(self, model):
            super().__init__()
            self.model = model

        def forward(self, sentences):
            return self.model.encode(sentences, convert_to_tensor=True)

    wrapper = EncoderWrapper(model)

    example = ["Hello OpenSearch!"]
    traced = torch.jit.trace(wrapper, (example,))
    torch.jit.save(traced, MODEL_PATH)
    print(f"✅ Model converted and saved as {MODEL_PATH}")

# -------------------------
# STEP 2: Register model in OpenSearch
# -------------------------
def register_model():
    with open(MODEL_PATH, "rb") as f:
        model_bytes = f.read()
    model_b64 = base64.b64encode(model_bytes).decode("utf-8")

    payload = {
        "name": MODEL_NAME,
        "version": "1.0.0",
        "model_format": "TORCH_SCRIPT",
        "model_task_type": "TEXT_EMBEDDING",
        "model_content": model_b64,
    }

    resp = requests.post(f"{OPENSEARCH_URL}/_plugins/_ml/models/_register",
                         headers={"Content-Type": "application/json"},
                         data=json.dumps(payload))
    resp.raise_for_status()
    model_id = resp.json()["model_id"]
    print(f"✅ Model registered with ID: {model_id}")
    return model_id

# -------------------------
# STEP 3: Deploy model
# -------------------------
def deploy_model(model_id):
    resp = requests.post(f"{OPENSEARCH_URL}/_plugins/_ml/models/{model_id}/_deploy")
    resp.raise_for_status()
    print(f"✅ Model {model_id} deployed")

# -------------------------
# STEP 4: Predict embeddings
# -------------------------
def get_embedding(model_id, text):
    payload = {"parameters": {"inputs": text}}
    resp = requests.post(f"{OPENSEARCH_URL}/_plugins/_ml/models/{model_id}/_predict",
                         headers={"Content-Type": "application/json"},
                         data=json.dumps(payload))
    resp.raise_for_status()
    embedding = resp.json()["inference_results"][0]["output"]
    print(f"✅ Got embedding of length {len(embedding)}")
    return embedding

# -------------------------
# STEP 5: Create index with vector field
# -------------------------
def create_index(index_name, dim=384):
    mapping = {
        "mappings": {
            "properties": {
                "text": {"type": "text"},
                "vector": {"type": "knn_vector", "dimension": dim}
            }
        }
    }
    resp = requests.put(f"{OPENSEARCH_URL}/{index_name}",
                        headers={"Content-Type": "application/json"},
                        data=json.dumps(mapping))
    if resp.status_code not in (200, 201):
        print(f"⚠️ Index creation response: {resp.status_code} {resp.text}")
    else:
        print(f"✅ Index {index_name} created")

# -------------------------
# STEP 6: Index doc with embedding
# -------------------------
def index_doc(index_name, text, embedding):
    doc = {"text": text, "vector": embedding}
    resp = requests.post(f"{OPENSEARCH_URL}/{index_name}/_doc",
                         headers={"Content-Type": "application/json"},
                         data=json.dumps(doc))
    resp.raise_for_status()
    print(f"✅ Document indexed: {text}")

# -------------------------
# STEP 7: kNN search
# -------------------------
def search(index_name, embedding, k=2):
    query = {
        "size": k,
        "query": {
            "knn": {"vector": {"vector": embedding, "k": k}}
        }
    }
    resp = requests.post(f"{OPENSEARCH_URL}/{index_name}/_search",
                         headers={"Content-Type": "application/json"},
                         data=json.dumps(query))
    resp.raise_for_status()
    hits = resp.json()["hits"]["hits"]
    print("🔎 Search results:")
    for h in hits:
        print(f"  - {h['_source']['text']} (score={h['_score']})")

# -------------------------
# MAIN PIPELINE
# -------------------------
if __name__ == "__main__":
    convert_model_to_torchscript()
    model_id = register_model()
    deploy_model(model_id)

    # Test embedding
    emb = get_embedding(model_id, "OpenSearch loves vectors")

    # Create index + add docs
    create_index("my-embeddings", dim=len(emb))
    index_doc("my-embeddings", "OpenSearch loves vectors", emb)
    index_doc("my-embeddings", "Semantic search with MiniLM", get_embedding(model_id, "Semantic search with MiniLM"))

    # Run query
    query_emb = get_embedding(model_id, "vector search")
    search("my-embeddings", query_emb, k=2)
