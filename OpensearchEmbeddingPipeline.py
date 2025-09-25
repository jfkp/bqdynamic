import os
import requests
import zipfile
import json
import base64
import torch
from sentence_transformers import SentenceTransformer

# -------------------------
# CONFIG
# -------------------------
OPENSEARCH_URL = "http://localhost:9200"   # adjust for your cluster
JFROG_BASE_URL = "https://registry.saas.cagip.group.gca/artifactory"
JFROG_REPO_KEY = "huggingfaceml-local"     # your repo key
JFROG_ARTIFACT = "sentence-transformers_all-MiniLM-L6-v2.zip"
JFROG_TOKEN = os.getenv("JFROG_TOKEN", "your-token-here")

EXTRACT_DIR = "models"
MODEL_NAME = "all-MiniLM-L6-v2"
TORCHSCRIPT_MODEL = os.path.join(EXTRACT_DIR, f"{MODEL_NAME}.pt")

# -------------------------
# STEP 1: Download + extract HF model from JFrog
# -------------------------
def load_model_from_jfrog(repo_key, artifact_name, token, extract_to="models"):
    url = f"{JFROG_BASE_URL}/{repo_key}/{artifact_name}"
    local_zip = os.path.join(extract_to, artifact_name)
    local_dir = os.path.join(extract_to, artifact_name.replace(".zip", ""))

    os.makedirs(extract_to, exist_ok=True)

    print(f"⬇️ Downloading {artifact_name} from {url}...")
    with requests.get(url, headers={"Authorization": f"Bearer {token}"}, stream=True, verify=False) as r:
        r.raise_for_status()
        with open(local_zip, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)

    print(f"📦 Extracting {artifact_name}...")
    with zipfile.ZipFile(local_zip, "r") as zip_ref:
        zip_ref.extractall(local_dir)

    print(f"✅ Extracted to {local_dir}")
    return local_dir

# -------------------------
# STEP 2: Convert HF model to TorchScript
# -------------------------
def convert_hf_to_torchscript(model_dir, output_path):
    model = SentenceTransformer(model_dir)

    class EncoderWrapper(torch.nn.Module):
        def __init__(self, model):
            super().__init__()
            self.model = model

        def forward(self, sentences: list[str]):
            return self.model.encode(sentences, convert_to_tensor=True)

    wrapper = EncoderWrapper(model)

    example_input = ["Hello OpenSearch!"]
    traced = torch.jit.trace(wrapper, (example_input,))
    torch.jit.save(traced, output_path)

    print(f"✅ TorchScript model saved to {output_path}")
    return output_path

# -------------------------
# STEP 3: Register model in OpenSearch
# -------------------------
def register_model(torch_model_path):
    with open(torch_model_path, "rb") as f:
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
# STEP 4: Deploy model
# -------------------------
def deploy_model(model_id):
    resp = requests.post(f"{OPENSEARCH_URL}/_plugins/_ml/models/{model_id}/_deploy")
    resp.raise_for_status()
    print(f"✅ Model {model_id} deployed")

# -------------------------
# STEP 5: Get embedding
# -------------------------
def get_embedding(model_id, text):
    payload = {"parameters": {"inputs": text}}
    resp = requests.post(f"{OPENSEARCH_URL}/_plugins/_ml/models/{model_id}/_predict",
                         headers={"Content-Type": "application/json"},
                         data=json.dumps(payload))
    resp.raise_for_status()
    embedding = resp.json()["inference_results"][0]["output"]
    print(f"✅ Got embedding for '{text}' (len={len(embedding)})")
    return embedding

# -------------------------
# STEP 6: Create index
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
# STEP 7: Index documents
# -------------------------
def index_doc(index_name, text, embedding):
    doc = {"text": text, "vector": embedding}
    resp = requests.post(f"{OPENSEARCH_URL}/{index_name}/_doc",
                         headers={"Content-Type": "application/json"},
                         data=json.dumps(doc))
    resp.raise_for_status()
    print(f"✅ Document indexed: {text}")

# -------------------------
# STEP 8: Search
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
    # 1. Download + extract HF model from JFrog
    model_dir = load_model_from_jfrog(JFROG_REPO_KEY, JFROG_ARTIFACT, JFROG_TOKEN, extract_to=EXTRACT_DIR)

    # 2. Convert to TorchScript
    torch_model_path = convert_hf_to_torchscript(model_dir, TORCHSCRIPT_MODEL)

    # 3. Register model in OpenSearch
    model_id = register_model(torch_model_path)

    # 4. Deploy model
    deploy_model(model_id)

    # 5. Get an embedding
    emb = get_embedding(model_id, "OpenSearch loves vectors")

    # 6. Create index
    create_index("my-embeddings", dim=len(emb))

    # 7. Index some docs
    index_doc("my-embeddings", "OpenSearch loves vectors", emb)
    index_doc("my-embeddings", "Semantic search with MiniLM",
              get_embedding(model_id, "Semantic search with MiniLM"))

    # 8. Run a query
    query_emb = get_embedding(model_id, "vector search")
    search("my-embeddings", query_emb, k=2)
