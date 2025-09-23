from huggingface_hub import snapshot_download

from opensearchpy import OpenSearch

# Connection config
host = "your-opensearch-host"
port = 9200
auth = ("username", "password")  # basic auth

# If HTTPS with self-signed certs, set verify_certs=False
client = OpenSearch(
    hosts=[{"host": host, "port": port}],
    http_auth=auth,
    use_ssl=True,              # if your cluster runs on https
    verify_certs=False,        # True if you trust CA, False to skip SSL check
    ssl_show_warn=False
)

# Test the connection
print(client.info())
indices = client.indices.get_alias("*")
print("Indices:")
for idx in indices:
    print(idx)

import requests
from huggingface_hub import HfApi

# Build a session that disables SSL verification
session = requests.Session()
session.verify = False   # <-- disables SSL check
session.proxies.update({
    "http":  "http://user:pass@proxy:8080",
    "https": "http://user:pass@proxy:8080"
})

api = HfApi(session=session)

# Example: list models (ignores SSL cert issues)
print(api.list_models(search="all-MiniLM-L6-v2"))

from huggingface_hub import snapshot_download
import huggingface_hub.file_download as fd

# Force requests to ignore SSL certs
fd._session = requests.Session()
fd._session.verify = False
fd._session.proxies.update({
    "http":  "http://user:pass@proxy:8080",
    "https": "http://user:pass@proxy:8080"
})

local_dir = snapshot_download(
    repo_id="sentence-transformers/all-MiniLM-L6-v2",
    local_dir="models/all-MiniLM-L6-v2",
    local_dir_use_symlinks=False
)

print("Model downloaded to:", local_dir)


# Downloads the entire repo to a local directory
local_dir = snapshot_download(
    repo_id="sentence-transformers/all-MiniLM-L6-v2",  # model repo
    local_dir="models/all-MiniLM-L6-v2",              # where to store locally
    local_dir_use_symlinks=False                      # optional: copy files instead of symlinks
)

print("Model downloaded to:", local_dir)

pip install huggingface_hub
from sentence_transformers import SentenceTransformer

model = SentenceTransformer("models/all-MiniLM-L6-v2")
print(model.encode("Hello world"))


import os

os.environ["HTTP_PROXY"] = "http://user:pass@proxy:port"
os.environ["HTTPS_PROXY"] = "http://user:pass@proxy:port"


os.environ["HF_HUB_DISABLE_SSL_VERIFY"] = "1"


models/all-MiniLM-L6-v2/
    ├── config.json
    ├── pytorch_model.bin
    ├── tokenizer.json
    ├── tokenizer_config.json
    ├── vocab.txt
    └── 1_Pooling/
from sentence_transformers import SentenceTransformer

model = SentenceTransformer("models/all-MiniLM-L6-v2")

embedding = model.encode("Hello from offline mode")
print(embedding[:5])

         
