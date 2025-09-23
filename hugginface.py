from huggingface_hub import snapshot_download

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

         
