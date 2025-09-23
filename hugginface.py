from huggingface_hub import snapshot_download

from opensearchpy import OpenSearch
https://opensearch-gaas-dashboard.se-int-caas.ca.cib/api/console/proxy?path=_search&method=GET
https://opensearch-gaas-dashboard.se-int-caas.ca.cib/app/home#/
https://opensearch-gaas-dashboard.se-int-caas.ca.cib/
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

import requests
from requests.auth import HTTPBasicAuth

BASE_URL = "https://opensearch-gaas-dashboard.se-int-caas.ca.cib/api/console/proxy"

auth = HTTPBasicAuth("username", "password")

import requests
from requests.auth import HTTPBasicAuth



import json
import requests
from requests.auth import HTTPBasicAuth

import json
import requests
from requests.auth import HTTPBasicAuth

class OpenSearchProxyClient:
    def __init__(self, base_url, username=None, password=None, verify_ssl=False):
        self.base_url = base_url.rstrip("/") + "/api/console/proxy"
        self.auth = HTTPBasicAuth(username, password) if username and password else None
        self.verify_ssl = verify_ssl

    def _call(self, path, method="GET", body=None, params=None, ndjson=False):
        url = f"{self.base_url}?path={path}&method={method}"
        headers = {"Content-Type": "application/json"}
        data = None

        if ndjson and body:  # for bulk requests
            headers["Content-Type"] = "application/x-ndjson"
            data = "\n".join(body) + "\n"
        elif body:
            data = body

        resp = requests.request(
            method=method,
            url=url,
            auth=self.auth,
            verify=self.verify_ssl,
            headers=headers,
            json=None if ndjson else body,
            data=data if ndjson else None,
            params=params
        )
        resp.raise_for_status()
        return resp.json()

    # --- API wrappers ---
    def info(self):
        return self._call("/")

    def search(self, index=None, body=None, size=10):
        path = f"{index}/_search" if index else "_search"
        return self._call(path, method="POST", body=body or {"query": {"match_all": {}}}, params={"size": size})

    def list_indices(self):
        return self._call("_cat/indices?format=json")

    def get_index(self, index):
        return self._call(index, method="GET")

    def count(self, index=None, body=None):
        path = f"{index}/_count" if index else "_count"
        return self._call(path, method="POST", body=body or {"query": {"match_all": {}}})

    def index(self, index, id=None, body=None):
        path = f"{index}/_doc"
        if id:
            path += f"/{id}"
        return self._call(path, method="POST", body=body)

    def bulk(self, actions, index=None):
        ndjson_lines = []
        for action in actions:
            if "index" in action:
                meta = {"index": {"_index": index or action["index"].get("_index"), "_id": action["index"].get("_id")}}
                ndjson_lines.append(json.dumps(meta))
                ndjson_lines.append(json.dumps(action["index"]["doc"]))
            elif "delete" in action:
                meta = {"delete": {"_index": index or action["delete"].get("_index"), "_id": action["delete"]["_id"]}}
                ndjson_lines.append(json.dumps(meta))
            elif "update" in action:
                meta = {"update": {"_index": index or action["update"].get("_index"), "_id": action["update"]["_id"]}}
                ndjson_lines.append(json.dumps(meta))
                ndjson_lines.append(json.dumps({"doc": action["update"]["doc"]}))
            else:
                raise ValueError(f"Unsupported bulk action: {action}")
        return self._call("_bulk", method="POST", body=ndjson_lines, ndjson=True)

    def scan(self, index, query=None, scroll="2m", size=1000):
        """
        Generator to iterate through all docs in an index using scroll.
        :param index: index name
        :param query: query dict (defaults to match_all)
        :param scroll: keep scroll context alive (e.g. "2m")
        :param size: batch size per scroll
        """
        # Initial search with scroll
        body = query or {"query": {"match_all": {}}}
        params = {"scroll": scroll, "size": size}
        result = self._call(f"{index}/_search", method="POST", body=body, params=params)

        scroll_id = result.get("_scroll_id")
        hits = result["hits"]["hits"]

        while hits:
            for hit in hits:
                yield hit

            # Get next batch
            result = self._call("_search/scroll", method="POST", body={"scroll": scroll, "scroll_id": scroll_id})
            scroll_id = result.get("_scroll_id")
            hits = result["hits"]["hits"]
client = OpenSearchProxyClient(
    base_url="https://opensearch-gaas-dashboard.se-int-caas.ca.cib",
    username="admin",
    password="yourpassword",
    verify_ssl=False
)

# Scan through all documents in an index
for doc in client.scan(index="security-auditlog-2025.09.10", size=500):
    print(doc["_id"], doc["_source"])


bulk_res = client.bulk([
    {"index": {"_id": "1", "doc": {"user": "alice", "msg": "hello"}}},
    {"index": {"_id": "2", "doc": {"user": "bob", "msg": "hi"}}}
], index="my-index")

print("Bulk result:", bulk_res)

bulk_res = client.bulk([
    {"delete": {"_id": "1"}}
], index="my-index")

bulk_res = client.bulk([
    {"update": {"_id": "2", "doc": {"msg": "updated hi"}}}
], index="my-index")


