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


         {
    "took": 10,
    "timed_out": false,
    "_shards": {
        "total": 7,
        "successful": 7,
        "skipped": 0,
        "failed": 0
    },
    "hits": {
        "total": {
            "value": 582,
            "relation": "eq"
        },
        "max_score": 1,
        "hits": [
            {
                "_index": ".kibana_1",
                "_id": "config:2.8.0",
                "_score": 1,
                "_source": {
                    "config": {
                        "buildNum": 6182
                    },
                    "type": "config",
                    "references": [],
                    "migrationVersion": {
                        "config": "7.9.0"
                    },
                    "updated_at": "2025-09-19T14:37:01.990Z"
                }
            },
            {
                "_index": "security-auditlog-2025.09.10",
                "_id": "PAqJM5kBEy-iU1hoLCdn",
                "_score": 1,
                "_source": {
                    "audit_cluster_name": "opensearch-gaas",
                    "audit_node_name": "opensearch-gaas-bootstrap-0",
                    "audit_trace_task_id": "PwmtyV36QFWOFMKnVgmkhg:197",
                    "audit_transport_request_type": "CreateIndexRequest",
                    "audit_category": "INDEX_EVENT",
                    "audit_request_origin": "REST",
                    "audit_request_body": "{\"number_of_shards\":\"1\",\"auto_expand_replicas\":\"0-1\"}",
                    "audit_node_id": "PwmtyV36QFWOFMKnVgmkhg",
                    "audit_request_layer": "TRANSPORT",
                    "@timestamp": "2025-09-10T12:10:49.204+00:00",
                    "audit_format_version": 4,
                    "audit_request_remote_address": "10.42.7.136",
                    "audit_request_privilege": "indices:admin/create",
                    "audit_node_host_address": "10.42.7.135",
                    "audit_request_effective_user": "admin",
                    "audit_trace_indices": [
                        ".kibana_1"
                    ],
                    "audit_node_host_name": "opensearch-gaas-bootstrap-0"
                }
            },
            {
                "_index": "security-auditlog-2025.09.10",
                "_id": "QwqJM5kBEy-iU1hoPCe7",
                "_score": 1,
                "_source": {
                    "audit_cluster_name": "opensearch-gaas",
                    "audit_node_name": "opensearch-gaas-bootstrap-0",
                    "audit_trace_task_id": "PwmtyV36QFWOFMKnVgmkhg:289",
                    "audit_transport_request_type": "RefreshRequest",
                    "audit_category": "INDEX_EVENT",
                    "audit_request_origin": "REST",
                    "audit_node_id": "PwmtyV36QFWOFMKnVgmkhg",
                    "audit_request_layer": "TRANSPORT",
                    "@timestamp": "2025-09-10T12:10:56.058+00:00",
                    "audit_format_version": 4,
                    "audit_request_remote_address": "10.42.10.152",
                    "audit_request_privilege": "indices:admin/refresh",
                    "audit_node_host_address": "10.42.7.135",
                    "audit_request_effective_user": "admin",
                    "audit_trace_indices": [
                        ".kibana_1"
                    ],
                    "audit_trace_resolved_indices": [
                        ".kibana_1"
                    ],
                    "audit_node_host_name": "opensearch-gaas-bootstrap-0"
                }
            },
            {
                "_index": "security-auditlog-2025.09.10",
                "_id": "QAqJM5kBEy-iU1hoOCcw",
                "_score": 1,
                "_source": {
                    "audit_cluster_name": "opensearch-gaas",
                    "audit_node_name": "opensearch-gaas-bootstrap-0",
                    "audit_trace_task_id": "PwmtyV36QFWOFMKnVgmkhg:273",
                    "audit_transport_request_type": "GetIndexRequest",
                    "audit_category": "INDEX_EVENT",
                    "audit_request_origin": "REST",
                    "audit_node_id": "PwmtyV36QFWOFMKnVgmkhg",
                    "audit_request_layer": "TRANSPORT",
                    "@timestamp": "2025-09-10T12:10:54.894+00:00",
                    "audit_format_version": 4,
                    "audit_request_remote_address": "10.42.7.136",
                    "audit_request_privilege": "indices:admin/get",
                    "audit_node_host_address": "10.42.7.135",
                    "audit_request_effective_user": "admin",
                    "audit_trace_indices": [
                        ".kibana"
                    ],
                    "audit_node_host_name": "opensearch-gaas-bootstrap-0"
                }
            },
            {
                "_index": "security-auditlog-2025.09.10",
                "_id": "OAqJM5kBEy-iU1hoJydk",
                "_score": 1,
                "_source": {
                    "audit_cluster_name": "opensearch-gaas",
                    "audit_node_name": "opensearch-gaas-bootstrap-0",
                    "audit_trace_task_id": "PwmtyV36QFWOFMKnVgmkhg:181",
                    "audit_transport_request_type": "GetIndexRequest",
                    "audit_category": "INDEX_EVENT",
                    "audit_request_origin": "REST",
                    "audit_node_id": "PwmtyV36QFWOFMKnVgmkhg",
                    "audit_request_layer": "TRANSPORT",
                    "@timestamp": "2025-09-10T12:10:48.755+00:00",
                    "audit_format_version": 4,
                    "audit_request_remote_address": "10.42.10.152",
                    "audit_request_privilege": "indices:admin/get",
                    "audit_node_host_address": "10.42.7.135",
                    "audit_request_effective_user": "admin",
                    "audit_trace_indices": [
                        ".kibana"
                    ],
                    "audit_node_host_name": "opensearch-gaas-bootstrap-0"
                }
            },
            {
                "_index": "security-auditlog-2025.09.10",
                "_id": "OgqJM5kBEy-iU1hoLCdl",
                "_score": 1,
                "_source": {
                    "audit_cluster_name": "opensearch-gaas",
                    "audit_node_name": "opensearch-gaas-bootstrap-0",
                    "audit_trace_task_id": "PwmtyV36QFWOFMKnVgmkhg:190",
                    "audit_transport_request_type": "GetIndexRequest",
                    "audit_category": "INDEX_EVENT",
                    "audit_request_origin": "REST",
                    "audit_node_id": "PwmtyV36QFWOFMKnVgmkhg",
                    "audit_request_layer": "TRANSPORT",
                    "@timestamp": "2025-09-10T12:10:49.189+00:00",
                    "audit_format_version": 4,
                    "audit_request_remote_address": "10.42.7.136",
                    "audit_request_privilege": "indices:admin/get",
                    "audit_node_host_address": "10.42.7.135",
                    "audit_request_effective_user": "admin",
                    "audit_trace_indices": [
                        ".kibana"
                    ],
                    "audit_node_host_name": "opensearch-gaas-bootstrap-0"
                }
            },
            {
                "_index": "security-auditlog-2025.09.10",
                "_id": "OwqJM5kBEy-iU1hoLCdm",
                "_score": 1,
                "_source": {
                    "audit_cluster_name": "opensearch-gaas",
                    "audit_node_name": "opensearch-gaas-bootstrap-0",
                    "audit_trace_task_id": "PwmtyV36QFWOFMKnVgmkhg:193",
                    "audit_transport_request_type": "GetIndexRequest",
                    "audit_category": "INDEX_EVENT",
                    "audit_request_origin": "REST",
                    "audit_node_id": "PwmtyV36QFWOFMKnVgmkhg",
                    "audit_request_layer": "TRANSPORT",
                    "@timestamp": "2025-09-10T12:10:49.196+00:00",
                    "audit_format_version": 4,
                    "audit_request_remote_address": "10.42.7.136",
                    "audit_request_privilege": "indices:admin/get",
                    "audit_node_host_address": "10.42.7.135",
                    "audit_request_effective_user": "admin",
                    "audit_trace_indices": [
                        ".kibana"
                    ],
                    "audit_node_host_name": "opensearch-gaas-bootstrap-0"
                }
            },
            {
                "_index": "security-auditlog-2025.09.10",
                "_id": "PgqJM5kBEy-iU1hoMSfm",
                "_score": 1,
                "_source": {
                    "audit_cluster_name": "opensearch-gaas",
                    "audit_node_name": "opensearch-gaas-bootstrap-0",
                    "audit_trace_task_id": "PwmtyV36QFWOFMKnVgmkhg:177",
                    "audit_transport_request_type": "GetIndexRequest",
                    "audit_category": "INDEX_EVENT",
                    "audit_request_origin": "REST",
                    "audit_node_id": "PwmtyV36QFWOFMKnVgmkhg",
                    "audit_request_layer": "TRANSPORT",
                    "@timestamp": "2025-09-10T12:10:48.676+00:00",
                    "audit_format_version": 4,
                    "audit_request_remote_address": "10.42.10.152",
                    "audit_request_privilege": "indices:admin/get",
                    "audit_node_host_address": "10.42.7.135",
                    "audit_request_effective_user": "admin",
                    "audit_trace_indices": [
                        ".kibana"
                    ],
                    "audit_node_host_name": "opensearch-gaas-bootstrap-0"
                }
            },
            {
                "_index": "security-auditlog-2025.09.10",
                "_id": "PQqJM5kBEy-iU1hoLCdt",
                "_score": 1,
                "_source": {
                    "audit_cluster_name": "opensearch-gaas",
                    "audit_node_name": "opensearch-gaas-bootstrap-0",
                    "audit_trace_task_id": "PwmtyV36QFWOFMKnVgmkhg:243",
                    "audit_transport_request_type": "GetIndexRequest",
                    "audit_category": "INDEX_EVENT",
                    "audit_request_origin": "REST",
                    "audit_node_id": "PwmtyV36QFWOFMKnVgmkhg",
                    "audit_request_layer": "TRANSPORT",
                    "@timestamp": "2025-09-10T12:10:51.883+00:00",
                    "audit_format_version": 4,
                    "audit_request_remote_address": "10.42.7.136",
                    "audit_request_privilege": "indices:admin/get",
                    "audit_node_host_address": "10.42.7.135",
                    "audit_request_effective_user": "admin",
                    "audit_trace_indices": [
                        ".kibana"
                    ],
                    "audit_node_host_name": "opensearch-gaas-bootstrap-0"
                }
            },
            {
                "_index": "security-auditlog-2025.09.10",
                "_id": "PwqJM5kBEy-iU1hoMidO",
                "_score": 1,
                "_source": {
                    "audit_cluster_name": "opensearch-gaas",
                    "audit_node_name": "opensearch-gaas-bootstrap-0",
                    "audit_trace_task_id": "PwmtyV36QFWOFMKnVgmkhg:258",
                    "audit_transport_request_type": "GetIndexRequest",
                    "audit_category": "INDEX_EVENT",
                    "audit_request_origin": "REST",
                    "audit_node_id": "PwmtyV36QFWOFMKnVgmkhg",
                    "audit_request_layer": "TRANSPORT",
                    "@timestamp": "2025-09-10T12:10:53.389+00:00",
                    "audit_format_version": 4,
                    "audit_request_remote_address": "10.42.7.136",
                    "audit_request_privilege": "indices:admin/get",
                    "audit_node_host_address": "10.42.7.135",
                    "audit_request_effective_user": "admin",
                    "audit_trace_indices": [
                        ".kibana"
                    ],
                    "audit_node_host_name": "opensearch-gaas-bootstrap-0"
                }
            }
        ]
    }
}
         
