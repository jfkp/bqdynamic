import requests
import os
import json

# ====== CONFIG ======
OPENSEARCH_URL = "https://opensearch-atlasrag.se-int-caas.ca.cib"
OS_TOKEN = os.getenv("OS_TOKEN")  # your OpenSearch access token

TRITON_URL = "https://nvidia-triton.my-domain.com/v2/models/embeddings/infer"
TRITON_TOKEN = os.getenv("TRITON_TOKEN")  # optional if your Triton endpoint is protected

# ====== REGISTER CONNECTOR ======
def register_connector():
    url = f"{OPENSEARCH_URL}/_plugins/_ml/connectors/_register"
    headers = {
        "Authorization": f"Bearer {OS_TOKEN}",
        "Content-Type": "application/json"
    }

    payload = {
        "name": "nvidia-triton-connector",
        "description": "Connector to NVIDIA Triton embeddings service",
        "protocol": "http",
        "parameters": {
            "endpoint": TRITON_URL
        },
        "credential": {
            "token": TRITON_TOKEN
        } if TRITON_TOKEN else {}
    }

    resp = requests.post(url, headers=headers, data=json.dumps(payload), verify=False)
    resp.raise_for_status()
    connector_id = resp.json().get("connector_id")
    print(f"✅ Connector registered: {connector_id}")
    return connector_id

# ====== PREDICT USING CONNECTOR ======
def predict_with_connector(connector_id, text_list):
    url = f"{OPENSEARCH_URL}/_plugins/_ml/models/_predict"
    headers = {
        "Authorization": f"Bearer {OS_TOKEN}",
        "Content-Type": "application/json"
    }

    payload = {
        "model_id": connector_id,  # use connector as a "model"
        "parameters": {
            "input": text_list
        }
    }

    resp = requests.post(url, headers=headers, data=json.dumps(payload), verify=False)
    resp.raise_for_status()
    print("✅ Prediction response:", json.dumps(resp.json(), indent=2))
    return resp.json()

# ====== DEMO ======
if __name__ == "__main__":
    connector_id = register_connector()
    predict_with_connector(connector_id, ["OpenSearch integrates with NVIDIA Triton!"])




Strategy	Needs opensearch-ml plugin?	Who calls the model?	Where embeddings are generated?
Connector	✅ Yes	OpenSearch	External service (e.g. NVIDIA Triton)
Local model in Python	❌ No	Your Python app	Local machine
Direct API call from Python	❌ No	Your Python app	External service (e.g. Triton, OpenAI, Hugging Face)
