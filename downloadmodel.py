
Uploading sentence-transformers/all-MiniLM-L6-v2 to https://registry.saas.cagip.group.gca/artifactory/huggingfaceml/sentence-transformers_all-MiniLM-L6-v2.zip

import os
from huggingface_hub.utils import RepositoryNotFoundError, RevisionNotFoundError, HfHubHTTPError
from huggingface_hub import snapshot_download
from huggingface_hub import HfApi
import requests
from huggingface_hub import configure_http_backend
from sentence_transformers import SentenceTransformer



os.environ["HF_HUB_ETAG_TIMEOUT"] = "1500000000"
os.environ["HF_ENDPOINT"] = "https://registry.saas.cagip.group.gca/artifactory/api/huggingfaceml/huggingface-remote"
os.environ["JF_TOKEN"] = 'token'
os.environ["HTTP_PROXY"] = "http://myproxy:8080"
os.environ["HTTPS_PROXY"] = "http://myproxy:8080"
os.environ['NO_PROXY'] = '.mydom'

def backend_factory() -> requests.Session:
    session = requests.Session()
    session.verify = False
    return session



def download_model():
    hf_token = 'hftoken'
    
    os.environ['HF_TOKEN'] = hf_token
    # Set the model ID
    model_id="sentence-transformers/all-MiniLM-L6-v2"
    # Set the base cache directory
    base_cache_dir = "models"
    try:
        # Check if the model exists and is accessible
        # api.model_info(model_id, token=hf_token)
        
        # If we get here, the model exists and is accessible
        print(f"Model {model_id} is accessible. Attempting to download...")
        
        # Download the model
        model_path = snapshot_download(
            repo_id=model_id,
            cache_dir=base_cache_dir,
            local_dir_use_symlinks=False,
            token=hf_token
        )
        print(f"Model downloaded to: {model_path}")
    except RepositoryNotFoundError:
        print(f"Error: The model {model_id} does not exist on Hugging Face.")
    except RevisionNotFoundError:
        print(f"Error: The specified revision of {model_id} was not found.")
    except HfHubHTTPError as e:
        if e.response.status_code == 403:
            print(f"Error: You don't have permission to access {model_id}.")
            print("Please check your token permissions and ensure you have access to this model.")
        else:
            print(f"HTTP Error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    return model_id,model_path
  

import shutil
import os
import requests

def upload_to_jfrog(model_id, model_path):
    if not model_path:
        print("⚠️ No model to upload.")
        return

    jfrog_token = os.environ["JF_TOKEN"]
    jfrog_base = os.environ["HF_ENDPOINT"]  # careful: this should point to your Artifactory REST API
    repo = "huggingfaceml"                  # adjust to your repo name
    artifact_name = f"{model_id.replace('/', '_')}.zip"

    # Full upload path (artifact storage URL, not API)
    upload_url = f"{jfrog_base}/{artifact_name}"

    print(f"📤 Uploading {model_id} to {upload_url} ...")

    try:
        # Create zip in current working directory
        zip_base = model_id.replace("/", "_")   
        zip_path = f"{zip_base}.zip"
        shutil.make_archive(zip_base, "zip", model_path)

        # Upload to JFrog
        with open(zip_path, "rb") as f:
            resp = requests.put(
                upload_url,
                headers={"Authorization": f"Bearer {jfrog_token}"},
                data=f,
                verify=False,
            )

        if resp.status_code in (200, 201):
            print("✅ Upload successful")

            # --- Verify using JFrog Storage API ---
            verify_url = f"{jfrog_base}/api/storage/{repo}/{artifact_name}"
            resp2 = requests.get(
                verify_url,
                headers={"Authorization": f"Bearer {jfrog_token}"},
                verify=False,
            )

            if resp2.status_code == 200:
                meta = resp2.json()
                print("🔎 Verification result:")
                print(f"  Repo: {meta.get('repo')}")
                print(f"  Path: {meta.get('path')}")
                print(f"  Size: {meta.get('size')} bytes")
                print(f"  Created: {meta.get('created')}")
                print(f"  SHA1: {meta['checksums'].get('sha1')}")
                print(f"  MD5: {meta['checksums'].get('md5')}")
            else:
                print(f"⚠️ Could not verify upload. Status: {resp2.status_code} {resp2.text}")

        else:
            print(f"❌ Upload failed: {resp.status_code}, {resp.text}")

    except Exception as e:
        print(f"❌ Upload error: {e}")

if __name__ == "__main__":

  
    configure_http_backend(backend_factory=backend_factory)
    model_id,model_path=download_model()
    model = SentenceTransformer("models/all-MiniLM-L6-v2")
    print(model.encode("Hello world"))
    
    upload_to_jfrog(model_id,model_path)
