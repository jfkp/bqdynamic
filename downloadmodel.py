
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
  

def upload_to_jfrog(model_id,model_path):
   # Initialize HfApi instance with the private endpoint and token
    api = HfApi(endpoint=os.environ["HF_ENDPOINT"], token='JF_TOKEN')

    try:
        # Upload the model to the private JFrog repository
        print(f"Uploading model from {model_path} to private JFrog repository...")
        api.upload_folder(
            folder_path="huggingface_mirror/models/sentence-transformers/all-MiniLM-L6-v2", 
            repo_id=model_id,
            repo_type="model",
            revision="5617a9f61b028005a4858fdac845db406aefb181"

        )
        print(f"Model {model_id} successfully uploaded to the private JFrog repository.")
    except Exception as e:
        print(f"An error occurred during the upload: {e}")

if __name__ == "__main__":

  
    configure_http_backend(backend_factory=backend_factory)
    model_id,model_path=download_model()
    model = SentenceTransformer("models/all-MiniLM-L6-v2")
    print(model.encode("Hello world"))
    
    upload_to_jfrog(model_id,model_path)
