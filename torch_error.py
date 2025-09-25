from transformers import AutoTokenizer, AutoModel
import torch

def convert_hf_to_torchscript(model_dir, output_path):
    # Load the model in Hugging Face form
    tokenizer = AutoTokenizer.from_pretrained(model_dir)
    model = AutoModel.from_pretrained(model_dir)

    class EncoderWrapper(torch.nn.Module):
        def __init__(self, model):
            super().__init__()
            self.model = model

        def forward(self, input_ids, attention_mask):
            outputs = self.model(input_ids=input_ids, attention_mask=attention_mask)
            # Use [CLS] token representation as embedding
            return outputs.last_hidden_state[:, 0, :]

    wrapper = EncoderWrapper(model)

    # Example tensorized input
    example = tokenizer("Hello OpenSearch!", return_tensors="pt")
    traced = torch.jit.trace(wrapper, (example["input_ids"], example["attention_mask"]))
    torch.jit.save(traced, output_path)

    print(f"✅ TorchScript model saved to {output_path}")
    return output_path
