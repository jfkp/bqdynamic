 Extracted to models\sentence-transformers_all-MiniLM-L6-v2
`SentenceTransformer._target_device` has been deprecated, please use `SentenceTransformer.device` instead.
`SentenceTransformer._target_device` has been deprecated, please use `SentenceTransformer.device` instead.
`loss_type=None` was set in the config but it is unrecognized. Using the default loss: `ForCausalLMLoss`.
Traceback (most recent call last):
  File "c:\Users\U50HO63\Downloads\opensearch-wiki-confluence-qa-full\opensearch-wiki-confluence-qa-full\opensearch\opensearchembedding.py", line 198, in <module>
    torch_model_path = convert_hf_to_torchscript(model_dir, TORCHSCRIPT_MODEL)
                       ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "c:\Users\U50HO63\Downloads\opensearch-wiki-confluence-qa-full\opensearch-wiki-confluence-qa-full\opensearch\opensearchembedding.py", line 64, in convert_hf_to_torchscript
    traced = torch.jit.trace(wrapper, (example_input,))
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Users\U50HO63\Downloads\opensearch-wiki-confluence-qa-full\qaenv\Lib\site-packages\torch\jit\_trace.py", line 1002, in trace
    traced_func = _trace_impl(
                  ^^^^^^^^^^^^
  File "C:\Users\U50HO63\Downloads\opensearch-wiki-confluence-qa-full\qaenv\Lib\site-packages\torch\jit\_trace.py", line 696, in _trace_impl
    return trace_module(
           ^^^^^^^^^^^^^
  File "C:\Users\U50HO63\Downloads\opensearch-wiki-confluence-qa-full\qaenv\Lib\site-packages\torch\jit\_trace.py", line 1282, in trace_module
    module._c._create_method_from_trace(
RuntimeError: Type 'Tuple[List[str]]' cannot be traced. Only Tensors and (possibly nested) Lists, Dicts, and Tuples of Tensors can be traced
