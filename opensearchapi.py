import json
from opensearchpy import OpenSearch
from opensearchpy.helpers import bulk, scan


class OpenSearchAPI:
    def __init__(self, host, port=9200, username=None, password=None,
                 use_ssl=True, verify_certs=False):
        """
        Initialize connection to OpenSearch cluster.
        """
        self.client = OpenSearch(
            hosts=[{"host": host, "port": port}],
            http_auth=(username, password) if username and password else None,
            use_ssl=use_ssl,
            verify_certs=verify_certs,
            ssl_show_warn=False
        )

    # --- Cluster-level operations ---
    def info(self):
        """Get cluster information"""
        return self.client.info()

    def list_indices(self, pattern="*"):
        """List indices matching a pattern"""
        indices = self.client.indices.get_alias(pattern)
        return list(indices.keys())

    def create_index(self, index, mapping_file=None):
        """
        Create an index.
        If mapping_file is provided, it must be a path to a JSON file containing
        index settings and mappings.
        """
        body = {}
        if mapping_file:
            with open(mapping_file, "r", encoding="utf-8") as f:
                body = json.load(f)
        return self.client.indices.create(index=index, body=body, ignore=400)

    def update_mapping(self, index, mapping_file):
        """
        Update mapping of an existing index.
        Example JSON file can contain only 'mappings' section.
        """
        with open(mapping_file, "r", encoding="utf-8") as f:
            body = json.load(f)

        if "mappings" not in body:
            raise ValueError("Mapping file must contain a 'mappings' key for update")

        return self.client.indices.put_mapping(index=index, body=body["mappings"])

    def delete_index(self, index):
        """Delete an index"""
        return self.client.indices.delete(index=index, ignore=[400, 404])

    # --- Document-level operations ---
    def index_doc(self, index, doc_id, body, mapping_file=None):
        """
        Index (insert/replace) a single document.
        If the index does not exist, it will be created.
        If mapping_file is provided, it will be used for index creation.
        """
        if not self.client.indices.exists(index=index):
            self.create_index(index, mapping_file=mapping_file)

        return self.client.index(index=index, id=doc_id, body=body)

    def get_doc(self, index, doc_id):
        """Retrieve a document by ID"""
        return self.client.get(index=index, id=doc_id)

    def delete_doc(self, index, doc_id):
        """Delete a document by ID"""
        return self.client.delete(index=index, id=doc_id)

    # --- Bulk operations ---
    def bulk_index(self, index, docs, mapping_file=None):
        """
        Bulk index a list of documents.
        Example docs:
          docs = [{"_id": "1", "field": "value"}, {"_id": "2", "field": "value2"}]
        If the index does not exist, it will be created with optional mapping_file.
        """
        if not self.client.indices.exists(index=index):
            self.create_index(index, mapping_file=mapping_file)

        actions = [
            {"_op_type": "index", "_index": index, "_id": d["_id"], "_source": d}
            for d in docs
        ]
        success, _ = bulk(self.client, actions)
        return success

    # --- Search operations ---
    def search(self, index, query, size=10):
        """Run a search query"""
        return self.client.search(index=index, body={"query": query}, size=size)

    def scan(self, index, query, size=500):
        """Efficiently scroll through large result sets"""
        return scan(self.client, index=index, query={"query": query}, size=size)

    # --- Update operations ---
    def update_doc(self, index, doc_id, body):
        """Update a single document by ID"""
        return self.client.update(index=index, id=doc_id, body={"doc": body})

    def update_by_query(self, index, query, script=None):
        """
        Update documents matching a query.
        Example:
            script = {"source": "ctx._source.field = 'new_value'"}
        """
        body = {"query": query}
        if script:
            body["script"] = script
        return self.client.update_by_query(index=index, body=body)

    def delete_by_query(self, index, query):
        """Delete documents matching a query"""
        return self.client.delete_by_query(index=index, body={"query": query})


sample index.mapping

{
  "settings": {
    "number_of_shards": 1,
    "number_of_replicas": 1
  },
  "mappings": {
    "properties": {
      "user": { "type": "keyword" },
      "msg": { "type": "text" },
      "timestamp": { "type": "date" }
    }
  }
}


# Suppose we add a new field "status" in mapping file
api.update_mapping("my-index", "user_index_mapping.json")
