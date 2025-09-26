{
  "mappings": {
    "properties": {
      "title": {
        "type": "text"
      },
      "url": {
        "type": "keyword"
      },
      "text": {
        "type": "text"
      },
      "parent": {
        "type": "keyword"
      },
      "metadata": {
        "properties": {
          "space": {
            "type": "keyword"
          },
          "page_id": {
            "type": "keyword"
          },
          "chunk": {
            "type": "integer"
          },
          "comments": {
            "type": "nested",
            "properties": {
              "author": { "type": "keyword" },
              "text":   { "type": "text" }
            }
          },
          "attachments": {
            "type": "nested",
            "properties": {
              "name": { "type": "keyword" },
              "link": { "type": "keyword" }
            }
          }
        }
      },
      "embedding": {
        "type": "knn_vector",
        "dimension": 384  // adjust depending on your embedding model
      }
    }
  }
}


def ingest_page(page, model, api, index_name):
    page_id = page["id"]
    title = page["title"]
    url = page["url"]
    parent = page.get("parent")
    attachments = page.get("attachments", [])
    comments = page.get("comments", [])
    
    # --- Base content ---
    text = clean_html(page["content"])

    # --- Process comments ---
    comments_structured = []
    if comments:
        for c in comments:
            author = c.get("creator", {}).get("displayName", "Unknown")
            body = clean_html(c.get("body", {}).get("storage", {}).get("value", ""))
            comments_structured.append({"author": author, "text": body})
        text += "\n\n" + "\n".join([f"Comment by {c['author']}: {c['text']}" for c in comments_structured])

    # --- Process attachments ---
    attachments_structured = []
    if attachments:
        for a in attachments:
            name = a.get("title", "Unknown file")
            link = a.get("_links", {}).get("download", "")
            attachments_structured.append({"name": name, "link": link})
        text += "\n\n" + "\n".join([f"Attachment: {a['name']} ({a['link']})" for a in attachments_structured])

    # --- Chunk + embed ---
    chunks = [text[i:i+1500] for i in range(0, len(text), 1500)]
    for i, chunk in enumerate(chunks):
        emb = model.encode(chunk).tolist()
        doc = {
            "title": title,
            "url": url,
            "text": chunk,
            "parent": parent,
            "metadata": {
                "space": page.get("space"),
                "page_id": page_id,
                "chunk": i,
                "comments": comments_structured,
                "attachments": attachments_structured
            },
            "embedding": emb,
        }
        api.index_doc(index_name, f"{page_id}_{i}", doc, mapping_file="opensearch/opensearch_mapping.json")

    print(f"Ingested {title} with {len(chunks)} chunks (body + comments + attachments)")


def ingest_space(space_pages, model, api, index_name):
    """
    Ingests an entire Confluence space (list of pages).
    Each page goes through `ingest_page`, which handles body, comments, and attachments.
    """
    for page in space_pages:
        try:
            ingest_page(page, model, api, index_name)
        except Exception as e:
            print(f"⚠️ Failed to ingest page {page.get('id')}: {e}")
