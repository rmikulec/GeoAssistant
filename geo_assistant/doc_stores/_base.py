import faiss
import openai
import pathlib
import json
import numpy as np

from abc import ABC

from geo_assistant.config import Configuration


class DocumentStore(ABC):
    """
    Generic FAISS + JSON store base.
    Subclasses must define:
      - self.parse_prompt  (str)
      - self.parse_model   (pydantic model type)
      - self.index_dirname (str)
    """
    _client = openai.AsyncOpenAI(api_key=Configuration.openai_key)
    _name = "base"

    def __init__(
        self,
        version: str,
        vector_dim: int = Configuration.embedding_dims,
        embedding_model: str = Configuration.embedding_model,
        docstore_root: str = Configuration.docstore_path,
    ):
        self.version = version
        self.vector_dim = vector_dim
        self.embedding_model = embedding_model
        self.export_path = pathlib.Path(docstore_root)/ self._name / version
        self.export_path.mkdir(parents=True, exist_ok=True)

        # ------ Load or create FAISS index ----------
        idx_file = self.export_path / "index.bin"
        if idx_file.exists():
            self.index = faiss.read_index(str(idx_file))
        else:
            # new empty FlatIP + IDMap
            base = faiss.IndexFlatIP(self.vector_dim)
            self.index = faiss.IndexIDMap(base)

        # ------ Load or init document map ----------
        docs_file = self.export_path / "documents.json"
        if docs_file.exists():
            raw = json.loads(docs_file.read_text())
            self.documents = {int(k):v for k,v in raw.items()}
        else:
            self.documents = {}

    
    async def add(self, documents: list[dict], index_key: str, text_key: str):
        """
        Add multiple documents to the FAISS index and in-memory store, then persist both.

        Args:
            documents(list[dict]):  A list of dicts, each containing at least the ID and the text to embed.
            index_key(str):  The key in each dict whose value is the integer ID.
            text_key(str):   The key in each dict whose value is the text to embed.
        """
        # Extract IDs and texts
        ids   = [int(doc[index_key]) for doc in documents]
        texts = [doc[text_key]       for doc in documents]

        # Batch‐embed all texts in one call
        resp = await self._client.embeddings.create(
            model=Configuration.embedding_model,
            input=texts
        )
        # Load embeddings into numpy
        embs = np.array([item.embedding for item in resp.data], dtype="float32")

        # Normalize and add to FAISS in one shot
        faiss.normalize_L2(embs)
        self.index.add_with_ids(embs, np.array(ids, dtype="int64"))

        # Update in‐memory document map
        for doc_id, doc in zip(ids, documents):
            # drop the raw text if you prefer
            meta = {k: v for k, v in doc.items() if k != text_key}
            self.documents[doc_id] = meta

        # Persist both index and documents
        self._export()



    async def query(self, text: str, k: int=5) -> list[dict]:
        """
        Query the DocumentStore to recieve top k results

        Args:
            text(str): The user's query, text to be matched against
            k(int): Returns top k documents. Defaults to 5.
        Returns:
            list[dict]: Top k closest documents with distances
        """
        # Embed and normalize
        resp = await self._client.embeddings.create(
            model=self.embedding_model,
            input=[text]
        )
        vec = np.array(resp.data[0].embedding, dtype="float32").reshape(1, -1)
        faiss.normalize_L2(vec)

        # Search the index
        D, I = self.index.search(vec, k)
        # Tie back up with the documents
        out = []
        for dist, idx in zip(D[0], I[0]):
            doc = self.documents.get(idx, {}).copy()
            # Add distance to the results
            doc["distance"] = float(dist)
            out.append(doc)
        return out

    def _export(self):
        """
        Private method to export the index and documents. Be careful when calling.
        """
        faiss.write_index(self.index,      str(self.export_path / "index.bin"))
        (self.export_path/"documents.json").write_text(
            json.dumps(self.documents, indent=2)
        )
