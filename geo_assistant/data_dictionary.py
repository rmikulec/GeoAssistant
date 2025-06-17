import faiss
import openai
import pathlib
import json
import numpy as np

from PyPDF2 import PdfReader
from pydantic import BaseModel, Field
from typing import Literal, Union, Self


FIELD_DEF_PARSE = """
You are an AI assistant specialized in extracting structured information from a data dictionary PDF. You will be given the complete, page-by-page text of a data dictionary (tables and entries may be split across pages). Your job is to:

1. Consolidate any multi-page or fragmented entries into a single coherent entry.  
2. Extract only the data exactly as presented in the text; do not infer, alter, summarize, or add any information.  
3. For each field definition, produce a `FieldDefinition` object capturing:
   - `name`: The formatted name of the field. Usually one of these formats (MyField, myField, my_field)
   - `name_pretty`: The raw, display-friendly name of the field
   - `description`: the full “Description:” text, preserving line breaks and markdown formatting if present.  
   - `source`: the exact “Data Source:” line(s).  
   - `format`: the exact type from the “Format:” line, mapped to one of `'number'`, `'boolean'`, or `'string'`.  

Produce exactly one JSON object conforming to the `DataDictionary` Pydantic schema, with key `"field_definitions". Output only this JSON—no additional text, commentary, or metadata.```
"""

SUPPLEMENT_INFO_PARSE = """
You are an AI assistant specialized in extracting data from a pdf. You are tasked with extracting all
supplemental data from a dictionary. This data is important for understanding the dictionary, while
not explicitly detailing all of the fields present. This could include any Abbreviations, Code lookups, relvant information, etc.

Please return the response as well formatted markdown, using sections, lists, and tables when needed
Please be detailed and include all appendencies
Please include exact information from all appendencies
Include all tables in all appendencies
"""

"""
Class definitions to define Pydantic models for Data Dictionary Parsing
"""
class FieldDefinition(BaseModel):
    name: str = Field(
        description=(
            "Machine-friendly field identifier"
            "(will be in ClassCase, snake_case or camelCase)."
        )
    )
    name_pretty: str = Field(
        description=(
            "Human-friendly field label taken from the PDF's 'Field Name' section, "
            "suitable for UI display."
        )
    )
    description: str = Field(
        description=(
            "Detailed explanation from the PDF's 'Description' section. "
            "Supports Markdown formatting."
            "Include all relevant information, including any abbreviations or codes"
        )
    )
    source: str = Field(
        description=(
            "Origin of the data as listed in the PDF's 'Data Source' section, "
            "e.g., department or system reference."
        )
    )
    format: Literal['number', 'boolean', 'string'] = Field(
        description=(
            "Normalized JSON data type based on the PDF's 'Format' section: 'string' for text, "
            "'number' for integers or floats, and 'boolean' for true/false values."
        )
    )


class DataDictionary(BaseModel):
    field_defintions: list[FieldDefinition]


class DataDictionaryStore:
    """
    Class to handle building and loading of a Field Definition Vector Store

    Methods:

        - load: Loads in a previously built vector store and documents
        - from_pdf: Uses openai to build a field def vector store from a data dictionary pdf
        - query: Queries the vector store for top k results
    """

    _client = openai.OpenAI(api_key=pathlib.Path("./openai.key").read_text())

    def __init__(self, index: faiss.IndexFlatL2, document_store: dict, supplement_info: str):
        self.index = index
        self.document_store = document_store
        self.supplement_info: str = supplement_info


    @classmethod
    def load(cls, store_path: Union[pathlib.Path, str]) -> Self:
        """
        Load a previously built vector store and documents

        Args:
            - store_path(Union[pathlib.Path, str]): The export path of the previously built
                vector store
        Returns:
            FieldDefinitionStore: A new field definition vector store
        Raises:
            Exception: Store path and files are not found
        """
        store_path = pathlib.Path(store_path)
        if store_path.exists():
            index = faiss.read_index(str(store_path/"index.bin"))
            document_store = json.loads((store_path/"documents.json").read_text())
            supplement_info = (store_path/"supplement_info.md").read_text()
        else:
            raise Exception("Invalid Index Path given, must contain both .bin and .json files")

        return cls(
            index=index,
            document_store=document_store,
            supplement_info=supplement_info
        )


    @staticmethod
    def _parse_docs(data_dictionary: DataDictionary) -> list[dict]:
        """
        Private method to parse a docs dictionary from a DataDictionary object
        """
        docs = []

        for i, field_def in enumerate(data_dictionary.field_defintions):
            docs.append(
                {
                    "id": i,
                    "text": f"{field_def.name_pretty}: {field_def.description}",
                    "metadata": {
                        **field_def.model_dump()
                    }
                }
            )
        
        return docs


    @classmethod
    def _add_embeddings(cls, docs: list[dict]) -> list[dict]:
        """
        Private method to add embeddings to a docs dictionary
        """
        resp = cls._client.embeddings.create(
            model="text-embedding-3-small",
            input=[d['text'] for d in docs]
        )
        # extract the vector
        for embedding_data, doc in zip(resp.data, docs):
            doc['embedding'] = embedding_data.embedding
        
        return docs
    

    @classmethod
    def _build_index(cls, data_dictionary: DataDictionary) -> faiss.IndexIDMap:
        """
        Private method to build an index from a DataDictionary object
        """
        docs = cls._parse_docs(data_dictionary)
        docs = cls._add_embeddings(docs)

        embeddings_np = np.array([doc['embedding'] for doc in docs], dtype="float32")
        faiss.normalize_L2(embeddings_np)
        dim = embeddings_np.shape[1]

        # we'll use a simple L2 index wrapped in an ID map
        index = faiss.IndexFlatIP(dim)
        index = faiss.IndexIDMap(index)

        # Assign Ids
        index.add_with_ids(embeddings_np, [doc['id'] for doc in docs])
        return index
        
    def _export_store(self, export_path: Union[str, pathlib.Path]):
        """
        Private method to export the index and document store to an export path
        """
        export_path = pathlib.Path(export_path)
        export_path.mkdir(parents=True, exist_ok=True)
        faiss.write_index(self.index, str(export_path/"index.bin"))
        (export_path/"documents.json").write_text(json.dumps(self.document_store, indent=2))
        (export_path/"supplement_info.md").write_text(self.supplement_info)


    @classmethod
    def from_pdf(cls, pdf_path: Union[str, pathlib.Path], export_path: Union[str, pathlib.Path]) -> Self:
        """
        Create a new Field Definition Index Store from a given data dictionary pdf

        Args:
            - pdf_path(Union[pathlib.Path, str]): The path to the data dictionary pdf
            - export_path(Union[pathlib.Path, str]): Where to export the store to
        Returns:
            FieldDefinitionStore: A newly built vector store of the field definitions parsed from
                the given pdf
        """
        # Set paths
        pdf_path = pathlib.Path(pdf_path)
        export_path = pathlib.Path(export_path)
        # Setup pdf reader
        reader = PdfReader(pdf_path)

        # Extract all PDF text
        pdf_text = ""
        for page_num, page in enumerate(reader.pages):
            pdf_text += f"\n\n"
            pdf_text += page.extract_text()
            pdf_text += "\n"

        # Parse field definitions
        res = cls._client.responses.parse(
            model="gpt-4o",
            input=[
                {'role': 'system', 'content': FIELD_DEF_PARSE},
                {'role': 'user', 'content': pdf_text}
            ],
            text_format=DataDictionary
        )
        data_dictionary = res.output_parsed
        docs = cls._parse_docs(data_dictionary)
        docs = cls._add_embeddings(docs)
        index = cls._build_index(data_dictionary)
        document_store = {doc['id']: doc['metadata'] for doc in docs}

        # Parse Supplement Info
        res = cls._client.responses.create(
            instructions=SUPPLEMENT_INFO_PARSE,
            input=pdf_text,
            model="gpt-4o",
            max_output_tokens=4_096
        )

        instance = cls(index, document_store, res.output_text)
        instance._export_store(export_path)
        return instance

    
    def query(self, message: str, k: int = 5) -> list[dict]:
        """
        Queries the FieldDefinitionStore for the top K closest results

        Args:
            message(str): The user's message to run the query on
            k(int): Returns the top 'k' results. Defaults to 5
        Returns:
            list[dict]: A list of the results from the document store
        """
        q_emb = self._client.embeddings.create(
            model="text-embedding-3-small",
            input=[message]
        ).data[0].embedding
        q_np = np.array([q_emb], dtype="float32")
        faiss.normalize_L2(q_np)

        distances, neighbors = self.index.search(q_np, k)

        results = []
        for rank, (idx, distance) in enumerate(zip(neighbors[0],distances[0]), start=1):
            doc = self.document_store[str(idx)]
            doc['distance'] = distance
            results.append(doc)

        return results
