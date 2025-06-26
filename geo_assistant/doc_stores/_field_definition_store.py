import pathlib
import hashlib
import asyncio
from typing import Union, List, Any, Dict, Literal
from pydantic import BaseModel, Field

from PyPDF2 import PdfReader, PageObject

from geo_assistant.logging import get_logger
from geo_assistant.config import Configuration
from geo_assistant.doc_stores._base import DocumentStore

logger = get_logger(__name__)

PARSE_SYSTEM_MESSAGE = """
You are an AI assistant specialized in extracting structured information from a data dictionary PDF. You will be given the complete, page-by-page text of a data dictionary (tables and entries may be split across pages). Your job is to:

1. Extract only the data exactly as presented in the text; do not infer, alter, summarize, or add any information.  
2. For each field definition, produce a `FieldDefinition` object capturing:
   - `name`: The formatted name of the field. Usually one of these formats (MyField, myField, my_field, MYFIELD, MY_FIELD)
   - `name_pretty`: The raw, display-friendly name of the field
   - `description`: the full “Description:” text, preserving line breaks and markdown formatting if present.  
   - `source`: the exact “Data Source:” line(s).  
   - `format`: the exact type from the “Format:” line, mapped to one of `'number'`, `'boolean'`, or `'string'`.  

Please include **every** field definition present in the entire text
"""


class FieldDefinition(BaseModel):
    name: str = Field(
        description=(
            "Machine-friendly field identifier"
            "(will be in ClassCase, snake_case, ALL_CAPS or camelCase)."
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
    enum: list[str] = Field(
        description="If applicable, a subset of values that are only available as responses to this field. Defaults to null",
        default=None
    )


class DataDictionary(BaseModel):
    field_defintions: list[FieldDefinition]



class FieldDefinitionStore(DocumentStore):
    _name = "field_definitions"
    _parse_prompt   = PARSE_SYSTEM_MESSAGE


    async def add_pdf(
        self,
        pdf_path: Union[str, pathlib.Path],
        table: str,
        start_page: int = None,
        end_page: int = None,
        batch_size: int = 2,
        window_size: int = 2
    ):
        """
        Build a new field-definition FAISS store from the given PDF.
        """
        self.table = table
        # Read & split PDF text
        pdf_path = pathlib.Path(pdf_path)
        reader   = PdfReader(pdf_path)

        if start_page and end_page:
            pages = reader.pages[start_page:end_page]
        elif start_page:
            pages = reader.pages[start_page:]
        elif end_page:
            pages = reader.pages[:end_page]
        else:
            pages= reader.pages


        page_batches = [
            pages[i : i + batch_size]
            for i in range(0, len(pages), window_size)
        ]

        async def _parse_data(page_batch: list[PageObject]):
            # Ask OpenAI to format into markdown
            resp = await self._client.responses.parse(
                instructions=self._parse_prompt,
                input="\n".join([page.extract_text() for page in page_batch]),
                model=Configuration.parsing_model,
                text_format=DataDictionary
            )
            return resp.output_parsed

        results = await asyncio.gather(*[_parse_data(page_batch) for page_batch in page_batches])
        field_definitions = [result.field_defintions for result in results]
        field_definitions = sum(field_definitions, [])


        # Turn each FieldDefinition into a document with id/text/metadata
        docs: List[Dict[str, Any]] = []
        for idx, fld in enumerate(field_definitions):
            id_ = hash(table+str(pdf_path.name)+str(idx))
            docs.append({
                "id":   id_,
                "text": f"{fld.name_pretty}: {fld.description}",
                "table": self.table,
                "source": str(pdf_path.name),
                **fld.model_dump()  # all other metadata
            })

        # Batch-add all our field-definition docs
        await self.add(docs, index_key="id", text_key="text")