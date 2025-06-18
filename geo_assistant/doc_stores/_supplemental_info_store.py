import pathlib
import asyncio
from typing import Union, Any, Dict, List
from pydantic import BaseModel, Field

from PyPDF2 import PdfReader, PageObject

from geo_assistant.config import Configuration
from geo_assistant.doc_stores._base import DocumentStore


PARSE_SYSTEM_MESSAGE = """
You are an AI assistant specialized in extracting data from a pdf. You are tasked with extracting all
supplemental data from a dictionary. This data is important for understanding the dictionary, while
not explicitly detailing all of the fields present. This could include any Abbreviations, Code lookups, relvant information, etc.

Please return the response as well formatted markdown, using sections, lists, and tables when needed
Please be detailed and include all appendencies
Please include exact information from all appendencies
Include all tables in all appendencies
"""


class MarkdownSection(BaseModel):
    title: str
    markdown: str


class SupplementalInfo(BaseModel):
    sections: list[MarkdownSection] = Field(description="Markdown sections, paragraphs, and tables of information")


class SupplementalInfoStore(DocumentStore):
    """
    A FAISS-backed store for “supplemental” PDF content (appendices, code tables, etc).
    Inherits load-or-create, query, and persistence logic from BaseStore.
    """
    _name = "supplemental_info"
    _parse_prompt = PARSE_SYSTEM_MESSAGE

    async def add_pdf(
        self,
        pdf_path: Union[str, pathlib.Path],
        start_page: int = None,
        end_page: int = None,
        batch_size: int = 2,
        window_size: int = 2
    ) -> "SupplementalInfoStore":
        """
        Build a supplemental-info store by extracting the appendix pages,
        parsing them via OpenAI, embedding the result, and persisting.
        """
        pdf_path    = pathlib.Path(pdf_path)
        reader      = PdfReader(pdf_path)

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
                text_format=SupplementalInfo
            )
            return resp.output_parsed

        results = await asyncio.gather(*[_parse_data(page_batch) for page_batch in page_batches])
        sections = [result.sections for result in results]
        sections = sum(sections, [])

        # Prepare a single “document” containing all supplemental info
        docs: List[Dict[str, Any]] = [
            {
                "id":   len(self.documents) + idx,
                "text": f"{section.title}: {section.markdown}",
                "source": str(pdf_path.name),
                **section.model_dump()
            }
            for idx, section in enumerate(sections)
        ]

        # 5) Batch-add into FAISS + JSON
        await self.add(docs, index_key="id", text_key="text")