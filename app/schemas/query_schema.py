from typing import List, Dict, Optional
from pydantic import BaseModel

class QueryRequest(BaseModel):
    query: str

class SourceDocument(BaseModel):
    metadata: Dict[str, str]
    content: str

class QAResponse(BaseModel):
    query: str
    answer: Optional[str]
    sources: List[SourceDocument]
