from fastapi import APIRouter, HTTPException
from app.schemas.query_schema import QueryRequest, QAResponse
from app.services.Retrieval_Generation_Service import RetrievalGenerationService

router = APIRouter()

retrieval_service = RetrievalGenerationService()

@router.post("/ask-question", response_model=QAResponse)
async def ask_question(request: QueryRequest):
    result = retrieval_service.get_answer(request.query)

    if result.get("error"):
        raise HTTPException(status_code=500, detail=result["error"])

    return result
