# from fastapi import FastAPI
# from app.api.v1 import routes_qa, routes_health

# app = FastAPI(
#     title="Medical RAG Assistant",
#     version="1.0",
#     description="A Retrieval-Augmented Generation (RAG) API for Medical Queries"
# )

# # Routers
# app.include_router(routes_qa.router, prefix="/api/v1", tags=["Question Answering"])
# app.include_router(routes_health.router, prefix="/api/v1", tags=["Health Check"])

# @app.get("/")
# def read_root():
#     return {"message": "Welcome to Medical RAG Assistant API"}


# main.py
import time
from fastapi import FastAPI, Request
from app.api.v1 import routes_qa, routes_health
from app.core.logger import get_logger

app = FastAPI(
    title="Medical RAG Assistant",
    version="1.0",
    description="A Retrieval-Augmented Generation (RAG) API for Medical Queries"
)

# Logger setup
logger = get_logger("API")

# Routers
app.include_router(routes_qa.router, prefix="/api/v1", tags=["Question Answering"])
app.include_router(routes_health.router, prefix="/api/v1", tags=["Health Check"])

@app.get("/")
def read_root():
    return {"message": "Welcome to Medical RAG Assistant API"}

# Request/Response logging middleware
@app.middleware("http")
async def log_requests(request: Request, call_next):
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time

    logger.info(f"{request.method} {request.url} completed in {process_time:.2f}s with status code {response.status_code}")
    return response
