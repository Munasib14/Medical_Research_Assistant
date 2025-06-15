import os
import sys
import traceback
from langchain_groq import ChatGroq
from langchain_community.vectorstores import FAISS
from langchain_community.embeddings import SentenceTransformerEmbeddings

from app.core.config import get_settings
from app.core.logger import get_logger
from app.models.langchain_wrapper import get_groq_llm
from app.models.groq_llm_model import LangchainWrapper

# Get the absolute path to the project root directory
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

settings = get_settings()
logger = get_logger("RetrievalService")


class RetrievalGenerationService:
    """
    Service to handle semantic retrieval and question answering using FAISS vector store and Groq LLM.
    """
    def __init__(self):
        try:
            logger.info("Starting RetrievalGenerationService initialization...")
            self.embedding_model_name = settings.EMBEDDING_MODEL_NAME
            self.FAISS_DB_DIR = settings.FAISS_DB_DIR
            self.FAISS_INDEX_NAME = settings.FAISS_INDEX_NAME
            self.TOP_K = settings.TOP_K

            self._load_embedding_model()
            self._load_vector_store()
            self._initialize_retriever()
            self._initialize_llm_chain()
            logger.info("RetrievalService initialized successfully.")
        except Exception as e:
            logger.error(f"Failed to initialize RetrievalService: {e}")
            logger.error(traceback.format_exc())
            raise


    def _load_embedding_model(self):
        """Load the embedding model."""
        try:
            logger.info(f"Loading embedding model: {self.embedding_model_name}")
            self.embedding_function = SentenceTransformerEmbeddings(model_name=self.embedding_model_name)
            logger.info("Embedding model loaded successfully.")
        except Exception as e:
            logger.error(f"Error loading embedding model: {e}")
            raise

    def _load_vector_store(self):
        """Load the FAISS vector store from disk."""
        try:
            logger.info(f"Connecting to FAISS DB at: {self.FAISS_DB_DIR}")
            self.db = FAISS.load_local(
                folder_path=self.FAISS_DB_DIR,
                index_name="faiss_index",  # Correct name without extension
                embeddings=self.embedding_function,
                allow_dangerous_deserialization=True  # Required for loading metadata
            )
            logger.info(f"Connected to FAISS DB using index: {self.FAISS_INDEX_NAME}")
        except Exception as e:
            logger.error(f"Error loading FAISS index: {e}")
            raise FileNotFoundError(f"FAISS index not found at {self.FAISS_DB_DIR}/{self.FAISS_INDEX_NAME}")

    def _initialize_retriever(self):
        """Initialize the FAISS retriever."""
        try:
            logger.info(f"Initializing retriever with Top-K: {self.TOP_K}")
            self.retriever = self.db.as_retriever(search_kwargs={"k": self.TOP_K})
            logger.info("Retriever initialized successfully.")
        except Exception as e:
            logger.error(f"Error initializing retriever: {e}")
            raise

    def _initialize_llm_chain(self):
        """Initialize the Groq LLM and LangChain QA wrapper."""
        try:
            logger.info("Loading Groq LLM and initializing QA chain...")
            self.llm = get_groq_llm()
            self.qa_chain = LangchainWrapper(llm=self.llm, retriever=self.retriever)
            logger.info("Groq LLM and QA chain initialized successfully.")
        except Exception as e:
            logger.error(f"Error initializing QA chain: {e}")
            raise

    def get_answer(self, query: str) -> dict:
        """
        Process a user query and return the answer along with source documents.

        Args:
            query (str): User input query.

        Returns:
            dict: Dictionary containing the query, answer, source documents, and error (if any).
        """
        try:
            logger.info(f"Processing query: {query}")
            response = self.qa_chain.ask(query)

            answer = response.get('answer', '')
            sources = response.get('source_documents', [])

            logger.info(f"Query processed successfully. Answer: {answer[:100]}...")

            return {
                "query": query,
                "answer": answer,
                "sources": [
                    {
                        "metadata": doc.metadata,
                        "content": doc.page_content
                    } for doc in sources
                ]
            }

        except Exception as e:
            logger.error(f"Error processing query: {e}")
            logger.error(traceback.format_exc())
            return {
                "query": query,
                "answer": None,
                "sources": [],
                "error": str(e)
            }