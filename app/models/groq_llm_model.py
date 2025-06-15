from langchain.chains import RetrievalQA
from app.core.logger import get_logger

logger = get_logger("LangchainRetrievalService")

class LangchainWrapper:
    """
    A LangChain-based service for Retrieval Augmented Generation (RAG) QA systems.
    """

    def __init__(self, llm, retriever):
        """
        Initialize the LangChain Retrieval Service.

        Args:
            llm: The language model instance (e.g., Groq, OpenAI).
            retriever: The retriever instance for fetching relevant documents.
        """
        try:
            self.qa_chain = RetrievalQA.from_chain_type(
                llm=llm,
                retriever=retriever,
                return_source_documents=True
            )
            logger.info("LangChain Retrieval QA chain initialized successfully.")
        except Exception as e:
            logger.error(f"Error initializing LangChain Retrieval QA chain: {e}", exc_info=True)
            raise RuntimeError(f"Initialization failed: {e}")

    def ask(self, query: str) -> dict:
        """
        Process the query using the LangChain RAG pipeline.

        Args:
            query (str): The user's input question.

        Returns:
            dict: A dictionary containing the answer and source documents.
        """
        try:
            logger.info(f"Processing query: {query}")
            response = self.qa_chain(query)

            if not response or 'result' not in response:
                logger.warning("Query processed but no result found.")
                return {
                    "answer": None,
                    "source_documents": None,
                    "error": "No answer returned by the model."
                }

            logger.info("Query processed successfully.")
            return {
                "answer": response['result'],
                "source_documents": response.get('source_documents', [])
            }

        except Exception as e:
            logger.error(f"Error during query processing: {e}", exc_info=True)
            return {
                "answer": None,
                "source_documents": None,
                "error": str(e)
            }

