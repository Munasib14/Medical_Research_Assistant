from langchain_groq import ChatGroq  
from app.core.config import get_settings
from app.core.logger import get_logger


settings = get_settings()
logger = get_logger("GroqLLMModel")

def get_groq_llm():
    """
    Initialize and return a configured Groq LLM object.

    Returns:
        ChatGroq: Configured Groq LLM instance.
    """
    try:
        GROQ_MODEL = settings.groq_model_name
        GROQ_API_KEY = settings.groq_api_key
        logger.info(f"Initializing Groq LLM with model: {GROQ_MODEL}")
        llm = ChatGroq(
            groq_api_key=GROQ_API_KEY,
            model=GROQ_MODEL
        )
        logger.info("Groq LLM initialized successfully.")
        return llm
    except Exception as e:
        logger.error(f"Failed to initialize Groq LLM: {e}")
        raise RuntimeError(f"Groq LLM initialization failed: {e}")
