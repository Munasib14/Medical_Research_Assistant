from pydantic_settings import BaseSettings
# from pydantic import BaseSettings
from pydantic import Field
from functools import lru_cache
from dotenv import load_dotenv
import os
# import spacy



# Get the absolute path to the project root directory and load the .env file
base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
dotenv_path = os.path.join(base_dir, ".env")
load_dotenv(dotenv_path)


class Settings(BaseSettings):
    # General settings
    app_name: str = "Medical Research Assistant"
    debug: bool = True

    # Model settings
    # EMBEDDING_MODEL_NAME: str = "intfloat/e5-large-v2"
    
    EMBEDDING_MODEL_NAME: str = Field(..., alias="EMBEDDING_MODEL")
    
    # Example FAISS config (you can update paths as per your setup)
    FAISS_DB_DIR: str = Field(default="C:/Users/Admin/Documents/medical-research-assistant/data/db_vector_store")  # Example folder path
    FAISS_INDEX_NAME: str = "faiss_index.index"             # FAISS index file name
    TOP_K: int = Field(default=5)  # Number of top results to retrieve
    # archive path for cord 19
    # cord19_archive_path:str = os.path.join(base_dir, "data//raw//cord-19_2022-06-02.tar.gz")
    # cord19_extracted_path:str = os.join(base_dir, "data//intermediate//cord-19_2022-06-02")
    # cord19_archive_path: str = os.path.join(base_dir, "data", "raw", "cord-19_2022-06-02.tar.gz")
    # cord19_extracted_path: str = os.path.join(base_dir, "data", "intermediate", "cord-19_2022-06-02")
    
    # Base directory for extracted JSON folders like `pdf_json`, `pmc_json`, etc.
    # cord19_extracted_path: str = os.getenv("CORD19_EXTRACTED_PATH", os.path.join(base_dir, "data/raw"))

    # JSON data folders (new additions for Phase 1)
    # Example URLs (replace with your actual file URLs)
    
    pdf_json_path: str = os.path.join(base_dir, "data", "raw", "pdf_json")
    pmc_json_path: str = os.path.join(base_dir, "data", "raw", "pmc_json")
    
    # Metadata CSV path for CORD-19
    cord19_metadata_path: str = os.path.join(base_dir, "data", "raw", "metadata.csv")

    
    # Output cleaned CSV path
    # clean_text_output_path: str = os.path.join(base_dir, "data", "processed", "clean_text_df.csv")
    
    # config.py (add this to Settings)
    # config.py (add this to Settings)
    extracted_parquet_path: str = os.path.join(base_dir, "data", "processed", "cord19_batches")
    input_for_embedding: str = os.path.join(base_dir, "data", "processed", "cord19_nlp_batches")
    clean_parquet_output_path: str = os.path.join(base_dir, "data", "processed", "cord19_cleaned.parquet")  
    
    parquet_input_path: str = os.path.join(base_dir, "data", "processed", "cord19_enriched.parquet")

    
    # Chunking settings
    CHUNK_SIZE: int = 500  # Number of words per chunk
    CHUNK_OVERLAP: int = 50  # Overlap between chunks

    # Embedding settings
    PROCESS_CHUNK_SIZE: int = 200 # total process chunk for embedding in single run 
    EMBEDDING_BATCH_SIZE: int = 20  # Batch size for embedding generation
    embedding_output_path: str = os.path.join(base_dir, "data", "embeddings", "embedding_batches")
    combine_embedding_path: str = os.path.join(base_dir, "data", "embeddings", "combine_embedding.parquet")
    
    
    # chroma db setting 
        ## Vector DB path
    input_chroma_data: str = os.path.join(base_dir, "data", "embeddings", "embedding_batches")
    # input_chroma_data: str = os.path.join(base_dir, "data", "embeddings", "combine_embedding.parquet") 
    vector_db_path: str = os.path.join(base_dir, "data", "db_vector_store")

    # Logging level
    logging_level: str = "INFO"
    # Output directory for NER CSVs
    # ner_combined_output_path: str = os.path.join(base_dir, "data", "processed", "ner_outputs", "ner_combined.parquet")
    
    # Default batch size and score threshold
    # ner_batch_size: int = 10
    # ner_min_score: float = 0.7

    
    # Add  model name
    # SCI_NLP_MODEL_NAME: str = "en_core_sci_lg"

    # API Keys
    groq_model_name: str = Field(..., alias="GROQ_MODEL")
    groq_api_key: str = Field(..., alias="GROQ_API_KEY")
    huggingface_api_key: str = Field(..., alias="HUGGINGFACE_API_KEY")

    class Config:
        env_file = ".env"
        validate_by_name = True

@lru_cache()
def get_settings():
    return Settings()

# def get_sci_nlp():
#     try:
#         return spacy.load("en_core_sci_lg")
#     except Exception as e:
#         raise RuntimeError(f"failed to load SciSpacy model: {e}")