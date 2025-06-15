import sys
import os
from app.services.faiss_service import FAISSService
from app.core.config import get_settings
from app.core.logger import get_logger

# Get the absolute path to the project root directory
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

settings = get_settings()
logger = get_logger("FAISSService")

def building_of_faiss():
    try:
        faiss_service = FAISSService(
            input_dir=settings.input_chroma_data,      # Read from config
            output_dir=settings.vector_db_path,        # Read from config
            embedding_model=settings.EMBEDDING_MODEL_NAME  # Read from config
        )

        faiss_service.build_faiss_index_with_resume()

    except Exception as e:
        logger.exception(f"Error occurred: {str(e)}")
        print(f"Error occurred: {str(e)}")

if __name__ == "__main__":
    building_of_faiss()
