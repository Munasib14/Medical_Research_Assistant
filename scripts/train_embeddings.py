# import os
# import sys
# import pandas as pd

# # Ensure the project root is in the system path
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

# from app.services.chunking_service import ChunkingService
# from app.services.embedding_service import EmbeddingService
# from app.core.logger import get_logger
# from app.core.config import get_settings

# settings = get_settings()
# logger = get_logger("EmbeddingPipelineRunner")


# def run_chunking_and_embedding_pipeline(input_file_path: str = None):
#     """
#     Runs the complete chunking and embedding pipeline in memory (no disk persistence).

#     Args:
#         input_file_path (str): Path to the cleaned input file.

#     Returns:
#         pd.DataFrame: DataFrame containing chunked text and corresponding embeddings.
#     """
#     input_file_path = input_file_path or settings.parquet_input_path

#     if not os.path.exists(input_file_path):
#         logger.error(f"Input file not found at: {input_file_path}")
#         raise FileNotFoundError(f"Input file not found at: {input_file_path}")

#     try:
#         logger.info("Starting the chunking and embedding pipeline...")

#         # Load cleaned data
#         logger.info(f"Loading cleaned data from: {input_file_path}")
#         df = pd.read_parquet(input_file_path)
#         logger.info(f"Data loaded successfully with {len(df)} records.")

#         # Step 1: Chunking
#         chunking_service = ChunkingService()
#         chunked_df = chunking_service.chunk_dataframe(df, text_column='body_text', max_tokens=settings.CHUNK_SIZE)
#         logger.info(f"Chunking completed. Total chunks: {len(chunked_df)}")
#         print(chunked_df.head(5))
#         print(chunked_df.info())

#         # Step 2: Embedding
#         embedding_service = EmbeddingService()
#         embedded_df = embedding_service.generate_embeddings(chunked_df, column='chunk_text', batch_size=settings.EMBEDDING_BATCH_SIZE)
#         logger.info(f"Embedding completed. Total embeddings: {len(embedded_df)}")
#         print(embedded_df.head(5))
#         logger.info("Chunking and embedding pipeline executed successfully!")

#         return embedded_df  # Directly return for the next phase (like FAISS)

#     except Exception as e:
#         logger.exception(f"pipeline execution failed: {e}")
#         raise RuntimeError(f"Pipeline execution failed: {e}")


# if __name__ == "__main__":
#     run_chunking_and_embedding_pipeline()


import os
import sys
import pandas as pd

# Ensure the project root is in the system path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

from app.services.chunking_service import ChunkingService
from app.services.embedding_service import EmbeddingService
from app.core.logger import get_logger
from app.core.config import get_settings

settings = get_settings()
logger = get_logger("EmbeddingPipelineRunner")


def run_chunking_and_embedding_pipeline(input_file_path: str = None):
    """
    Runs the complete chunking and embedding pipeline in memory and saves combined embeddings.

    Args:
        input_file_path (str): Path to the cleaned input file.

    Returns:
        pd.DataFrame: Combined DataFrame containing chunked text and corresponding embeddings.
    """
    input_file_path = input_file_path or settings.parquet_input_path 

    if not os.path.exists(input_file_path):
        logger.error(f"Input file not found at: {input_file_path}")
        raise FileNotFoundError(f"Input file not found at: {input_file_path}")

    try:
        logger.info("Starting the chunking and embedding pipeline...")

        # Load cleaned data
        logger.info(f"Loading cleaned data from: {input_file_path}")
        df = pd.read_parquet(input_file_path)
        logger.info(f"Data loaded successfully with {len(df)} records.")

        # Step 1: Chunking
        chunking_service = ChunkingService()
        chunked_df = chunking_service.chunk_dataframe(df, text_column='body_text', max_tokens=settings.CHUNK_SIZE)
        logger.info(f"Chunking completed. Total chunks: {len(chunked_df)}")
        print(chunked_df.head(5))
        print(chunked_df.info())

        # Step 2: Embedding
        embedding_service = EmbeddingService()
        embedding_service.generate_embeddings(chunked_df, column='chunk_text', batch_size=settings.EMBEDDING_BATCH_SIZE, process_chunk_size=settings.PROCESS_CHUNK_SIZE)
        logger.info(f"Embedding completed and batches saved successfully!")

        # Step 3: Combine all batches into a single file
        combined_df = embedding_service.combine_batches()
        logger.info(f"Combined embedding file created successfully! Total records: {len(combined_df)}")
        print(combined_df.head(5))

        logger.info("Chunking, embedding, and combination pipeline executed successfully!")

        return combined_df  # Return combined DataFrame for next phase like FAISS indexing

    except Exception as e:
        logger.exception(f"Pipeline execution failed: {e}")
        raise RuntimeError(f"Pipeline execution failed: {e}")


if __name__ == "__main__":
    run_chunking_and_embedding_pipeline()
