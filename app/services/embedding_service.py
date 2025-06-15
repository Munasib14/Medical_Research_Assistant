import os 
import sys
from sentence_transformers import SentenceTransformer
from tqdm import tqdm
import pandas as pd
from typing import Optional

from app.core.config import get_settings
from app.core.logger import get_logger


# # Get the absolute path to the project root directory
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))


settings = get_settings()
logger = get_logger("EmbeddingService")

# Add this for production: Set Hugging Face API Key globally
os.environ["HUGGINGFACE_HUB_TOKEN"] = settings.huggingface_api_key


class EmbeddingService:
    def __init__(self, model_name: Optional[str] = None):
        """
        Initialize the EmbeddingService with the specified model.
        """
        self.model_name = model_name or settings.EMBEDDING_MODEL_NAME
        self.model = self._load_model()

    def _load_model(self) -> SentenceTransformer:
        """
        Load the sentence transformer embedding model.

        Returns:
            SentenceTransformer: Loaded embedding model.
        """
        try:
            logger.info(f"🔍 Loading embedding model: {self.model_name}")
            model = SentenceTransformer(self.model_name)
            logger.info("✅ Embedding model loaded successfully.")
            return model
        except Exception as e:
            logger.exception(f"❌ Failed to load embedding model: {e}")
            raise RuntimeError(f"Failed to load embedding model: {e}")


    def generate_embeddings(
        self,
        df: pd.DataFrame,
        column: str = "chunk_text",
        batch_size: int = 5000,
        process_chunk_size: int = 10000  # Number of chunks to process per run
    ):
        """
        Generate and save embeddings in batches with auto-resume support.

        Args:
            df (pd.DataFrame): Input DataFrame containing text chunks.
            column (str): Column containing text to embed.
            batch_size (int): Batch size for embedding generation.
            process_chunk_size (int): Number of chunks to process in this run.
        """
        output_dir = settings.embedding_output_path
        os.makedirs(output_dir, exist_ok=True)

        # Find all existing batch files to skip them
        existing_batches = sorted([
            int(f.split("_")[1].split(".")[0]) for f in os.listdir(output_dir)
            if f.startswith("batch_") and f.endswith(".parquet")
        ])
        last_batch_index = max(existing_batches) if existing_batches else -1
        logger.info(f"✅ Last completed batch index: {last_batch_index}")

        total_chunks = len(df)
        logger.info(f"🚀 Total chunks in the dataset: {total_chunks}")

        # Calculate starting index for this run
        start_idx = (last_batch_index + 1) * batch_size

        if start_idx >= total_chunks:
            logger.info("🎉 All chunks have been processed. Nothing more to do.")
            return

        # Limit how many chunks to process in this run
        end_idx = min(start_idx + process_chunk_size, total_chunks)
        logger.info(f"🚀 Processing from chunk index {start_idx} to {end_idx - 1}")

        processed_chunks = 0

        for i in tqdm(range(start_idx, end_idx, batch_size), desc="Embedding Batches"):
            if processed_chunks >= process_chunk_size:
                logger.info("✅ Reached the chunk processing limit for this run.")
                break

            batch_number = i // batch_size

            batch_df = df.iloc[i:i + batch_size].copy()
            batch_texts = batch_df[column].tolist()

            try:
                logger.info(f"🚀 Generating embeddings for batch {batch_number}...")
                batch_embeddings = self.model.encode(batch_texts, show_progress_bar=False, normalize_embeddings=True)
                batch_df["embedding"] = batch_embeddings.tolist()

                batch_file_path = os.path.join(output_dir, f"batch_{batch_number}.parquet")
                batch_df.to_parquet(batch_file_path, index=False)

                logger.info(f"✅ Saved batch {batch_number} to {batch_file_path}")

                processed_chunks += len(batch_df)

            except Exception as e:
                logger.exception(f"❌ Failed to process batch {batch_number}: {e}")
                raise RuntimeError(f"Batch {batch_number} failed: {e}")

        logger.info("🎯 Embedding completed and batches saved successfully.")


    def combine_batches(self):
        """
        Combine all batch files into a single DataFrame and save to the configured path.
        """
        output_dir = settings.embedding_output_path
        batch_files = sorted([
            os.path.join(output_dir, f) for f in os.listdir(output_dir)
            if f.startswith("batch_") and f.endswith(".parquet")
        ])

        if not batch_files:
            logger.warning("⚠️ No batch files found to combine.")
            return None

        logger.info(f"🚀 Combining {len(batch_files)} batches...")

        combined_df = pd.concat((pd.read_parquet(f) for f in batch_files), ignore_index=True)
        logger.info(f"✅ Combined {len(batch_files)} batches. Total records: {len(combined_df)}")

        # Save the combined DataFrame to the configured path
        combined_output_path = settings.combine_embedding_path
        os.makedirs(os.path.dirname(combined_output_path), exist_ok=True)
        combined_df.to_parquet(combined_output_path, index=False)

        logger.info(f"✅ Combined embeddings saved to {combined_output_path}")

        return combined_df






# def generate_embeddings(
#         self,
#         df: pd.DataFrame,
#         column: str = "chunk_text",
#         batch_size: int = 5000,
#         total_limit: int = 10000
#     ):
#         """
#         Generate and save embeddings in batches with auto-resume support.

#         Args:
#             df (pd.DataFrame): Input DataFrame containing text chunks.
#             column (str): Column containing text to embed.
#             batch_size (int): Batch size for embedding generation.
#             total_limit (int): Total number of chunks to process.
#         """
#         output_dir = settings.embedding_output_path
#         os.makedirs(output_dir, exist_ok=True)  

#         # Find all existing batch files to skip them
#         existing_batches = sorted([
#             int(f.split("_")[1].split(".")[0]) for f in os.listdir(output_dir)
#             if f.startswith("batch_") and f.endswith(".parquet")
#         ])
#         last_batch_index = max(existing_batches) if existing_batches else -1
#         logger.info(f"✅ Last completed batch index: {last_batch_index}")

#         total_chunks = min(len(df), total_limit)
#         logger.info(f"🚀 Total chunks to process (limited to {total_limit}): {total_chunks}")

#         processed_chunks = 0

#         for i in tqdm(range(0, total_chunks, batch_size), desc="Embedding Batches"):
#             if processed_chunks >= total_limit:
#                 logger.info("✅ Reached total processing limit.")
#                 break

#             batch_number = i // batch_size

#             if batch_number <= last_batch_index:
#                 logger.info(f"✅ Batch {batch_number} already exists. Skipping...")
#                 continue

#             batch_df = df.iloc[i:i + batch_size].copy()
#             batch_texts = batch_df[column].tolist()

#             try:
#                 logger.info(f"🚀 Generating embeddings for batch {batch_number}...")
#                 batch_embeddings = self.model.encode(batch_texts, show_progress_bar=False, normalize_embeddings=True)
#                 batch_df["embedding"] = batch_embeddings.tolist()

#                 batch_file_path = os.path.join(output_dir, f"batch_{batch_number}.parquet")
#                 batch_df.to_parquet(batch_file_path, index=False)

#                 logger.info(f"✅ Saved batch {batch_number} to {batch_file_path}")

#                 processed_chunks += len(batch_df)

#             except Exception as e:
#                 logger.exception(f"❌ Failed to process batch {batch_number}: {e}")
#                 raise RuntimeError(f"Batch {batch_number} failed: {e}")

#         logger.info("🎯 All embeddings generated and saved successfully.")