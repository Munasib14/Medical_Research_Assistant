import os
import sys
import uuid
from typing import List, Dict
import pandas as pd
from tqdm import tqdm

from app.core.config import get_settings
from app.core.logger import get_logger

# Ensure the project root is in the system path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

settings = get_settings()
logger = get_logger("ChunkingService")


class ChunkingService:
    def __init__(self, chunk_size: int = None, overlap: int = None):
        self.chunk_size = chunk_size or settings.CHUNK_SIZE
        self.overlap = overlap or settings.CHUNK_OVERLAP

        if self.overlap >= self.chunk_size:
            raise ValueError("Overlap must be smaller than chunk size.")

    def chunk_text(self, text: str, max_tokens: int = None) -> List[str]:
        if not isinstance(text, str) or not text.strip():
            logger.warning("Empty or invalid text encountered during chunking.")
            return []

        chunk_size = max_tokens or self.chunk_size
        words = text.split()
        chunks = []

        for i in range(0, len(words), chunk_size - self.overlap):
            chunk = ' '.join(words[i:i + chunk_size])
            if chunk:
                chunks.append(chunk)

        logger.debug(f"Chunked text into {len(chunks)} chunks.")
        return chunks

    def chunk_dataframe(self, df: pd.DataFrame, text_column: str = 'body_text', max_tokens: int = None) -> pd.DataFrame:
        logger.info("🚀 Starting in-memory chunking of articles...")

        if text_column not in df.columns:
            logger.error(f"Input DataFrame must contain a '{text_column}' column.")
            raise ValueError(f"Missing '{text_column}' column in input DataFrame.")

        records: List[Dict] = []

        try:
            for row in tqdm(df.itertuples(index=False), total=len(df), desc="Chunking articles"):
                chunks = self.chunk_text(getattr(row, text_column, ''), max_tokens=max_tokens)
                for idx, chunk in enumerate(chunks):
                    records.append({
                        "chunk_id": str(uuid.uuid4()),
                        "paper_id": getattr(row, 'paper_id', ''),
                        "chunk_index": idx,
                        "chunk_text": chunk,
                        "title": getattr(row, 'title', ''),
                        "abstract": getattr(row, 'abstract_text', ''),
                        "journal": getattr(row, 'journal', ''),
                        "publish_time": getattr(row, 'publish_time', ''),
                        "doi": getattr(row, 'doi', ''),
                        "source": getattr(row, 'source', '')
                    })
        except Exception as e:
            logger.exception(f"Exception occurred while chunking articles: {e}")
            raise RuntimeError(f"Chunking failed: {e}")

        chunk_df = pd.DataFrame(records)
        logger.info(f"✅ Chunking complete. Generated {len(chunk_df)} chunks.")
        return chunk_df


# import os
# import sys 
# import uuid
# from typing import List, Dict
# import pandas as pd

# from app.core.config import get_settings
# from app.core.logger import get_logger

# # Ensure the project root is in the system path
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

# settings = get_settings()
# logger = get_logger("ChunkingService")


# class ChunkingService:
#     def __init__(self, chunk_size: int = None, overlap: int = None):
#         """
#         Initialize the ChunkingService with optional chunk size and overlap.
#         """
#         self.chunk_size = chunk_size or settings.CHUNK_SIZE
#         self.overlap = overlap or settings.CHUNK_OVERLAP

#         if self.overlap >= self.chunk_size:
#             raise ValueError("Overlap must be smaller than chunk size.")

#     def chunk_text(self, text: str) -> List[str]:
#         """
#         Split text into overlapping word-based chunks.

#         Args:
#             text (str): Input text to be chunked.

#         Returns:
#             List[str]: List of text chunks.
#         """
#         if not isinstance(text, str) or not text.strip():
#             logger.warning("Empty or invalid text encountered during chunking.")
#             return []

#         words = text.split()
#         chunks = []

#         for i in range(0, len(words), self.chunk_size - self.overlap):
#             chunk = ' '.join(words[i:i + self.chunk_size])
#             if chunk:
#                 chunks.append(chunk)

#         logger.debug(f"Chunked text into {len(chunks)} chunks.")
#         return chunks

#     def chunk_dataframe(self, df: pd.DataFrame, text_column: str = 'body_text') -> pd.DataFrame:
#         """
#         Chunk all articles in the DataFrame.

#         Args:
#             df (pd.DataFrame): DataFrame containing articles.
#             text_column (str): The column containing text to chunk.

#         Returns:
#             pd.DataFrame: DataFrame containing the generated text chunks.
#         """
#         logger.info("🚀 Starting in-memory chunking of articles...")

#         if text_column not in df.columns:
#             logger.error(f"Input DataFrame must contain a '{text_column}' column.")
#             raise ValueError(f"Missing '{text_column}' column in input DataFrame.")

#         records: List[Dict] = []

#         try:
#             for row in df.itertuples(index=False):
#                 chunks = self.chunk_text(getattr(row, text_column, ''))
#                 for idx, chunk in enumerate(chunks):
#                     records.append({
#                         "chunk_id": str(uuid.uuid4()),
#                         "paper_id": getattr(row, 'paper_id', ''),
#                         "chunk_index": idx,
#                         "chunk_text": chunk,
#                         "title": getattr(row, 'title', ''),
#                         "abstract": getattr(row, 'abstract_text', ''),
#                         "journal": getattr(row, 'journal', ''),
#                         "publish_time": getattr(row, 'publish_time', ''),
#                         "doi": getattr(row, 'doi', ''),
#                         "source": getattr(row, 'source', '')
#                     })
#         except Exception as e:
#             logger.exception(f"Exception occurred while chunking articles: {e}")
#             raise RuntimeError(f"Chunking failed: {e}")

#         chunk_df = pd.DataFrame(records)
#         logger.info(f"✅ Chunking complete. Generated {len(chunk_df)} chunks.")
#         return chunk_df




# # chunking_service.py
# import os
# import sys
# import uuid
# import pandas as pd
# from typing import List, Dict
# from app.core.config import get_settings
# from app.core.logger import get_logger


# # # Get the absolute path to the project root directory
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))


# settings = get_settings()
# logger = get_logger("ExtrationLogger")

# def chunk_text(text: str, chunk_size: int = None, overlap: int = None) -> List[str]:
#     """
#     Split text into overlapping word-based chunks.

#     Args:
#         text (str): Input text to be chunked.
#         chunk_size (int): Maximum size of each chunk in words.
#         overlap (int): Number of overlapping words between chunks.

#     Returns:
#         List[str]: List of text chunks.
#     """
    
#     chunk_size = chunk_size or settings.CHUNK_SIZE
#     overlap = overlap or settings.CHUNK_OVERLAP
    
#     if not isinstance(text, str) or not text.strip():
#         logger.warning("Empty or invalid text encountered during chunking.")
#         return []

#     if chunk_size <= 0:
#         logger.error("Chunk size must be a positive integer. Provided: %d", chunk_size)
#         return []

#     if overlap >= chunk_size:
#         logger.error("Overlap must be smaller than chunk size. Provided overlap: %d", overlap)
#         return []

#     words = text.split()

#     chunks = []
#     for i in range(0, len(words), chunk_size - overlap):
#         chunk = ' '.join(words[i:i + chunk_size])
#         if chunk:
#             chunks.append(chunk)

#     logger.debug("Chunked text into %d chunks.", len(chunks))
#     return chunks


# def chunk_articles(df: pd.DataFrame) -> pd.DataFrame:
#     """
#     Process all articles in the DataFrame and return a new DataFrame containing chunks.

#     Args:
#         df (pd.DataFrame): DataFrame containing articles with 'body_text' and metadata.

#     Returns:
#         pd.DataFrame: DataFrame containing the generated text chunks.
#     """
#     logger.info("Starting in-memory chunking of articles...")

#     if 'body_text' not in df.columns:
#         logger.error("Input DataFrame must contain a 'body_text' column.")
#         raise ValueError("Missing 'body_text' column in input DataFrame.")

#     records: List[Dict] = []

#     try:
#         for row in df.itertuples(index=False):
#             chunks = chunk_text(getattr(row, 'body_text', ''))
#             for idx, chunk in enumerate(chunks):
#                 records.append({
#                     "chunk_id": str(uuid.uuid4()),
#                     "paper_id": getattr(row, 'paper_id', ''),
#                     "chunk_index": idx,
#                     "chunk_text": chunk,
#                     "title": getattr(row, 'title', ''),
#                     "abstract": getattr(row, 'abstract_text', ''),
#                     "journal": getattr(row, 'journal', ''),
#                     "publish_time": getattr(row, 'publish_time', ''),
#                     "doi": getattr(row, 'doi', ''),
#                     "source": getattr(row, 'source', '')
#                 })
#     except Exception as e:
#         logger.exception("Exception occurred while chunking articles: %s", e)
#         raise

#     chunk_df = pd.DataFrame(records)
#     logger.info("Chunking complete. Generated %d chunks.", len(chunk_df))

#     return chunk_df
