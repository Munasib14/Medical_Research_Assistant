import os
import sys
import time
from typing import List
import pandas as pd
import numpy as np
from tqdm import tqdm
import faiss
from langchain_core.documents import Document
from langchain_huggingface.embeddings import HuggingFaceEmbeddings
from langchain_community.vectorstores import FAISS
from app.core.config import get_settings
from app.core.logger import get_logger

# Ensure project root is in system path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

settings = get_settings()
logger = get_logger("FAISSService")


class FAISSService:
    def __init__(self, input_dir: str = None, output_dir: str = None, embedding_model: str = None):
        self.input_dir = os.path.abspath(input_dir or settings.input_chroma_data)
        self.output_dir = os.path.abspath(output_dir or settings.vector_db_path)
        self.embedding_model = embedding_model or settings.EMBEDDING_MODEL_NAME

        self.index_path = os.path.join(self.output_dir, "faiss_index.index")
        self.dimension = None  # Dynamically set later

        self.embedding_function = HuggingFaceEmbeddings(model_name=self.embedding_model)
        self.processed_log_path = os.path.join(self.output_dir, "processed_batches.txt")

        if not os.path.exists(self.input_dir):
            logger.error("Input batch directory not found at %s", self.input_dir)
            raise FileNotFoundError(f"Input batch directory not found: {self.input_dir}")

        os.makedirs(self.output_dir, exist_ok=True)

        if not os.path.exists(self.processed_log_path):
            with open(self.processed_log_path, "w") as f:
                pass  # Create the file if it doesn't exist

        logger.info(f"FAISSService initialized successfully.\n"
                    f"Input Directory: {self.input_dir}\n"
                    f"Output Directory: {self.output_dir}")

        # Load or create FAISS index
        if os.path.exists(self.index_path):
            logger.info(f"Loading existing FAISS index from {self.index_path}")
            self.index = faiss.read_index(self.index_path)
            self.dimension = self.index.d

            # FIXED: Load the FAISS vector store with the security flag
            from langchain_community.vectorstores import FAISS
            self.faiss_vector_store = FAISS.load_local(
                folder_path=self.output_dir,
                index_name="faiss_index",
                embeddings=self.embedding_function,
                allow_dangerous_deserialization=True  # Add this parameter
            )
        else:
            logger.info("Creating a new FAISS index")
            self.index = None  # Will initialize dynamically later



    def _load_batch_files(self) -> List[str]:
        batch_files = sorted([
            f for f in os.listdir(self.input_dir)
            if f.startswith("batch_") and f.endswith(".parquet")
        ])
        if not batch_files:
            logger.error("No batch files found in embedding directory: %s", self.input_dir)
            raise FileNotFoundError("No batch files found in embedding directory.")
        return batch_files

    def _load_processed_batches(self) -> set:
        if not os.path.exists(self.processed_log_path):
            return set()
        with open(self.processed_log_path, "r") as f:
            return set(line.strip() for line in f if line.strip())

    def _log_processed_batch(self, batch_file: str):
        with open(self.processed_log_path, "a") as f:
            f.write(batch_file + "\n")
            f.flush()
            os.fsync(f.fileno())
        logger.info(f"Logged processed batch: {batch_file}")

    def _prepare_texts(self, df: pd.DataFrame) -> List[str]:
        texts = df['chunk_text'].dropna().tolist()
        if not texts:
            raise ValueError("No valid texts found in the dataset.")
        return texts

    def build_faiss_index_with_resume(self) -> None:
        try:
            processed_batches = self._load_processed_batches()
            batch_files = self._load_batch_files()

            logger.info(f"Total batches found: {len(batch_files)}")

            batches_processed = 0

            for idx, batch_file in enumerate(tqdm(batch_files, desc="Processing Batches", dynamic_ncols=True)):
                if batch_file in processed_batches:
                    logger.info(f"Skipping already processed batch: {batch_file}")
                    continue

                batch_path = os.path.join(self.input_dir, batch_file)
                logger.info(f"Processing batch: {batch_file} ({idx + 1}/{len(batch_files)})")

                start_time = time.time()
                try:
                    df = pd.read_parquet(batch_path)
                    texts = self._prepare_texts(df)

                    if not texts:
                        logger.warning(f"No texts to process in batch: {batch_file}")
                        self._log_processed_batch(batch_file)
                        continue

                    # Prepare documents with metadata
                    documents = []
                    for i, text in enumerate(texts):
                        documents.append(Document(page_content=text, metadata={"batch_file": batch_file, "doc_id": f"{batch_file}_{i}"}))

                    if self.index is None:
                        # Create FAISS vector store from documents
                        self.faiss_vector_store = FAISS.from_documents(documents, self.embedding_function)
                        self.index = self.faiss_vector_store.index
                        logger.info("Created new LangChain FAISS vector store.")
                    else:
                        # FIXED: Correctly reuse existing vector store and add documents
                        self.faiss_vector_store.add_documents(documents)
                        logger.info("Added new documents to existing LangChain FAISS vector store.")

                    self._save_index()
                    self._log_processed_batch(batch_file)
                    batches_processed += 1

                    elapsed_time = time.time() - start_time
                    logger.info(f"Batch {batch_file} processed successfully in {elapsed_time:.2f} seconds.")

                except Exception as batch_error:
                    logger.exception(f"Failed to process batch {batch_file}: {batch_error}")
                    continue  # Skip to next batch even if this one fails

            if batches_processed > 0:
                logger.info(f"All {batches_processed} new batches processed and persisted successfully.")
            else:
                logger.info("No new batches to process. All batches are already up-to-date.")

            logger.info("FAISS index build process completed successfully.")

        except Exception as e:
            logger.exception("Critical failure in FAISS index building: %s", str(e))
            raise
    
    def _save_index(self):
        # FIXED: Directly save the existing faiss_vector_store
        self.faiss_vector_store.save_local(folder_path=self.output_dir, index_name="faiss_index")
        logger.info(f"FAISS vector store (index + metadata) saved at {self.output_dir}")



        
