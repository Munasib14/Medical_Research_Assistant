import os
import sys
import json
from tqdm import tqdm
import pandas as pd
from typing import List, Dict, Optional
from app.core.config import get_settings
from app.core.logger import get_logger


# # Get the absolute path to the project root directory
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))


settings = get_settings()
logger = get_logger("ExtrationLogger")

# load the json file 
def load_json_file(filepath: str, ) -> Optional[dict]:
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            return json.load(f)
        
    except json.JSONDecodeError as e:
        logger.warning(f"Malformed JSON in file: {filepath} — Skipping. Error: {e}")
        return None
        
    except Exception as e:
        logger.error(f"failed to read the file: {filepath} — Error: {e}")
        return None
    

def extract_sections(json_data: Dict, source: str) -> Dict:
    try:
        paper_id = json_data.get("paper_id", "")

        metadata = json_data.get("metadata", {})
        title = metadata.get("title", "")

        authors = "; ".join([
            f"{author.get('first', '')} {author.get('last', '')}".strip()
            for author in metadata.get("authors", [])
        ])

        abstract = " ".join(
            entry.get("text", "")
            for entry in json_data.get("abstract", [])
        )

        body_text = " ".join(
            entry.get("text", "")
            for entry in json_data.get("body_text", [])
        )

        bib_entry_count = len(json_data.get("bib_entries", {}))
        ref_entry_count = len(json_data.get("ref_entries", {}))

        return {
            "paper_id": paper_id,
            "title": title,
            "authors": authors,
            "abstract_text": abstract,
            "body_text": body_text,
            "bib_entry_count": bib_entry_count,
            "ref_entry_count": ref_entry_count,
            "source": source
        }

    except Exception as e:
        logger.warning(f"Extraction failed from source: {source} | Error: {e}")
        return {}

    
# def extract_sections(json_data: Dict, source: str) -> Dict:
#     try:
#         paper_id = json_data.get("paper_id", "")
#         metadata = json_data.get("metadata", {})
        
#         title = metadata.get("title", "")
        
#         abstract = " ".join (entry.get("text", "") for entry in json_data.get("abstract", [])) # type: ignore
#         body_text = " ".join (entry.get("text", "") for entry in json_data.get("body_text", [])) # type: ignore
        
#         return {
#             "paper_id" : paper_id,
#             "metadata" : metadata,
#             "title"    : title,
#             "abstract" : abstract,
#             "body_text": body_text
            
#         }
        
#     except Exception as e:
#         logger.warning(f"Extraction failes from the source: {source} - {e}")
#         return {}
    

def load_checkpoint() -> set:
    ch_dir_path = settings.checkpoint_path
    if os.path.exists(ch_dir_path):
        try:
            with open(ch_dir_path, "r") as f:
                data = json.load(f)
                return set(data.get("processed files", []))
        
        except Exception as e:
            logger.warning(f"failed to load checkpoint: {e}")
    return set()

    

def save_checkpoint(processed_files: set):
    try:
        ch_dir_path = settings.checkpoint_path
        os.makedirs(os.path.dirname(ch_dir_path), exist_ok = True)
        with open(ch_dir_path, "w") as f:
            json.dump({"processed_files": list(processed_files)}, f, indent=2)
        logger.info(f"checkpoint saved with {len(processed_files)} files.")
        
    except Exception as e:
        logger.error(f"failed to save checkpoint: {e}")

#def extrat_all_document_to_df(sources: List[str] = ["pdf_json", "pmc_json"],  max_files: int = 3) -> pd.DataFrame:
# def extrat_all_document_to_df(sources: List[str] = ["pdf_json", "pmc_json"]) -> pd.DataFrame:
#     records = []
#     processed_files = load_checkpoint()
#     # files_processed = 0
    
#     for source in sources:
#         dir_path = os.path.join(settings.cord19_extracted_path, source)

        
#         if not os.path.exists(dir_path):
#             logger.warning(f"Directory not found: {dir_path}")
#             continue
        
#         logger.info(f"Scanning directory: {dir_path}")
#         for root, _, files in os.walk(dir_path):
#             for file in files:
#                 if file.endswith(".json"):
#                     file_path = os.path.join(root, file)
#                     json_data = load_json_file(file_path)
                    
#                     if json_data:
#                         doc = extract_sections(json_data, source)
                        
#                     if doc:
#                         records.append(doc)
#                         # processed_files.add(file_path)
#                         # save_checkpoint(processed_files)  # ✅ Save progress
#                         # files_processed += 1
                        
#                     # if files_processed >= max_files:
#                     #     logger.info(f"Reached max file limit: {max_files}")
#                     #     df = pd.DataFrame(records)
#                     #     logger.info(f"Extraction complete: {df.shape[0]} records extracted.")
#                     #     return df
                        
#     if not records:
#         logger.warning("No records were extracted — check if files are valid JSON and contain expected fields.")
        
#     df = pd.DataFrame(records)
#     logger.info(f"Extraction complete: {df.shape[0]} records extracted.")
#     return df  # ✅ Add this line    
    

def extract_all_document(sources: List[str] = ["pdf_json", "pmc_json"], batch_size: int = 5000) -> pd.DataFrame:
    output_dir = settings.extracted_parquet_path
    os.makedirs(output_dir, exist_ok=True)

    # Determine the last completed batch
    existing_batches = sorted([
        int(f.split("_")[1].split(".")[0]) for f in os.listdir(output_dir)
        if f.startswith("batch_") and f.endswith(".parquet")
    ])
    last_batch = existing_batches[-1] if existing_batches else 0
    batch_number = last_batch + 1
    processed_count = last_batch * batch_size

    logger.info(f"Last completed batch: {last_batch}")
    logger.info(f"Batch number starting from: {batch_number}")
    logger.info(f"Skipping first {processed_count} files")

    records = []
    file_counter = 0
    total_seen = 0
    total_limit = 100000
    source_limits = {"pdf_json": 50000, "pmc_json": 50000}
    source_counts = {src: 0 for src in sources}

    for source in sources:
        if source_counts[source] >= source_limits[source]:
            continue

        dir_path = os.path.join(settings.cord19_extracted_path, source)
        if not os.path.exists(dir_path):
            logger.warning(f"Directory not found: {dir_path}")
            continue

        logger.info(f"Scanning directory: {dir_path}")

        for root, _, files in os.walk(dir_path):
            for file in files:
                if file.endswith(".json"):
                    total_seen += 1
                    if total_seen <= processed_count:
                        continue  # Skip already processed files

                    if source_counts[source] >= source_limits[source]:
                        break  # Stop when limit for this source is reached

                    file_path = os.path.join(root, file)
                    json_data = load_json_file(file_path)

                    if json_data:
                        doc = extract_sections(json_data, source)
                        if doc:
                            records.append(doc)
                            file_counter += 1
                            source_counts[source] += 1

                    if file_counter >= batch_size:
                        batch_df = pd.DataFrame(records)
                        output_file = os.path.join(output_dir, f"batch_{batch_number}.parquet")
                        batch_df.to_parquet(output_file, index=False)
                        logger.info(f"Saved batch {batch_number} with {len(batch_df)} records.")
                        batch_number += 1
                        file_counter = 0
                        records = []

                if sum(source_counts.values()) >= total_limit:
                    logger.info(f"Reached global extraction limit: {total_limit}")
                    break
            if sum(source_counts.values()) >= total_limit:
                break

    # Save any remaining records
    if records:
        batch_df = pd.DataFrame(records)
        output_file = os.path.join(output_dir, f"batch_{batch_number}.parquet")
        batch_df.to_parquet(output_file, index=False)
        logger.info(f"Saved final batch {batch_number} with {len(batch_df)} records.")

    logger.info("Extraction complete.")
    return pd.DataFrame(records)


# def extract_all_document(sources: List[str] = ["pdf_json", "pmc_json"], batch_size: int = 20000) -> pd.DataFrame:
#     output_dir = settings.extracted_parquet_path
#     os.makedirs(output_dir, exist_ok=True)

#     # Determine the last completed batch
#     existing_batches = sorted([
#         int(f.split("_")[1].split(".")[0]) for f in os.listdir(output_dir)
#         if f.startswith("batch_") and f.endswith(".parquet")
#     ])
#     last_batch = existing_batches[-1] if existing_batches else 0
#     batch_number = last_batch + 1
#     processed_count = last_batch * batch_size

#     logger.info(f"Last completed batch: {last_batch}")
#     logger.info(f"batch Number: {batch_number}")
#     logger.info(f"Skipping first {processed_count} files")
    
#     records = []
#     file_counter = 0
#     total_seen = 0
#     # max_files = 10
#     # total_processed = 0
    
#     for source in sources:
#         dir_path = os.path.join(settings.cord19_extracted_path, source)        
#         if not os.path.exists(dir_path):
#             logger.warning(f"Directory not found: {dir_path}")
#             continue
        
#         logger.info(f"Scanning directory: {dir_path}")
#         for root, _, files in os.walk(dir_path):
#             for file in files:
#                 if file.endswith(".json"):
#                     total_seen += 1 
#                     if total_seen <= processed_count:
#                         continue  # Skip already processed files
#                     file_path = os.path.join(root, file)
#                     json_data = load_json_file(file_path)
                    
#                     if json_data:
#                         doc = extract_sections(json_data, source)                        
#                         if doc:
#                             records.append(doc)
#                             file_counter += 1
#                             # total_processed += 1
#                         # processed_files.add(file_path)
#                         # save_checkpoint(processed_files)  # ✅ Save progress
#                         # files_processed += 1
                        
#                     # if files_processed >= max_files:
#                     #     logger.info(f"Reached max file limit: {max_files}")
#                     #     df = pd.DataFrame(records)
#                     #     logger.info(f"Extraction complete: {df.shape[0]} records extracted.")
#                     #     return df
#                     if file_counter >= batch_size:
#                         batch_df = pd.DataFrame(records)
#                         output_file = os.path.join(output_dir, f"batch_{batch_number}.parquet")
#                         batch_df.to_parquet(output_file, index=False)
#                         logger.info(f"Saved batch {batch_number} with {len(batch_df)} records.")
#                         batch_number += 1
#                         file_counter = 0
#                         records = []
#             # if total_processed >= max_files:
#             #         break                
                        
                
# # Save final remaining records if any
#     if records:
#         batch_df = pd.DataFrame(records)
#         output_file = os.path.join(output_dir, f"batch_{batch_number}.parquet")
#         batch_df.to_parquet(output_file, index=False)
#         logger.info(f"Saved final batch {batch_number} with {len(batch_df)} records.")

#     logger.info("Extraction complete.")
#     return pd.DataFrame(records)  # Return final batch for inspection


def combine_parquet_batches() -> pd.DataFrame:
    """
    Combines all .parquet batch files from a directory into a single DataFrame,
    ensuring consistent columns across all batches.
    """
    batch_dir = settings.extracted_parquet_path
    logger.info(f"Combining parquet files from: {batch_dir}")

    if not os.path.exists(batch_dir):
        raise FileNotFoundError(f"Batch directory does not exist: {batch_dir}")

    batch_files = sorted([
        file for file in os.listdir(batch_dir) if file.endswith(".parquet")
    ])

    if not batch_files:
        raise FileNotFoundError(f"No .parquet batch files found in: {batch_dir}")

    all_batches = []
    all_columns = set()

    # First pass: collect all column names
    for file in tqdm(batch_files, desc="Analyzing batch schema"):
        file_path = os.path.join(batch_dir, file)
        try:
            batch_df = pd.read_parquet(file_path, engine='pyarrow')
            all_columns.update(batch_df.columns.tolist())
        except Exception as e:
            logger.warning(f"Failed to read {file_path}: {e}")

    all_columns = sorted(all_columns)  # for consistent column order

    # Second pass: read files and align columns
    for file in tqdm(batch_files, desc="Reading & aligning batches"):
        file_path = os.path.join(batch_dir, file)
        try:
            batch_df = pd.read_parquet(file_path, engine='pyarrow')
            for col in all_columns:
                if col not in batch_df.columns:
                    batch_df[col] = pd.NA
            batch_df = batch_df[all_columns]  # ensure column order
            all_batches.append(batch_df)
        except Exception as e:
            logger.warning(f"Failed to read/align {file_path}: {e}")

    final_df = pd.concat(all_batches, ignore_index=True)
    logger.info(f"Successfully combined {len(batch_files)} files.")
    logger.info(f"Final DataFrame shape: {final_df.shape}")
    return final_df


# def combine_parquet_batches() -> pd.DataFrame:
#     """
#     Combines all .parquet batch files from a directory into a single DataFrame.
#     """
    
#     batch_dir = settings.extracted_parquet_path
#     logger.info(f"Combining parquet files from: {batch_dir}")

#     if not os.path.exists(batch_dir):
#         raise FileNotFoundError(f"Batch directory does not exist: {batch_dir}")

#     batch_files = sorted([
#         file for file in os.listdir(batch_dir) if file.endswith(".parquet")
#     ])

#     if not batch_files:
#         raise FileNotFoundError(f"No .parquet batch files found in: {batch_dir}")

#     all_batches = []
#     for file in tqdm(batch_files, desc="Reading batches"):
#         file_path = os.path.join(batch_dir, file)
#         try:
#             batch_df = pd.read_parquet(file_path, engine='pyarrow')
#             all_batches.append(batch_df)
#         except Exception as e:
#             logger.warning(f"Failed to read {file_path}: {e}")

#     final_df = pd.concat(all_batches, ignore_index=True)
#     logger.info(f"Successfully combined {len(batch_files)} files.")
#     logger.info(f"Final DataFrame shape: {final_df.shape}")
#     return final_df
