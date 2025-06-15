# import sys
# import os
# import argparse
# from tqdm import tqdm
# import pandas as pd

# # Ensure parent directory is on sys.path
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# # Internal imports
# from app.core.logger import get_logger
# from app.services import data_storage_service
# from pipeline.ner_pipeline import run_biomedical_ner_pipeline
# from app.core.config import get_settings

# # Load settings
# settings = get_settings()

# # Constants from settings
# BATCH_SIZE = settings.ner_batch_size
# MIN_SCORE = settings.ner_min_score
# DEFAULT_INPUT_PATH = settings.clean_parquet_output_path
# DEFAULT_OUTPUT_PATH = settings.ner_combined_output_path
# OUTPUT_DIR = os.path.dirname(DEFAULT_OUTPUT_PATH)

# logger = get_logger("NER_Script")


# def process_batch(texts, doc_ids, output_records):
#     for text, doc_id in zip(texts, doc_ids):
#         if not text.strip():
#             logger.warning(f"Skipping empty text for doc_id={doc_id}")
#             continue
#         try:
#             result = run_biomedical_ner_pipeline(
#                 text=text,
#                 base_dir=OUTPUT_DIR,
#                 min_score=MIN_SCORE,
#                 doc_id=doc_id
#             )
#             ner_results = result.get("ner_results", [])
#             for entity in ner_results:
#                 entity["doc_id"] = doc_id
#                 output_records.append(entity)
#         except Exception as e:
#             logger.error(f"Failed to process doc_id={doc_id}: {e}", exc_info=True)


# def run_ner_pipeline(input_path: str = None, output_path: str = None):
#     try:
#         logger.info("Loading processed data...")

#         if input_path:
#             df = pd.read_parquet(input_path)
#         else:
#             df = data_storage_service.load_dataframe()

#         if df.empty:
#             logger.warning("Input DataFrame is empty.")
#             return

#         if "body_text" not in df.columns:
#             logger.error("body_text column is missing in the input data.")
#             return

#         texts = df["body_text"].fillna("").tolist()
#         doc_ids = df["doc_id"].fillna("").astype(str).tolist() if "doc_id" in df.columns else [str(i) for i in range(len(texts))]

#         logger.info(f"Starting NER on {len(texts)} documents in batches of {BATCH_SIZE}...")

#         all_entities = []
#         for i in tqdm(range(0, len(texts), BATCH_SIZE), desc="Running NER"):
#             batch_texts = texts[i:i + BATCH_SIZE]
#             batch_ids = doc_ids[i:i + BATCH_SIZE]
#             process_batch(batch_texts, batch_ids, all_entities)

#         if not all_entities:
#             logger.warning("No NER entities were extracted. Nothing will be saved.")
#             return

#         final_output_path = output_path or DEFAULT_OUTPUT_PATH
#         os.makedirs(os.path.dirname(final_output_path), exist_ok=True)
#         pd.DataFrame(all_entities).drop_duplicates().to_parquet(final_output_path, index=False)

#         logger.info(f"NER pipeline completed. Output saved to: {final_output_path}")

#     except Exception as e:
#         logger.error(f"NER pipeline failed: {e}", exc_info=True)


# if __name__ == "__main__":
#     parser = argparse.ArgumentParser(description="Run Biomedical NER Pipeline")
#     parser.add_argument("--input_path", type=str, help="Optional path to input Parquet file")
#     parser.add_argument("--output_path", type=str, help="Optional path to output Parquet file")
#     args = parser.parse_args()

#     run_ner_pipeline(input_path=args.input_path, output_path=args.output_path)






# # scripts/run_ner_pipeline.py
# import sys
# import os
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# from app.core.logger import get_logger
# from app.services import data_storage_service
# from pipeline.ner_pipeline import extract_and_save_entities



# logger = get_logger("NER_Script")

# def run_ner_pipeline():
#     try:
#         logger.info("Loading processed data for NER...")
#         df = data_storage_service.load_dataframe()

#         if df.empty:
#             logger.warning("No data available for NER.")
#             return
        
        
#         print("Available columns:", df.columns.tolist())
#         print(df.head())
        
#         if "body_text" not in df.columns:
#             logger.error("Expected column 'text' not found in DataFrame.")
#             print("Available columns:", df.columns.tolist())
#             exit(1)


#         texts = df["body_text"].dropna().tolist()
#         logger.info(f"Running NER on {len(texts)} documents.")
        
#         # ✅ Call the actual NER function from pipeline.ner_pipeline
#         extract_and_save_entities(texts)

#     except Exception as e:
#         logger.error(f"NER pipeline failed: {e}")

# if __name__ == "__main__":
#     run_ner_pipeline()
