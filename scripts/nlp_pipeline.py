import os
import sys
from app.core.config import get_settings
from app.core.logger import get_logger
from pipeline.batch_processor import BatchProcessor

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

logger = get_logger("BatchRunner")

def get_batch_files(parquet_path: str):
    """
    Retrieve a list of batch files from the provided base directory.
    """
    if not os.path.exists(parquet_path):
        logger.error(f"Input path does not exist: {parquet_path}")
        return []

    batch_files = [os.path.join(parquet_path, f) for f in os.listdir(parquet_path) if f.endswith('.parquet')]

    if not batch_files:
        logger.warning(f"No batch files found in directory: {parquet_path}")
    else:
        logger.info(f"Found {len(batch_files)} batch files to process.")

    return batch_files

def nlp_pipeline():
    """
    Executes the complete NLP processing pipeline across all batch files.
    """
    try:
        settings = get_settings()

        input_base_dir = settings.extracted_parquet_path
        output_base_dir = settings.input_for_embedding  # Output directory from config.py
        metadata_path = settings.cord19_metadata_path

        batch_files = get_batch_files(input_base_dir)

        if not batch_files:
            logger.warning("No batch files found to process. Exiting pipeline.")
            return

        for batch_file in batch_files:
            logger.info(f"Starting processing for batch file: {batch_file}")
            input_file_path = batch_file  # Full path already

            try:
                processor = BatchProcessor(
                    input_file_path=input_file_path,
                    output_base_dir=output_base_dir,
                    batch_name=os.path.basename(batch_file),
                    metadata_path=metadata_path
                )
                processor.process_file(batch_file)
                logger.info(f"Batch file {batch_file} processing completed successfully.\n")

            except Exception as e:
                logger.error(f"Error processing batch file {batch_file}: {e}")
                continue

        logger.info("All batch files processed successfully.")

    except Exception as e:
        logger.critical(f"Critical error in the NLP pipeline: {e}", exc_info=True)

if __name__ == "__main__":
    logger.info("Starting NLP pipeline...")
    nlp_pipeline()
    logger.info("NLP pipeline completed.")