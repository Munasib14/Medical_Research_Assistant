import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


import pandas as pd 
from app.core.logger import get_logger
from app.services import text_extraction_service, data_storage_service

logger = get_logger("Data_Prepare_logger")

def prepare_and_store_data():
    try:
        logger.info(f"Starting document extraction.....")
        
        # Step 0: Extract documents and generate batch parquet files
        text_extraction_service.extract_all_document()
        logger.info("Document extraction complete. Proceeding to combine batches...")
        
        # Step 1: Combine parquet batch files
        df = text_extraction_service.combine_parquet_batches()
        
        if df.empty:
            logger.info(f"No data extracted from documents. Exiting process.")
            return
        
        logger.info(f"Extracted {len(df)} records. Proceeding to save data...")
        saved_path = data_storage_service.save_dataframe(df)
        logger.info(f"Data preparation complete. File saved to: {saved_path}")
        
        
        # load the data frame 
        logger.info(f"Load the dataframe..")
        loaded_df = data_storage_service.load_dataframe(str(saved_path))
        
        # show the df 
        print("\nSample of loaded data:")
        print(len(loaded_df))
        print(loaded_df.head(5))
        loaded_df.info()

        
    except Exception as e:
        logger.info(f"Data preparation failed: {e}")
        
        
        
        
if __name__ == "__main__":
    prepare_and_store_data()
