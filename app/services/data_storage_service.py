import os
import sys
import json
import pandas as pd
from pathlib import Path
from typing import List, Dict, Optional
from app.core.config import get_settings
from app.core.logger import get_logger


# # Get the absolute path to the project root directory
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

settings = get_settings()
logger = get_logger("StorageLogger")


def save_dataframe(df:pd.DataFrame, path: str = None) -> Path: # type: ignore
    """
    Saves a DataFrame to disk in Parquet format.

    Args:
        df (pd.DataFrame): Data to save.
        path (str): Optional custom output path. Defaults to config setting.

    Returns:
        Path: Path to saved file.
    """
    
    save_path = Path(path or settings.clean_parquet_output_path) # type: ignore
    
    try:
        save_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(save_path, index=False)
        logger.info(f"DataFrame saved to {save_path}")
    
    except Exception as e:
        logger.exception(f"Failed to save DataFrame to {save_path}: {e}")
        raise
    
    return save_path

def load_dataframe(path: str = None) -> pd.DataFrame:
    """
    Loads a DataFrame from a Parquet file.

    Args:
        path (str): Optional custom path. Defaults to config setting.

    Returns:
        pd.DataFrame: Loaded DataFrame.
    """
    
    load_path = Path(path or settings.clean_parquet_output_path) # type: ignore
    if not load_path.exists():
        logger.error(f"file not found: {load_path}")
        raise FileNotFoundError(f"{load_path} does not exist")
    
    try:
        df = pd.read_parquet(load_path)
        logger.info("DataFrame loaded from {load_path}")
        return df
    except Exception as e:
        logger.exception("Failed to load DataFrame  from {load_path}: {e}")
        raise