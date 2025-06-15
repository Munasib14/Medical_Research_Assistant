# from app.core.config import get_settings
# from app.core.logger import get_logger
# import os, tarfile
# from typing import Optional

# settings = get_settings()
# logger = get_logger("Archive Logger")


# def extract_cord19_archive(archive_path: str, extract_to: str, overwrite: bool):
#     """
#     Extracts a .tar.gz archive to a specified directory.

#     Parameters:
#         archive_path (str): Path to the .tar.gz file
#         extract_to (str): Directory where files will be extracted
#         overwrite (bool): Whether to overwrite existing extraction folder
#     """
    
#     try:
#         if not os.path.exists(archive_path):
#             raise FileNotFoundError(f"Archive not found as :{archive_path}")
        
#         if os.path.exists(extract_to):
#             if overwrite: # type: ignore
#                 logger.info(f"Overwriting existing directory:{extract_to}")
                
#             else:
#                 logger.info(f"Extraction Skipped - Directory already exist:{extract_to}")
#                 return
#         else:
#             os.makedirs(extract_to, exist_ok=True)
            
#             with tarfile.open(archive_path, "r:gz") as tar:
#                 tar.extractall(path=extract_to)
#                 logger.info(f"Successfully extracted archive to:{extract_to}")
                
#     except Exception as e:
#         logger.error(f"failed to extract archive: {e}", exc_info=True)
#         raise
    

    
# extract_cord19_archive(
#         archive_path=settings.cord19_archive_path,
#         extract_to=settings.cord19_extracted_path,
#         overwrite=False  # or True if you want to force re-extraction
#     )