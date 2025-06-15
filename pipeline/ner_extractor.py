import os
import sys
import logging
from typing import List, Tuple
import spacy
from app.core.logger import get_logger
from app.core.config import get_settings

# Ensure project root is in system path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

# Get the logger instance
logger = get_logger("NERExtractor")
settings = get_settings()

class NERExtractor:
    """
    Extracts named entities from text using spaCy's NER.
    """

    def __init__(self):
        try:
            self.nlp = spacy.load("en_core_web_sm")
        except Exception as e:
            logger.error(f"[NERExtractor] Error loading spaCy model: {e}")
            raise e

    def extract_entities(self, text: str) -> List[Tuple[str, str]]:
        try:
            doc = self.nlp(text)
            entities = [(ent.text, ent.label_) for ent in doc.ents]
            return entities
        except Exception as e:
            logger.error(f"[NERExtractor] Error extracting entities: {e}")
            return []
