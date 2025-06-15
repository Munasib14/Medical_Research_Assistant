import os
import sys
from typing import List
from app.core.logger import get_logger
from app.core.config import get_settings
import spacy
# Ensure project root is in system path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

logger = get_logger("SentenceSplitter")

class SentenceSplitter:
    """
    Splits cleaned text into sentences using spaCy's sentence segmentation.

    Attributes:
        nlp: Preloaded spaCy language model for sentence segmentation.
    """

    def __init__(self, model_name: str = "en_core_web_sm"):
        """
        Initialize the SentenceSplitter with a specified spaCy model.

        Args:
            model_name (str): Name of the spaCy model to load.
        """
        try:
            logger.info(f"[SentenceSplitter] Loading spaCy model: {model_name}")
            self.nlp = spacy.load(model_name)
            logger.info("[SentenceSplitter] spaCy model loaded successfully.")
        except Exception as e:
            logger.error(f"[SentenceSplitter] Error loading spaCy model: {e}")
            raise RuntimeError(f"Failed to load spaCy model: {model_name}") from e

    def split_sentences(self, text: str) -> List[str]:
        """
        Splits the provided text into individual sentences.

        Args:
            text (str): Cleaned input text.

        Returns:
            List[str]: A list of sentences extracted from the text.
        """
        try:
            if not isinstance(text, str):
                logger.warning(f"[SentenceSplitter] Provided text is not a string: {text}")
                return []

            doc = self.nlp(text)
            sentences = [sent.text.strip() for sent in doc.sents if sent.text.strip()]

            return sentences

        except Exception as e:
            logger.error(f"[SentenceSplitter] Error splitting sentences: {e}")
            return []

