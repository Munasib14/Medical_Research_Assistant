import re
import os
import sys
from app.core.logger import get_logger
from app.core.config import get_settings
# Get the absolute path to the project root directory
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))    

logger = get_logger("TextCleaner")
settings = get_settings()


class TextCleaner:
    """
    Cleans raw text by:
    - Removing HTML tags
    - Removing email addresses
    - Removing URLs
    - Removing emojis and non-ASCII characters
    - Normalizing whitespace
    - Lowercasing the text
    """

    def __init__(self):
        self.html_pattern = re.compile(r'<.*?>')
        self.email_pattern = re.compile(r'\S+@\S+\.\S+')
        self.url_pattern = re.compile(r'http\S+|www\S+')
        self.emoji_pattern = re.compile(r'[^\x00-\x7F]+')
        self.whitespace_pattern = re.compile(r'\s+')

    def clean_text(self, text: str) -> str:
        try:
            if not isinstance(text, str):
                logger.warning(f"[TextCleaner] Input is not a string: {text}")
                return ""

            text = self.html_pattern.sub(' ', text)
            text = self.email_pattern.sub(' ', text)
            text = self.url_pattern.sub(' ', text)
            text = self.emoji_pattern.sub(' ', text)
            text = self.whitespace_pattern.sub(' ', text)
            cleaned_text = text.strip().lower()

            if cleaned_text == "":
                logger.warning("[TextCleaner] Cleaned text is empty.")
                return ""

            return cleaned_text

        except Exception as e:
            logger.error(f"[TextCleaner] Error cleaning text: {e}")
            return ""

    def clean_batch(self, texts: list) -> list:
        """
        Clean a batch of text strings.
        """
        return [self.clean_text(text) for text in texts]
