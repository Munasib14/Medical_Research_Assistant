import os
import sys
import pandas as pd
from app.core.logger import get_logger
from app.core.config import get_settings
from pipeline.cleaning import TextCleaner
from pipeline.sentence_splitter import SentenceSplitter
from pipeline.ner_extractor import NERExtractor

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

logger = get_logger("BatchProcessor")
settings = get_settings()


class BatchProcessor:
    def __init__(self, input_file_path: str, batch_name: str, output_base_dir: str, metadata_path: str, text_column: str = "body_text"):
        self.input_file_path = input_file_path
        self.batch_name = batch_name
        self.output_base_dir = output_base_dir
        self.metadata_path = metadata_path
        self.text_column = text_column

        self.input_dir = input_file_path
        self.output_dir = self.output_base_dir
        self.checkpoint_file = os.path.join(self.output_dir, "processed_files.txt")

        self.cleaner = TextCleaner()
        self.splitter = SentenceSplitter()
        self.ner_extractor = NERExtractor()

        os.makedirs(self.output_dir, exist_ok=True)
        self.processed_files = self._load_checkpoint()

        self.metadata_df = pd.read_csv(self.metadata_path, low_memory=False)
        self.metadata_df = self.metadata_df[['sha', 'publish_time', 'journal', 'doi', 'license', 'url']]
        self.metadata_df.dropna(subset=['sha'], inplace=True)

    def _load_checkpoint(self):
        if os.path.exists(self.checkpoint_file):
            with open(self.checkpoint_file, "r") as f:
                processed = {line.strip() for line in f}
            logger.info(f"Loaded {len(processed)} processed files from checkpoint for this batch.")
            return processed
        else:
            return set()

    def _update_checkpoint(self, filename):
        with open(self.checkpoint_file, "a") as f:
            f.write(filename + "\n")

    def process_file(self, file_path: str):
        try:
            filename = os.path.basename(file_path)

            if filename in self.processed_files:
                logger.info(f"Skipping already processed file: {filename}")
                return

            logger.info(f"Processing file: {file_path}")
            df = pd.read_parquet(file_path)

            cleaned_texts = []
            sentences_list = []
            entities_list = []

            for text in df[self.text_column]:
                clean_text = self.cleaner.clean_text(text)
                sentences = self.splitter.split_sentences(clean_text)
                entities = self.ner_extractor.extract_entities(clean_text)

                cleaned_texts.append(clean_text)
                sentences_list.append(sentences)
                entities_list.append(entities)

            df["clean_text"] = cleaned_texts
            df["sentences"] = sentences_list
            df["named_entities"] = entities_list

            merged_df = df.merge(
                self.metadata_df,
                how='left',
                left_on='paper_id',
                right_on='sha'
            )

            merged_df.drop(columns=['sha'], inplace=True)

            output_file = os.path.join(self.output_dir, filename)
            merged_df.to_parquet(output_file, index=False)
            logger.info(f"Processed and merged file saved to: {output_file}")

            self._update_checkpoint(filename)

        except Exception as e:
            logger.error(f"Error processing file {file_path}: {e}")
            self._update_checkpoint(os.path.basename(file_path))
            logger.error(f"Error processing file {file_path}: {e}")
