# test_storage.py

import pandas as pd

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from app.services.data_storage_service import save_dataframe, load_dataframe
from app.core.config import get_settings

# Load path from config
settings = get_settings()
test_path = settings.clean_parquet_output_path

# Step 1: Create sample DataFrame
data = {
    "title": ["COVID-19 Study", "SARS Vaccine"],
    "year": [2020, 2021],
    "citations": [120, 87]
}
df = pd.DataFrame(data)

# Step 2: Save DataFrame
print("📝 Saving DataFrame...")
saved_path = save_dataframe(df, test_path)

# Step 3: Load DataFrame
print("📂 Loading DataFrame...")
loaded_df = load_dataframe(test_path)

# Step 4: Print and compare
print("✅ Loaded DataFrame:")
print(loaded_df)

# Optional: Check if original and loaded data match
assert df.equals(loaded_df), "❌ Mismatch between saved and loaded DataFrame"
print("✅ Data verified successfully!")
