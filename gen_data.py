import pandas as pd
import numpy as np
from datetime import timedelta
import random

# Set random seed for reproducibility
np.random.seed(42)
random.seed(42)

# Number of rows
n_rows = 1000

# Generate the dataset
data = {
    # Numeric columns
    "length": np.random.normal(0.5, 0.3, n_rows),
    "width": np.random.normal(2.0, 1.0, n_rows),
    "height": np.random.normal(10.0, 3.0, n_rows),
    "weight": np.random.exponential(2.0, n_rows),
    "temperature": np.random.uniform(-10, 40, n_rows),
    "pressure": np.random.gamma(2, 2, n_rows),
    # Categorical column
    "category": np.random.choice(
        ["Type_A", "Type_B", "Type_C", "Type_D"], n_rows, p=[0.3, 0.25, 0.25, 0.2]
    ),
    # String column
    "product_name": [f"Product_{i:04d}" for i in range(n_rows)],
    # Boolean column
    "is_active": np.random.choice([True, False], n_rows, p=[0.7, 0.3]),
    # Datetime column
    "timestamp": pd.date_range(start="2020-01-01", end="2024-12-31", periods=n_rows),
    # Duration column (timedelta)
    "duration": [
        timedelta(
            days=np.random.randint(1, 100),
            hours=np.random.randint(0, 24),
            minutes=np.random.randint(0, 60),
        )
        for _ in range(n_rows)
    ],
    # Object column (string representations of mixed types - parquet compatible)
    "mixed_object": [
        f'{{"id": {i}, "value": {np.random.random():.3f}}}'
        if i % 3 == 0
        else f"string_{i}"
        if i % 3 == 1
        else str(np.random.randint(1, 100))
        for i in range(n_rows)
    ],
    # Empty column (all NaN)
    "empty_column": [np.nan] * n_rows,
}

# Create DataFrame
df = pd.DataFrame(data)

# Convert categorical column to proper categorical type
df["category"] = df["category"].astype("category")

# Add some NaN values to make it more realistic
# Add NaN to numeric columns
nan_indices = np.random.choice(n_rows, size=int(n_rows * 0.05), replace=False)
df.loc[nan_indices, "length"] = np.nan

nan_indices = np.random.choice(n_rows, size=int(n_rows * 0.03), replace=False)
df.loc[nan_indices, "weight"] = np.nan

# Add NaN to boolean column
nan_indices = np.random.choice(n_rows, size=int(n_rows * 0.02), replace=False)
df.loc[nan_indices, "is_active"] = np.nan

# Add NaT (Not a Time) to duration column
nan_indices = np.random.choice(n_rows, size=int(n_rows * 0.04), replace=False)
df.loc[nan_indices, "duration"] = pd.NaT

# Display info about the dataset
print("Dataset Info:")
print(f"Shape: {df.shape}")
print("\nData Types:")
print(df.dtypes)
print("\nFirst few rows:")
print(df.head())
print("\nSample of data types:")
print(df.info())

# Save as parquet file
try:
    df.to_parquet("synthetic_dataset.parquet", index=False)
    print("\nDataset saved as 'synthetic_dataset.parquet'")

    # Verify the parquet file
    df_loaded = pd.read_parquet("synthetic_dataset.parquet")
    print(f"\nLoaded dataset shape: {df_loaded.shape}")
    print("Data types after loading from parquet:")
    print(df_loaded.dtypes)

except Exception as e:
    print(f"\nError saving to parquet: {e}")
    print("Trying alternative approach...")

    # Convert problematic columns to strings
    df_copy = df.copy()

    # Convert object columns to strings where needed
    for col in df_copy.select_dtypes(include=["object"]).columns:
        if col != "empty_column":  # Keep empty column as is
            df_copy[col] = df_copy[col].astype(str)

    # Save the converted dataframe
    df_copy.to_parquet("synthetic_dataset.parquet", index=False)
    print(
        "Dataset saved as 'synthetic_dataset.parquet' (with object columns converted to strings)"
    )

    # Verify the parquet file
    df_loaded = pd.read_parquet("synthetic_dataset.parquet")
    print(f"\nLoaded dataset shape: {df_loaded.shape}")
    print("Data types after loading from parquet:")
    print(df_loaded.dtypes)
