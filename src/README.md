🚕 NYC TLC High Volume FHV DataLoader

Author: Jeremy Hsu

🧠 What It Does

A simple data loader for NYC TLC High Volume FHV dataset (250M+ rows). Built with Polars for fast, memory-efficient loading.

You specify which columns and months you need → get raw data → handle feature engineering and cleaning yourself.

🧰 How to Use

**Setup:**
```bash
pip install polars
```

**Basic Usage:**
```python
import polars as pl
from src.data_loader import TLCDataLoader

loader = TLCDataLoader("data/raw")

# Load specific columns
df = loader.load(
    features=["PULocationID", "pickup_datetime", "trip_miles", "base_passenger_fare"],
    months=["2024-01", "2024-02"],
    sample_ratio=0.1  # 0.1 = 10% sample, 1.0 = full data
)
```

**For Large Datasets (12+ months):**
```python
# Process in batches to avoid memory issues
df = loader.load_in_batches(
    features=["PULocationID", "pickup_datetime", "trip_miles"],
    months=[f"2024-{i:02d}" for i in range(1, 13)],
    batch_size=3  # process 3 months at a time
)
```

**See Available Columns:**
```python
loader.list_features()
```

💡 How It Works

**The Logic:**

1. **You specify columns** → System validates they exist in parquet files
2. **System loads only those columns** → No wasted memory
3. **System normalizes datetime precision** → Prevents schema mismatch errors when merging files
4. **System returns raw data** → No preprocessing, no feature derivation

**What You Get:**
- Raw columns exactly as requested
- No automatic cleaning
- No automatic feature engineering
- You handle everything yourself

**Example:**
```python
# Load raw columns
df = loader.load(
    features=["pickup_datetime", "trip_miles", "base_passenger_fare"],
    months=["2024-01"]
)

# You compute features yourself
df = df.with_columns([
    pl.col("pickup_datetime").dt.hour().alias("pickup_hour"),
    (pl.col("base_passenger_fare") / pl.col("trip_miles")).alias("avg_price_per_mile")
])

# You handle cleaning yourself
df = df.filter(pl.col("trip_miles") > 0)
```

**Why This Design:**
- ✅ You control what gets loaded (saves memory)
- ✅ You control feature engineering (flexibility)
- ✅ You control data cleaning (transparency)
- ✅ System just loads data, nothing else