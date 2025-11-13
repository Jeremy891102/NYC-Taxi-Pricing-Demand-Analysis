🚕 NYC TLC High Volume FHV DataLoader

Author: Jeremy Hsu

🧠 What It Does

This DataLoader helps you quickly load and process the NYC TLC High Volume FHV dataset (250M+ rows).
It’s built with Polars, so it’s fast, memory-efficient, and easy to use for everyone on the team.

You can pick the months and features you need — and get a clean dataframe ready for analysis.

⚙️ Key Features

🧩 Feature selection – just choose the columns you want

⚡ Lazy loading – loads data efficiently without running out of memory

🧼 Basic cleaning – removes invalid trips (0 miles, 0 fare, 0 time)

📅 Batch loading – process months in groups (e.g., 3 at a time)

🧮 Optional sampling – load a small fraction for quick testing

🔄 Datetime fix – automatically unifies timestamp precision across files

🧰 How to Use
1️⃣ Setup
pip install polars


Make sure your parquet files are under:

data/raw/
   fhvhv_tripdata_2024-01.parquet
   fhvhv_tripdata_2024-02.parquet
   ...

2️⃣ Example Usage
from src.data_loader import TLCDataLoader

loader = TLCDataLoader("data/raw")

df = loader.load_in_batches(
    features=["pickup_hour", "pickup_zone", "avg_price_per_mile"],
    months=[f"2024-{i:02d}" for i in range(1, 13)],
    batch_size=3,         # how many months per batch
    sample_ratio=1.0      # 1.0 = full data, 0.1 = 10% sample
)

print(df.shape)
print(df.head())


Output:

✅ Total rows: 239,340,416
📊 Shape: (239340416, 3)

3️⃣ List Available Features
loader.list_features()


Output:

pickup_datetime
pickup_hour
pickup_dayofweek
pickup_date
pickup_zone
dropoff_zone
trip_distance
trip_duration
fare
avg_price_per_mile

📦 What It Returns

A clean Polars DataFrame with only the columns you asked for.
Example:

shape: (5, 3)
┌─────────────┬─────────────┬────────────────────┐
│ pickup_hour ┆ pickup_zone ┆ avg_price_per_mile │
╞═════════════╪═════════════╪════════════════════╡
│ 0           ┆ 161         ┆ 16.11              │
│ 0           ┆ 137         ┆ 6.40               │
│ 0           ┆ 79          ┆ 9.12               │
└─────────────┴─────────────┴────────────────────┘

💡 Notes

The loader automatically filters out invalid trips (0 distance/time/fare).

You can easily turn cleaning off if needed.

Works perfectly for analysis modules:

Module 1 – Demand patterns

Module 2 – Price sensitivity

Module 3 – Supply-demand imbalance

Module 4 – Revenue and efficiency