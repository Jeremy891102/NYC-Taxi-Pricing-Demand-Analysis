"""
NYC TLC High Volume FHV DataLoader (Production Ready)
------------------------------------------------------
✅ Unified Polars DataLoader for 250M+ rows
✅ Modular, memory-efficient, and team-friendly
✅ Supports feature selection and month-based loading
------------------------------------------------------
"""

import polars as pl
from pathlib import Path
from glob import glob
from typing import List, Optional, Dict


RAW_DATA_DIR = Path("data/raw")

class TLCDataLoader:
    """
    A production-ready Polars DataLoader for NYC TLC High Volume FHV dataset.

    Core Features:
    ---------------
    - Lazy loading with Polars (low memory footprint)
    - Automatic column selection based on requested features
    - Auto merges monthly parquet files
    - Built-in cleaning and type casting
    """

    # Define all available features and their derivation logic
    FEATURE_MAP: Dict[str, Dict] = {
        "pickup_datetime": {
            "requires": ["pickup_datetime"],
            "derive": pl.col("pickup_datetime")
        },
        "pickup_hour": {
            "requires": ["pickup_datetime"],
            "derive": pl.col("pickup_datetime").dt.hour().alias("pickup_hour")
        },
        "pickup_dayofweek": {
            "requires": ["pickup_datetime"],
            "derive": pl.col("pickup_datetime").dt.weekday().alias("pickup_dayofweek")
        },
        "pickup_date": {
            "requires": ["pickup_datetime"],
            "derive": pl.col("pickup_datetime").dt.date().alias("pickup_date")
        },
        "pickup_zone": {
            "requires": ["PULocationID"],
            "derive": pl.col("PULocationID").alias("pickup_zone")
        },
        "dropoff_zone": {
            "requires": ["DOLocationID"],
            "derive": pl.col("DOLocationID").alias("dropoff_zone")
        },
        "trip_distance": {
            "requires": ["trip_miles"],
            "derive": pl.col("trip_miles").alias("trip_distance")
        },
        "trip_duration": {
            "requires": ["trip_time"],
            "derive": pl.col("trip_time").alias("trip_duration")
        },
        "fare": {
            "requires": ["base_passenger_fare"],
            "derive": pl.col("base_passenger_fare").alias("fare")
        },
        "avg_price_per_mile": {
            "requires": ["base_passenger_fare", "trip_miles"],
            "derive": (pl.col("base_passenger_fare") / pl.col("trip_miles")).alias("avg_price_per_mile")
        }
    }

    # ========== Initialization ==========
    def __init__(self, data_dir: Path = RAW_DATA_DIR):
        self.data_dir = Path(data_dir)
        self.files = sorted(glob(str(self.data_dir / "fhvhv_tripdata_*.parquet")))
        if not self.files:
            raise FileNotFoundError(f"No parquet files found under {self.data_dir}")
        print(f"🚀 TLCDataLoader initialized with {len(self.files)} monthly files.")

    # ========== Helper: Select files to read ==========
    def _select_files(self, months: Optional[List[str]] = None) -> List[str]:
        if not months:
            return self.files
        selected = [f for f in self.files if any(m in f for m in months)]
        print(f"📂 Selected {len(selected)} files: {[Path(f).name for f in selected]}")
        return selected

    # ========== Helper: Determine required raw columns ==========
    def _required_columns(self, features: List[str]) -> List[str]:
        cols = set()
        for f in features:
            if f not in self.FEATURE_MAP:
                raise ValueError(f"❌ Feature '{f}' not defined in FEATURE_MAP.")
            cols.update(self.FEATURE_MAP[f]["requires"])
        return list(cols)

    # ========== Helper: Add derived features ==========
    def _derive_features(self, df: pl.LazyFrame, features: List[str]) -> pl.LazyFrame:
        for f in features:
            expr = self.FEATURE_MAP[f]["derive"]
            df = df.with_columns(expr)
        return df

    # ========== Main function ==========
    def load(
        self,
        features: List[str],
        months: Optional[List[str]] = None,
        sample_ratio: float = 1.0,
        collect: bool = True
    ) -> pl.DataFrame:
        """
        Load dataset with only the requested features.

        Parameters
        ----------
        features : List[str]
            Feature names defined in FEATURE_MAP.
        months : List[str], optional
            e.g. ["2023-01", "2023-02"]
        sample_ratio : float, optional
            Fraction of data to load for testing (0 < r ≤ 1.0)
        collect : bool, optional
            If True, returns a materialized DataFrame; otherwise LazyFrame.

        Returns
        -------
        pl.DataFrame or pl.LazyFrame
        """
        files = self._select_files(months)
        cols = self._required_columns(features)
        # Ensure data cleaning columns are always loaded
        cleaning_cols = ["trip_miles", "base_passenger_fare", "trip_time"]
        cols = list(set(cols + cleaning_cols))
        print(f"🔍 Loading columns: {cols}")

        # Read parquet files and normalize datetime precision to avoid schema mismatch
        lazy_frames = []
        for f in files:
            lf = pl.scan_parquet(f)
            # Force datetime columns to microseconds precision for consistency
            # This fixes schema mismatch errors when different months have different precisions (ns vs μs)
            if "pickup_datetime" in lf.columns:
                lf = lf.with_columns(
                    pl.col("pickup_datetime").cast(pl.Datetime("us"))
                )
            lazy_frames.append(lf.select(cols))
        
        df = pl.concat(lazy_frames, how="vertical")

        # Basic data cleaning
        df = (
            df.filter(pl.col("trip_miles") > 0)
              .filter(pl.col("trip_miles") < 100)
              .filter(pl.col("base_passenger_fare") > 0)
              .filter(pl.col("trip_time") > 0)
        )

        # Add derived features
        df = self._derive_features(df, features)

        # Sampling (for quick testing)
        # Use hash-based sampling on LazyFrame to avoid loading all data into memory
        if sample_ratio < 1.0:
            # Use hash of trip_miles and base_passenger_fare for deterministic sampling
            # These columns are always loaded due to data cleaning filters
            # Hash value modulo 100 gives us a way to sample
            threshold = int(sample_ratio * 100)
            # Combine hashes of multiple columns for better distribution
            df = df.filter(
                ((pl.col("trip_miles").hash(seed=42) + pl.col("base_passenger_fare").hash(seed=42)) % 100) < threshold
            )

        print(f"⚙️ Ready features: {features}")
        return df.select(features).collect() if collect else df.select(features)

    # ========== Helper function: List available features ==========
    def list_features(self):
        """Print all available features."""
        print("\n📋 Available Features:")
        for f in self.FEATURE_MAP:
            print(f" - {f}")

    # ========== Batch loading function ==========
    def load_in_batches(
        self,
        features: List[str],
        months: Optional[List[str]] = None,
        batch_size: int = 3,
        sample_ratio: float = 1.0,
        collect: bool = True
    ) -> pl.DataFrame:
        """
        Load data in batches to avoid memory issues with large datasets.
        
        Parameters
        ----------
        features : List[str]
            Feature names defined in FEATURE_MAP.
        months : List[str], optional
            e.g. ["2024-01", "2024-02", ...]. If None, loads all months.
        batch_size : int, optional
            Number of months to process in each batch (default: 3).
        sample_ratio : float, optional
            Fraction of data to load for testing (0 < r ≤ 1.0).
        collect : bool, optional
            If True, returns a materialized DataFrame; otherwise LazyFrame.
            
        Returns
        -------
        pl.DataFrame
            Combined data from all batches.
        """
        # If months is None, get all available months from file names
        if months is None:
            months = []
            for f in self.files:
                # Extract month from filename like "fhvhv_tripdata_2024-01.parquet"
                month_str = Path(f).stem.split("_")[-1]  # e.g., "2024-01"
                months.append(month_str)
            months = sorted(list(set(months)))
        
        all_dfs = []
        total_batches = (len(months) + batch_size - 1) // batch_size
        
        for i in range(0, len(months), batch_size):
            batch = months[i:i + batch_size]
            batch_num = i // batch_size + 1
            print(f"\n🧩 Processing batch {batch_num}/{total_batches}: {batch}")
            
            df_part = self.load(
                features=features,
                months=batch,
                sample_ratio=sample_ratio,
                collect=True  # Always collect for batch processing
            )
            all_dfs.append(df_part)
            print(f"   ✓ Batch {batch_num} completed: {len(df_part):,} rows")
        
        print(f"\n🔗 Merging {len(all_dfs)} batches...")
        df_final = pl.concat(all_dfs)
        print(f"✅ Total rows: {len(df_final):,}")
        
        return df_final


# ================= Example Usage =================
if __name__ == "__main__":
    loader = TLCDataLoader("data/raw")
    loader.list_features()
    df = loader.load(
        features=["pickup_hour", "pickup_zone", "avg_price_per_mile"],
        months=["2023-01", "2023-02"],
        sample_ratio=0.1
    )
    print(df.head())
