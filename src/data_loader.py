"""
NYC TLC High Volume FHV DataLoader (Production Ready)
------------------------------------------------------
Unified Polars DataLoader for 250M+ rows
Modular, memory-efficient, and team-friendly
Supports feature selection and month-based loading
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
        # Cache raw column names (lazy load on first use)
        self._raw_columns = None
        print(f"TLCDataLoader initialized with {len(self.files)} monthly files.")
    
    # ========== Helper: Get raw column names ==========
    def _get_raw_columns(self) -> List[str]:
        """Get all column names from the raw parquet files."""
        if self._raw_columns is None:
            # Read schema from first file
            lf = pl.scan_parquet(self.files[0])
            self._raw_columns = lf.collect_schema().names()
        return self._raw_columns

    # ========== Helper: Select files to read ==========
    def _select_files(self, months: Optional[List[str]] = None) -> List[str]:
        if not months:
            return self.files
        selected = [f for f in self.files if any(m in f for m in months)]
        print(f"Selected {len(selected)} files: {[Path(f).name for f in selected]}")
        return selected

    # ========== Helper: Validate and return requested columns ==========
    def _required_columns(self, features: List[str]) -> List[str]:
        """
        Validate that requested features exist as raw columns.
        Returns the list of columns to load (no preprocessing/derivation).
        """
        raw_cols = self._get_raw_columns()
        invalid_cols = []
        
        for f in features:
            if f not in raw_cols:
                invalid_cols.append(f)
        
        if invalid_cols:
            raise ValueError(
                f"Column(s) not found: {invalid_cols}\n"
                f"Available columns: {raw_cols[:20]}..." if len(raw_cols) > 20 else f"   Available columns: {raw_cols}"
            )
        
        return features


    # ========== Main function ==========
    def load(
        self,
        features: List[str],
        months: Optional[List[str]] = None,
        sample_ratio: float = 1.0,
        collect: bool = True
    ) -> pl.DataFrame:
        """
        Load dataset with only the requested raw columns.

        Parameters
        ----------
        features : List[str]
            Raw column names from the parquet files (e.g., "PULocationID", "trip_miles", "pickup_datetime").
            Users should compute derived features themselves after loading.
        months : List[str], optional
            e.g. ["2024-01", "2024-02"]
        sample_ratio : float, optional
            Fraction of data to load for testing (0 < r ≤ 1.0)
        collect : bool, optional
            If True, returns a materialized DataFrame; otherwise LazyFrame.

        Returns
        -------
        pl.DataFrame or pl.LazyFrame
            DataFrame with only the requested columns. No preprocessing or feature derivation.
        """
        files = self._select_files(months)
        # Only load columns needed for the requested features
        cols = self._required_columns(features)
        print(f"Loading columns: {cols}")

        # Read parquet files and normalize datetime precision to avoid schema mismatch
        # Only normalize datetime columns that the user actually requested
        lazy_frames = []
        for f in files:
            lf = pl.scan_parquet(f)
            # Get schema to check column types
            schema = lf.collect_schema()
            
            # Only normalize datetime columns that are in the requested columns
            datetime_cols_to_normalize = []
            for col_name in cols:
                if col_name in schema:
                    dtype = schema[col_name]
                    # Check if it's a datetime type (could be Datetime("ns"), Datetime("us"), etc.)
                    if isinstance(dtype, pl.Datetime):
                        datetime_cols_to_normalize.append(col_name)
            
            # Normalize datetime precision to microseconds for consistency
            if datetime_cols_to_normalize:
                cast_exprs = [
                    pl.col(col).cast(pl.Datetime("us"))
                    for col in datetime_cols_to_normalize
                ]
                lf = lf.with_columns(cast_exprs)
            
            lazy_frames.append(lf.select(cols))
        
        df = pl.concat(lazy_frames, how="vertical")

        # No automatic data cleaning or feature derivation
        # Users should handle data cleaning and feature engineering themselves

        # Sampling (for quick testing)
        # Use hash-based sampling on LazyFrame to avoid loading all data into memory
        if sample_ratio < 1.0:
            threshold = int(sample_ratio * 100)
            # Use available columns for sampling (prefer numeric columns)
            # Try to use columns that are likely to be loaded
            sampling_cols = []
            for col in ["trip_miles", "base_passenger_fare", "PULocationID", "pickup_datetime"]:
                if col in cols:
                    sampling_cols.append(col)
            
            if len(sampling_cols) >= 2:
                # Use multiple columns for better distribution
                hash_expr = pl.col(sampling_cols[0]).hash(seed=42) + pl.col(sampling_cols[1]).hash(seed=42)
                df = df.filter((hash_expr % 100) < threshold)
            elif len(sampling_cols) == 1:
                # Use single column
                df = df.filter((pl.col(sampling_cols[0]).hash(seed=42) % 100) < threshold)
            else:
                # Fallback: use first available column
                if len(cols) > 0:
                    df = df.filter((pl.col(cols[0]).hash(seed=42) % 100) < threshold)

        print(f"Ready columns: {features}")
        # Return only requested columns (no derivation)
        return df.select(features).collect() if collect else df.select(features)

    # ========== Helper function: List available columns ==========
    def list_features(self):
        """Print all available raw columns from the parquet files."""
        raw_cols = self._get_raw_columns()
        print(f"\nAvailable Columns ({len(raw_cols)} total):")
        for col in raw_cols:
            print(f" - {col}")

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
            print(f"\nProcessing batch {batch_num}/{total_batches}: {batch}")
            
            df_part = self.load(
                features=features,
                months=batch,
                sample_ratio=sample_ratio,
                collect=True  # Always collect for batch processing
            )
            all_dfs.append(df_part)
            print(f"Batch {batch_num} completed: {len(df_part):,} rows")
        
        print(f"\nMerging {len(all_dfs)} batches...")
        df_final = pl.concat(all_dfs)
        print(f"Total rows: {len(df_final):,}")
        
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
