import json
from pathlib import Path

import pandas as pd


# Path to raw ECB API response (saved during ingestion)
RAW_DATA_PATH = Path("../data/raw/ecb_fx_rates.json")

# Directory for processed outputs
PROCESSED_DIR = Path("../data/processed")

# Output paths
STAGING_OUTPUT_PATH = PROCESSED_DIR / "stg_fx_rates.csv"
MART_OUTPUT_PATH = PROCESSED_DIR / "mart_fx_summary.csv"


def load_raw_data(path: Path) -> dict:
    """Load raw JSON file"""
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def get_dimension_index(dimensions: list[dict], dim_id: str) -> int:
    """Find index of a dimension (e.g. CURRENCY) in series key"""
    for i, dim in enumerate(dimensions):
        if dim["id"] == dim_id:
            return i
    raise ValueError(f"Dimension '{dim_id}' not found.")


def build_index_map(values: list[dict]) -> dict[str, str]:
    """Map index -> actual value (e.g. '1' -> 'GBP')"""
    return {str(i): v["id"] for i, v in enumerate(values)}


def transform_to_staging(raw: dict) -> pd.DataFrame:
    """
    Flatten ECB JSON into a row-based table:
    one row per (date, currency)
    """
    ingested_at = raw["ingested_at"]
    source_data = raw["data"]

    structure = source_data["structure"]
    dataset = source_data["dataSets"][0]

    series_dims = structure["dimensions"]["series"]
    obs_dims = structure["dimensions"]["observation"]

    # Find position of currency in series key
    currency_dim_idx = get_dimension_index(series_dims, "CURRENCY")

    # Build mapping dictionaries
    currency_index_map = build_index_map(series_dims[currency_dim_idx]["values"])
    time_index_map = build_index_map(obs_dims[0]["values"])

    series_data = dataset["series"]

    records: list[dict] = []

    # Each series = one currency time series
    for series_key, series_value in series_data.items():
        key_parts = series_key.split(":")

        # Extract currency from series key
        target_currency = currency_index_map[key_parts[currency_dim_idx]]

        observations = series_value.get("observations", {})

        # Each observation = one date
        for obs_index, obs_value in observations.items():
            date = time_index_map[obs_index]

            # First value is the actual exchange rate
            exchange_rate = obs_value[0] if isinstance(obs_value, list) else obs_value

            records.append(
                {
                    "date": date,
                    "base_currency": "EUR",
                    "target_currency": target_currency,
                    "exchange_rate": exchange_rate,
                    "ingested_at": ingested_at,
                }
            )

    df = pd.DataFrame(records)

    if df.empty:
        raise ValueError("No data produced from transformation.")

    # Type conversions
    df["date"] = pd.to_datetime(df["date"])
    # Convert exchange_rate to numeric, coercing errors to NaN
    df["exchange_rate"] = pd.to_numeric(df["exchange_rate"], errors="coerce")
    df["ingested_at"] = pd.to_datetime(df["ingested_at"], errors="coerce")

    # Sort for time series calculations
    df = df.sort_values(["target_currency", "date"]).reset_index(drop=True)

    return df


def build_mart(df_stg: pd.DataFrame) -> pd.DataFrame:
    """
    Add simple time-series features
    """
    df = df_stg.copy()

    # 7-day moving average -> binding seven days' data through rolling window
    # min_periods=1 allows calculation even for the first 6 days (with fewer than 7 data points)
    df["moving_avg_7d"] = (
        df.groupby("target_currency")["exchange_rate"]
        .transform(lambda s: s.rolling(window=7, min_periods=1).mean())
    )

    # Daily percentage change
    # As the df has already been sorted through 
    # df = df.sort_values(["target_currency", "date"]).reset_index(drop=True)
    # we can directly use pct_change() to calculate the percentage change from the previous day for each currency
    df["daily_pct_change"] = (
        df.groupby("target_currency")["exchange_rate"]
        .pct_change()
    )

    return df


def save_outputs(df_stg: pd.DataFrame, df_mart: pd.DataFrame) -> None:
    """Save results to CSV"""
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    df_stg.to_csv(STAGING_OUTPUT_PATH, index=False)
    df_mart.to_csv(MART_OUTPUT_PATH, index=False)

    print(f"Saved staging table to {STAGING_OUTPUT_PATH}")
    print(f"Saved mart table to {MART_OUTPUT_PATH}")


def run_transform() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Run full pipeline"""
    raw = load_raw_data(RAW_DATA_PATH)
    df_stg = transform_to_staging(raw)
    df_mart = build_mart(df_stg)
    save_outputs(df_stg, df_mart)
    return df_stg, df_mart


if __name__ == "__main__":
    run_transform()