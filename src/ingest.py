import requests
import json
from datetime import datetime
from pathlib import Path

ECB_API_URL = (
    "https://data-api.ecb.europa.eu/service/data/EXR/"
    "D.USD+GBP+KRW+DKK+SEK+NOK+JPY.EUR.SP00.A" 
    # Retrieves exchange rate data for USD, GBP, KRW, DKK, SEK, NOK, and JPY against EUR on a daily basis
    # D : Daily, USD+GBP+KRW+DKK+SEK+NOK+JPY : Currencies, EUR : Base currency, SP00 : Spot rate, A : Average
    "?startPeriod=2024-01-01&format=jsondata"
)

RAW_DATA_PATH = Path("../data/raw/ecb_fx_rates.json")

def fetch_ecb_data():
    # Fetch FX data from ECB API
    response = requests.get(ECB_API_URL, timeout=30)
    response.raise_for_status()
    return response.json()


def save_raw_data(data: dict):
    
    # Adding ingestion timestamp
    wrapped_data = {
        "ingested_at": datetime.utcnow().isoformat(),
        "source": "ECB",
        "data": data
    }

    RAW_DATA_PATH.parent.mkdir(parents=True, exist_ok=True)

    # JSON format
    with open(RAW_DATA_PATH, "w", encoding="utf-8") as f:
        json.dump(wrapped_data, f, indent=2)

    print(f"Raw data saved to {RAW_DATA_PATH}")


def run_ingestion():
    data = fetch_ecb_data()
    save_raw_data(data)
    
if __name__ == "__main__":
    run_ingestion()