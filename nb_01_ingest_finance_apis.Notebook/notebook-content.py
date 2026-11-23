# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "922f950f-5aec-4c3c-8387-bc8532f2e77c",
# META       "default_lakehouse_name": "lh_finance_fx_macro",
# META       "default_lakehouse_workspace_id": "24fc40af-3cf5-495e-be3e-02846833798a",
# META       "known_lakehouses": [
# META         {
# META           "id": "922f950f-5aec-4c3c-8387-bc8532f2e77c"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!

days_back = 45

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os
import json
import requests
from datetime import datetime, timedelta, timezone
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

session = requests.Session()
retries = Retry(total=5, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504])
session.mount("https://", HTTPAdapter(max_retries=retries))
ingested_at = datetime.now(timezone.utc).isoformat()
run_ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

# 1. Fetch Frankfurter FX Rates
end_date = datetime.now(timezone.utc).date()
start_date = end_date - timedelta(days=days_back)
host = "api" + "." + "frankfurter.dev"
path = "v1" + "/" + f"{start_date}..{end_date}"
fx_url = f"https://{host}/{path}?base=USD&symbols=EUR,GBP,INR,JPY,CAD,AUD,CHF"
fx_data = session.get(fx_url, timeout=15).json()

fx_records = [
    {
        "rate_date": r_date,
        "base_currency": fx_data.get("base", "USD"),
        "target_currency": curr,
        "exchange_rate": float(val),
        "ingested_at": ingested_at
    }
    for r_date, rates in fx_data.get("rates", {}).items()
    for curr, val in rates.items() if val is not None
]

os.makedirs("/lakehouse/default/Files/bronze/fx", exist_ok=True)
with open(f"/lakehouse/default/Files/bronze/fx/fx_rates_{run_ts}.json", "w") as f:
    for r in fx_records:
        f.write(json.dumps(r) + "\n")

# 2. Fetch World Bank Macro GDP
wb_host = "api" + "." + "worldbank.org"
wb_path = "v2/country/USA;GBR;IND;JPN;CAN;AUS;CHE/indicator/NY.GDP.MKTP.CD"
wb_raw = session.get(wb_url, timeout=60).json()
wb_entries = wb_raw[1] if len(wb_raw) > 1 else []

wb_records = [
    {
        "country_iso3": e.get("countryiso3code"),
        "country_name": e.get("country", {}).get("value"),
        "year": int(e.get("date")),
        "gdp_usd": float(e.get("value")),
        "ingested_at": ingested_at
    }
    for e in wb_entries if e.get("value") is not None
]

os.makedirs("/lakehouse/default/Files/bronze/macro", exist_ok=True)
with open(f"/lakehouse/default/Files/bronze/macro/wb_gdp_{run_ts}.json", "w") as f:
    for r in wb_records:
        f.write(json.dumps(r) + "\n")

print(f"Landed {len(fx_records)} FX rows and {len(wb_records)} GDP rows.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
