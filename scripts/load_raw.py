import os
import sqlite3
from pathlib import Path
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime, timezone

load_dotenv()

# DB location (SQLite file)
DB_PATH = Path(os.getenv("DB_PATH", "data/airbnb.db"))
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

CSV_PATH = Path("data/raw/listings.csv")
if not CSV_PATH.exists():
    raise SystemExit("Put Inside Airbnb listings.csv at data/raw/listings.csv")

# 1) Init DB and tables (SQLite DDL)
with sqlite3.connect(DB_PATH) as con:
    con.executescript(Path("sql/00_create_schema.sql").read_text())

# 2) Read CSV
df = pd.read_csv(
    CSV_PATH,
    low_memory=False,
    encoding="utf-8",
    encoding_errors="replace",  
)

cols = {
    "id": "id",
    "name": "name",
    "host_id": "host_id",
    "neighbourhood": "neighbourhood",
    "latitude": "latitude",
    "longitude": "longitude",
    "room_type": "room_type",
    "price": "price",
    "minimum_nights": "minimum_nights",
    "number_of_reviews": "number_of_reviews",
    "last_review": "last_review",
    "reviews_per_month": "reviews_per_month",
    "availability_365": "availability_365",
}
missing = [k for k in cols if k not in df.columns]
if missing:
    raise SystemExit(f"CSV missing expected columns: {missing}")

df = df.rename(columns=cols)[list(cols.values())].copy()
df["_loaded_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")

# 3) Insert into raw_listings
def _nan_to_none(v):
    return None if pd.isna(v) else v

records = [
    tuple(_nan_to_none(df.at[i, c]) for c in list(cols.values()) + ["_loaded_at"])
    for i in range(len(df))
]

with sqlite3.connect(DB_PATH) as con:
    con.execute("DELETE FROM raw_listings;")
    if records:
        placeholders = ",".join(["?"] * len(records[0]))
        con.executemany(
            f"""
            INSERT INTO raw_listings
            ({",".join(list(cols.values()) + ["_loaded_at"])})
            VALUES ({placeholders})
            """,
            records,
        )
    con.commit()

print(f"Loaded {len(df):,} rows into raw_listings (SQLite @ {DB_PATH})")
