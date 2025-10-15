import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

csv_path = Path("data/raw/listings.csv")
if not csv_path.exists():
    raise SystemExit("Put Inside Airbnb listings.csv at data/raw/listings.csv")

pgurl = os.getenv("PGURL")
engine = create_engine(pgurl)

# ensure schemas/tables exist
with engine.begin() as conn, open("sql/00_create_schema.sql") as f:
    conn.execute(f.read())

# load as text to staging
df = pd.read_csv(csv_path, low_memory=False)
# keep only common columns
rename = {
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
df = df.rename(columns=rename)[list(rename.values())].copy()

with engine.begin() as conn:
    conn.execute("TRUNCATE raw.listings;")
df.to_sql("listings", engine, schema="raw", if_exists="append", index=False)

print(f"Loaded {len(df):,} rows into raw.listings")
