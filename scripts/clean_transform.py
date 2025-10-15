import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()
engine = create_engine(os.getenv("PGURL"))

# read staging
raw = pd.read_sql("SELECT * FROM raw.listings", engine)

before_rows = len(raw)

# Cleaning steps

# 1) trim whitespace on text columns
for col in ["name", "neighbourhood", "room_type"]:
    if col in raw.columns:
        raw[col] = raw[col].astype(str).str.strip().replace({"nan": None})

# 2) cast numeric columns, set invalid parses to NaN
def to_float(s): 

    return (
        s.astype(str)
         .str.replace(r"[^0-9\.\-]", "", regex=True)
         .replace({"": np.nan})
         .astype(float)
    )

def to_int(s):
    out = pd.to_numeric(s, errors="coerce")
    return out.astype("Int64")

raw["latitude"] = to_float(raw["latitude"])
raw["longitude"] = to_float(raw["longitude"])
raw["price"] = to_float(raw["price"])
raw["minimum_nights"] = to_int(raw["minimum_nights"])
raw["number_of_reviews"] = to_int(raw["number_of_reviews"])
raw["reviews_per_month"] = pd.to_numeric(raw["reviews_per_month"], errors="coerce")
raw["availability_365"] = to_int(raw["availability_365"])
raw["id"] = pd.to_numeric(raw["id"], errors="coerce").astype("Int64")
raw["host_id"] = pd.to_numeric(raw["host_id"], errors="coerce").astype("Int64")

# 3) dates
raw["last_review"] = pd.to_datetime(raw["last_review"], errors="coerce").dt.date

# 4) missing values
# reviews_per_month: fill 0 when number_of_reviews == 0, else keep NaN
mask_zero = (raw["number_of_reviews"].fillna(0) == 0) & raw["reviews_per_month"].isna()
raw.loc[mask_zero, "reviews_per_month"] = 0.0

# 5) duplicates
raw = raw.sort_index()
raw = raw.drop_duplicates(subset=["id"], keep="first")

# 6) geographic sanity
raw = raw[(raw["latitude"].between(-90, 90)) & (raw["longitude"].between(-180, 180))]

# 7) outliers for price
p1, p99 = raw["price"].quantile([0.01, 0.99])
raw["price"] = raw["price"].clip(lower=p1, upper=p99)

# 8) minimum nights cap
raw.loc[raw["minimum_nights"] > 365, "minimum_nights"] = 365

after_rows = len(raw)
print(f"Rows before: {before_rows:,} | after cleaning: {after_rows:,}")

# prepare final dtypes
final = raw[[
    "id","name","host_id","neighbourhood","latitude","longitude",
    "room_type","price","minimum_nights","number_of_reviews",
    "last_review","reviews_per_month","availability_365"
]].copy()

# load to core
with engine.begin() as conn:
    conn.execute("TRUNCATE core.listings;")
final.to_sql("listings", engine, schema="core", if_exists="append", index=False)

# indexes
with engine.begin() as conn, open("sql/01_constraints_indexes.sql") as f:
    conn.execute(f.read())

print("Loaded cleaned data into core.listings")
