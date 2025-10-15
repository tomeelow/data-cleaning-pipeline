import os
import sqlite3
from pathlib import Path
from datetime import datetime, timezone
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from typing import Any, Callable, Dict

load_dotenv()
DB_PATH = Path(os.getenv("DB_PATH", "data/airbnb.db"))

if not DB_PATH.exists():
    raise SystemExit(f"Database not found at {DB_PATH}. Run scripts/load_raw.py first.")

# Read raw into pandas using sqlite3 connection
with sqlite3.connect(DB_PATH) as con:
    raw = pd.read_sql_query("SELECT * FROM raw_listings", con)

before_rows = len(raw)

# 1) Normalize text
for col in ["name", "neighbourhood", "room_type"]:
    if col in raw.columns:
        raw[col] = (
            raw[col]
            .apply(lambda v: v.decode("utf-8", "replace") if isinstance(v, (bytes, bytearray)) else v)
            .astype("string")
            .str.strip()
            .replace({"": None})
        )

def _safe_text(s: pd.Series) -> pd.Series:
    # Decode bytes, keep None/NaN, stringify everything else
    return s.apply(
        lambda v: (
            None if (v is None or (isinstance(v, float) and np.isnan(v)))
            else (v.decode("utf-8", "replace") if isinstance(v, (bytes, bytearray)) else str(v))
        )
    ).astype("string")

# 2) Robust numeric parsing
def to_float(s: pd.Series) -> pd.Series:
    t = _safe_text(s)
    return (
        t.str.replace(r"[^0-9\.\-]", "", regex=True)
         .replace({"": np.nan})
         .astype(float)
    )

def to_int_clean(s: pd.Series) -> pd.Series:
    t = _safe_text(s)
    t = t.str.replace(r"[^\d\-]", "", regex=True).replace({"": np.nan})
    return pd.to_numeric(t, errors="coerce").astype("Int64")

raw["latitude"] = to_float(raw["latitude"])
raw["longitude"] = to_float(raw["longitude"])
raw["price"] = to_float(raw["price"])
raw["minimum_nights"] = to_int_clean(raw["minimum_nights"])
raw["number_of_reviews"] = to_int_clean(raw["number_of_reviews"])
raw["availability_365"] = to_int_clean(raw["availability_365"])
raw["id"] = to_int_clean(raw["id"])
raw["host_id"] = to_int_clean(raw["host_id"])
raw["reviews_per_month"] = pd.to_numeric(raw.get("reviews_per_month", np.nan), errors="coerce")

# 3) Dates 
raw["last_review"] = pd.to_datetime(raw["last_review"], errors="coerce").dt.date.astype("string")

# 4) Missing policy
mask_zero = (raw["number_of_reviews"].fillna(0) == 0) & raw["reviews_per_month"].isna()
raw.loc[mask_zero, "reviews_per_month"] = 0.0

# 5) Duplicates & required keys
dup_count = len(raw) - len(raw.drop_duplicates(subset=["id"], keep="first"))
raw = raw.drop_duplicates(subset=["id"], keep="first")
missing_id = raw["id"].isna().sum()
raw = raw[raw["id"].notna()]  # ensure primary key present

# 6) Geo sanity
geo_before = len(raw)
raw = raw[(raw["latitude"].between(-90, 90)) & (raw["longitude"].between(-180, 180))]
geo_dropped = geo_before - len(raw)

# 7) Price outliers 
if raw["price"].notna().sum() >= 10:
    p1, p99 = raw["price"].quantile([0.01, 0.99])
    raw["price"] = raw["price"].clip(lower=p1, upper=p99)

# 8) Minimum nights cap
raw.loc[raw["minimum_nights"] > 365, "minimum_nights"] = 365

after_rows = len(raw)
print(f"Rows before: {before_rows:,} | after cleaning: {after_rows:,}")
print(f" - dropped duplicates by id: {dup_count}")
print(f" - rows with missing id: {missing_id}")
print(f" - invalid lat/lon dropped: {geo_dropped}")

# Final projection
final_cols = [
    "id","name","host_id","neighbourhood","latitude","longitude",
    "room_type","price","minimum_nights","number_of_reviews",
    "last_review","reviews_per_month","availability_365"
]
final = raw[final_cols].copy()
final["loaded_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")

def to_py_int(x) -> Any:
    if x is None or (isinstance(x, float) and np.isnan(x)) or pd.isna(x):
        return None
    try:
        return int(x)
    except Exception:
        return None

def to_py_float(x) -> Any:
    if x is None or (isinstance(x, float) and np.isnan(x)) or pd.isna(x):
        return None
    try:
        return float(x)
    except Exception:
        return None

def to_py_str(x) -> Any:
    if x is None or (isinstance(x, float) and np.isnan(x)) or pd.isna(x):
        return None
    if isinstance(x, (bytes, bytearray)):
        return x.decode("utf-8", "replace")
    return str(x)

coercers: Dict[str, Callable[[Any], Any]] = {
    "id": to_py_int,
    "name": to_py_str,
    "host_id": to_py_int,
    "neighbourhood": to_py_str,
    "latitude": to_py_float,
    "longitude": to_py_float,
    "room_type": to_py_str,
    "price": to_py_float,
    "minimum_nights": to_py_int,
    "number_of_reviews": to_py_int,
    "last_review": to_py_str,          # ISO date string or None
    "reviews_per_month": to_py_float,
    "availability_365": to_py_int,
    "loaded_at": to_py_str,
}

# Apply coercion and collect simple diagnostics
null_counts = {k: 0 for k in coercers}
rows = []
for row in final[final_cols + ["loaded_at"]].itertuples(index=False, name=None):
    coerced = []
    for (col, val) in zip(final_cols + ["loaded_at"], row):
        pyval = coercers[col](val)
        if pyval is None:
            null_counts[col] += 1
        coerced.append(pyval)
    rows.append(tuple(coerced))

print("Coerced NULL counts on insert:", {k: int(v) for k, v in null_counts.items()})

# Insert into SQLite
with sqlite3.connect(DB_PATH) as con:
    con.execute("DELETE FROM core_listings;")
    if rows:
        placeholders = ",".join(["?"] * len(rows[0]))
        con.executemany(
            f"""
            INSERT INTO core_listings
            (id,name,host_id,neighbourhood,latitude,longitude,room_type,price,
             minimum_nights,number_of_reviews,last_review,reviews_per_month,
             availability_365,loaded_at)
            VALUES ({placeholders})
            """,
            rows,
        )
    # Add indexes
    con.executescript(Path("sql/01_constraints_indexes.sql").read_text())
    con.commit()

print("Loaded cleaned data into core_listings (SQLite)")
