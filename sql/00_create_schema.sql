DROP TABLE IF EXISTS raw_listings;
CREATE TABLE raw_listings (
  id TEXT,
  name TEXT,
  host_id TEXT,
  neighbourhood TEXT,
  latitude TEXT,
  longitude TEXT,
  room_type TEXT,
  price TEXT,
  minimum_nights TEXT,
  number_of_reviews TEXT,
  last_review TEXT,
  reviews_per_month TEXT,
  availability_365 TEXT,
  _loaded_at TEXT
);

DROP TABLE IF EXISTS core_listings;
CREATE TABLE core_listings (
  id INTEGER PRIMARY KEY,
  name TEXT,
  host_id INTEGER,
  neighbourhood TEXT,
  latitude REAL,
  longitude REAL,
  room_type TEXT,
  price REAL,
  minimum_nights INTEGER,
  number_of_reviews INTEGER,
  last_review TEXT,
  reviews_per_month REAL,
  availability_365 INTEGER,
  loaded_at TEXT
);
