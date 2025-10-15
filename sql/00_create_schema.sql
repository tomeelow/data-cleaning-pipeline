CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS core;

DROP TABLE IF EXISTS raw.listings;
CREATE TABLE raw.listings (
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
  _loaded_at TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS core.listings (
  id BIGINT PRIMARY KEY,
  name TEXT,
  host_id BIGINT,
  neighbourhood TEXT,
  latitude DOUBLE PRECISION,
  longitude DOUBLE PRECISION,
  room_type TEXT,
  price NUMERIC(10,2),
  minimum_nights INT,
  number_of_reviews INT,
  last_review DATE,
  reviews_per_month NUMERIC(6,2),
  availability_365 INT,
  loaded_at TIMESTAMP DEFAULT now()
);
