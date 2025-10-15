# data-cleaning-pipeline

This project takes the Inside Airbnb listings.csv file, cleans it with clear, documented rules, and stores the cleaned result in a lightweight SQLite database that runs entirely on your machine. The aim is to demonstrate a simple, reproducible data pipeline with proper version control, a real database, and transparent cleaning steps.

## Project goal

Load the raw CSV into a staging table, apply a series of cleaning and typing steps (handling missing values, duplicates, outliers, and incorrect types), and produce an analysis-ready table you can query or export.

## Dataset source

Use the Inside Airbnb data for any city. Download the uncompressed file named “listings.csv” (not the compressed .csv.gz version) and place it in the project’s data/raw folder as data/raw/listings.csv.

## Technology choice

The pipeline uses SQLite as the database. SQLite is a real SQL database stored as a single file (data/airbnb.db), so it requires no server, credentials, or Docker. Python with pandas is used to load, clean, and write the data.

## What the pipeline does

1. Creates two tables in SQLite: a permissive staging table (raw_listings) and a typed, cleaned table (core_listings).
2. Loads the raw CSV “as is” into the staging table.
3. Cleans and transforms the data, then writes the result into the cleaned table.
4. (Optional) Exports the cleaned table to data/processed if you want a file artifact.

## Cleaning choices (documented)

• Text normalization: trims whitespace for key text columns (name, neighbourhood, room_type) and converts empty strings to nulls.
• Data types: robustly parses numbers that may include symbols or spaces (price, ids, counts), parses last_review as a date, sets correct numeric types for latitude, longitude, and counters.
• Missing data: sets reviews_per_month to 0 only when number_of_reviews equals 0; otherwise leaves it null. Keeps last_review null when it cannot be parsed. Drops rows with a missing id (id is the listing identifier).
• Duplicates: removes duplicate listings by id, keeping the first occurrence.
• Validity checks: keeps only rows with latitude in [-90, 90] and longitude in [-180, 180].
• Outliers: winsorizes price at the 1st and 99th percentiles to reduce the impact of extreme values, and caps minimum_nights at 365.
• Indexes: adds indexes on neighbourhood and price for quicker queries of the cleaned table.

## How to set up and run locally

1. Clone the repository to your computer and open it in your editor or terminal.
2. Create and activate a Python virtual environment, then install the dependencies from requirements.txt.
3. Download the Inside Airbnb listings.csv for your chosen city and save it as data/raw/listings.csv.
4. Run the loader script to create the SQLite database and staging table, then load the raw CSV.
5. Run the cleaning script to transform data from raw_listings into core_listings.
6. Optionally export the cleaned table to the data/processed folder if you want a CSV or Parquet file for inspection.

After running the two scripts, the SQLite database file (data/airbnb.db) will contain both the raw and cleaned tables. You can inspect the cleaned data by opening the database with any SQLite viewer or using short Python snippets to query row counts and sample rows.
