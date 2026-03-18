import pandas as pd
import os
import sqlite3

os.makedirs("ML/data", exist_ok=True)
# Load all source files
rides = pd.read_csv("DDR/data/chicago_rideshare_q4_2025.csv")
weather = pd.read_csv("DDR/data/raw_weather.csv")
events = pd.read_csv("DDR/data/raw_events.csv")
demographics = pd.read_csv("DDR/data/raw_demographics.csv")

print(f"Rides loaded: {len(rides):,} rows")
print(f"Weather loaded: {len(weather):,} rows")
print(f"Events loaded: {len(events):,} rows")
print(f"Demographics loaded: {len(demographics):,} rows")

#This section cleans the rides data
#First we drop rows where core fields are missing
rides = rides.dropna(
    subset=["fare", "trip_miles", "trip_seconds", "pickup_community_area"]
)

#Then we keep only rows with positive values cause negative values are clear data errors
rides = rides[rides["fare"] > 0]
rides = rides[rides["trip_miles"] > 0]
rides = rides[rides["trip_seconds"] > 0]

print(f"\nRides after basic cleaning: {len(rides):,} rows")
dt_parsed = pd.to_datetime(rides["trip_start_timestamp"])

rides["date"] = dt_parsed.dt.strftime("%Y-%m-%d")
rides["hour"] = dt_parsed.dt.hour
rides["month"] = dt_parsed.dt.month

#dayofweek is 0 to 6, so 5 and 6 are weekend
rides["is_weekend"] = (dt_parsed.dt.dayofweek >= 5).astype(int)

#This section prepares weather keys to match rides
weather["timestamp"] = pd.to_datetime(weather["timestamp"])
weather["date"] = weather["timestamp"].dt.strftime("%Y-%m-%d")
weather["hour"] = weather["timestamp"].dt.hour

#Keep only columns needed in the merged dataset
weather = weather[
    ["date", "hour", "temperature_c", "precipitation", "windspeed", "weather_label"]
]

#Merge rides with weather using date and hour
rides = rides.merge(weather, on=["date", "hour"], how="left")

print(f"After weather merge: {len(rides):,} rows")

#This creates one daily event flag from the raw events table.
daily_events = events.groupby("date", as_index=False)["is_major_event"].max()
daily_events.rename(columns={"is_major_event": "day_has_major_event"}, inplace=True)
rides = rides.merge(daily_events, on="date", how="left")
print(f"After events merge: {len(rides):,} rows")

#Merge rides with demographics using pickup_community_area.
rides = rides.merge(
    demographics, left_on="pickup_community_area", right_on="community_area", how="left"
)
print(f"After demographics merge: {len(rides):,} rows")

rides["fare_per_mile"] = rides["fare"] / rides["trip_miles"]
rides["trip_duration_min"] = rides["trip_seconds"] / 60

keep_cols = [
    "trip_start_timestamp",
    "trip_miles",
    "trip_seconds",
    "fare",
    "pickup_community_area",
    "dropoff_community_area",
    "date",
    "hour",
    "month",
    "is_weekend",
    "temperature_c",
    "precipitation",
    "windspeed",
    "weather_label",
    "day_has_major_event",
    "community_area",
    "community_area_name",
    "per_capita_income",
    "pct_below_poverty",
    "hardship_index",
    "fare_per_mile",
    "trip_duration_min",
]

rides = rides[keep_cols]

#Drop any rows with NaN in the final dataset
rides = rides.dropna()

print(f"\nDropped {rows_before - len(rides):,} rows with NaN after all merges")
print(f"Final master dataset: {len(rides):,} rows, {len(rides.columns)} columns")

rides.to_csv("ML/data/chicago_rides_master.csv", index=False)
print("Saved to ML/data/chicago_rides_master.csv")

#We also save to SQLite for easier querying
conn = sqlite3.connect("DDR/data/rideshare.db")

#Reload raw files so the DB stores source tables and master table.
raw_rides = pd.read_csv("DDR/data/chicago_rideshare_q4_2025.csv")
raw_weather = pd.read_csv("DDR/data/raw_weather.csv")
raw_events = pd.read_csv("DDR/data/raw_events.csv")
raw_demographics = pd.read_csv("DDR/data/raw_demographics.csv")

raw_rides.to_sql("rides", conn, if_exists="replace", index=False)
raw_weather.to_sql("weather", conn, if_exists="replace", index=False)
raw_events.to_sql("events", conn, if_exists="replace", index=False)
raw_demographics.to_sql("demographics", conn, if_exists="replace", index=False)
rides.to_sql("master", conn, if_exists="replace", index=False)

#Query 1: average fare by income tier.
validation_query = """
    SELECT
        CASE
            WHEN per_capita_income < 15000 THEN 'Low'
            WHEN per_capita_income < 30000 THEN 'Middle'
            ELSE 'High'
        END AS income_tier,
        ROUND(AVG(fare), 2) AS avg_fare,
        ROUND(AVG(fare_per_mile), 2) AS avg_fare_per_mile,
        COUNT(*) AS num_trips
    FROM master
    GROUP BY income_tier
    ORDER BY avg_fare DESC
"""

validation_df = pd.read_sql_query(validation_query, conn)
print("\nAvg fare by neighborhood income tier:")
print(validation_df.to_string(index=False))

#Query 2: average fare by event day type.
event_query = """
    SELECT
        CASE
            WHEN day_has_major_event = 1 THEN 'Major event day'
            ELSE 'Normal day'
        END AS day_type,
        ROUND(AVG(fare_per_mile), 2) AS avg_fare_per_mile,
        ROUND(AVG(fare), 2) AS avg_fare,
        COUNT(*) AS num_trips
    FROM master
    GROUP BY day_has_major_event
    ORDER BY avg_fare_per_mile DESC
"""

event_df = pd.read_sql_query(event_query, conn)
print("\nAvg fare by event day type:")
print(event_df.to_string(index=False))

#Query 3: average fare by weather condition.
weather_query = """
    SELECT
        weather_label,
        ROUND(AVG(fare_per_mile), 2) AS avg_fare_per_mile,
        ROUND(AVG(fare), 2) AS avg_fare,
        COUNT(*) AS num_trips
    FROM master
    GROUP BY weather_label
    ORDER BY num_trips DESC
"""

weather_df = pd.read_sql_query(weather_query, conn)
print("\nAvg fare by weather condition:")
print(weather_df.to_string(index=False))

conn.close()
print("\nSaved to DDR/data/rideshare.db")