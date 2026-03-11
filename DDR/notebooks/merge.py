import pandas as pd
import os

os.makedirs("ML/data", exist_ok=True)


#---------- Load All Four Source Files ----------

rides = pd.read_csv("DDR/data/chicago_rideshare_q1_2025.csv")
weather = pd.read_csv("DDR/data/raw_weather.csv")
events = pd.read_csv("DDR/data/raw_events.csv")
demographics = pd.read_csv("DDR/data/raw_demographics.csv")

print(f"Rides loaded:        {len(rides):,} rows")
print(f"Weather loaded:      {len(weather):,} rows")
print(f"Events loaded:       {len(events):,} rows")
print(f"Demographics loaded: {len(demographics):,} rows")


#---------- Clean Rides ----------

# drop rows where any core column is missing - these can't be modeled at all
rides = rides.dropna(subset=["fare", "trip_miles", "trip_seconds", "pickup_community_area"])

# zero or negative values are data entry errors, not real trips
rides = rides[(rides["fare"] > 0) & (rides["trip_miles"] > 0) & (rides["trip_seconds"] > 0)]

print(f"\nRides after basic cleaning: {len(rides):,} rows")


#---------- Extract Time Features from Rides ----------

# parse to datetime temporarily just to pull out the features we need
# the original trip_start_timestamp string column is left untouched
dt_parsed = pd.to_datetime(rides["trip_start_timestamp"])

rides["date"] = dt_parsed.dt.strftime("%Y-%m-%d")
rides["hour"] = dt_parsed.dt.hour
rides["month"] = dt_parsed.dt.month

# dayofweek is 0=Monday through 6=Sunday, so >= 5 catches Saturday and Sunday
rides["is_weekend"] = (dt_parsed.dt.dayofweek >= 5).astype(int)


#---------- Prep Weather for Join ----------

# open-meteo returns timestamps as "2025-01-01T00:00" with no seconds
# parse and split into date + hour so we can match on the same key as rides
weather["timestamp"] = pd.to_datetime(weather["timestamp"])
weather["date"] = weather["timestamp"].dt.strftime("%Y-%m-%d")
weather["hour"] = weather["timestamp"].dt.hour

# only carry the columns we actually want in the final output
weather = weather[["date", "hour", "temperature_c", "precipitation", "windspeed", "weather_label"]]


#---------- Merge Rides + Weather ----------

# left join so a missing weather row doesn't silently drop a valid trip
rides = rides.merge(weather, on=["date", "hour"], how="left")

print(f"After weather merge:      {len(rides):,} rows")


#---------- Prep Events for Join ----------

# raw events has one row per permit - collapse to a single 0/1 per calendar day
# if any event on that day was flagged major, the whole day gets a 1
daily_events = (
    events.groupby("date")["is_major_event"]
    .max()
    .reset_index()
    .rename(columns={"is_major_event": "day_has_major_event"})
)


#---------- Merge Rides + Events ----------

rides = rides.merge(daily_events, on="date", how="left")

# dates with zero permits won't appear in daily_events at all, so they come in as NaN - fill with 0
rides["day_has_major_event"] = rides["day_has_major_event"].fillna(0).astype(int)

print(f"After events merge:       {len(rides):,} rows")


#---------- Merge Rides + Demographics ----------

# both pickup_community_area and community_area come from SODA and are float64
# (values like 1.0, 2.0 ... 77.0) so they match as-is without any casting
# rides whose community area isn't in 1-77 will get NaN in the demo columns
# and will be dropped in the final dropna below
rides = rides.merge(
    demographics,
    left_on="pickup_community_area",
    right_on="community_area",
    how="left"
)

print(f"After demographics merge: {len(rides):,} rows")


#---------- Derived Features ----------

# fare_per_mile normalizes price so short and long trips are comparable
rides["fare_per_mile"] = rides["fare"] / rides["trip_miles"]

# trip_seconds is hard to reason about directly - minutes is more intuitive
rides["trip_duration_min"] = rides["trip_seconds"] / 60


#---------- Select Final Columns ----------

# lock in the column order that every downstream script will expect
keep_cols = [
    "trip_start_timestamp", "trip_miles", "trip_seconds", "fare",
    "pickup_community_area", "dropoff_community_area",
    "date", "hour", "month", "is_weekend",
    "temperature_c", "precipitation", "windspeed", "weather_label",
    "day_has_major_event",
    "community_area", "community_area_name",
    "per_capita_income", "pct_below_poverty", "hardship_index",
    "fare_per_mile", "trip_duration_min"
]

rides = rides[keep_cols]


#---------- Drop Remaining NaNs ----------

# anything still NaN at this point is a ride we couldn't fully enrich
# the main culprit is trips where pickup_community_area had no match in demographics
rows_before = len(rides)
rides = rides.dropna()

print(f"\nDropped {rows_before - len(rides):,} rows with NaN after all merges")
print(f"Final master dataset: {len(rides):,} rows, {len(rides.columns)} columns")


#---------- Save ----------

rides.to_csv("ML/data/chicago_rides_master.csv", index=False)
print("Saved to ML/data/chicago_rides_master.csv")
