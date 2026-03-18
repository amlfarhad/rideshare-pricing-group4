import requests
import pandas as pd
import time
import os
import random
from datetime import datetime, timedelta


# Chicago Rideshare Data

url = "https://data.cityofchicago.org/resource/6dvr-xwnh.json"

# The date range is split into 10-day windows.
# Inside each window I pick a random day for each hour and then pull data
# from an explicit one-hour time range.
# Total target rows are roughly 225K across all windows.

windows = [
    # October
    ("2025-10-01", "2025-10-10"),
    ("2025-10-11", "2025-10-20"),
    ("2025-10-21", "2025-10-31"),
    # November
    ("2025-11-01", "2025-11-10"),
    ("2025-11-11", "2025-11-20"),
    ("2025-11-21", "2025-11-30"),
    # December
    ("2025-12-01", "2025-12-10"),
    ("2025-12-11", "2025-12-20"),
    ("2025-12-21", "2025-12-31"),
]

rows_per_hour = 25000 // 24  #around 1,042 rows per hour per window
random.seed(42)
all_rides = []

for win_start_str, win_end_str in windows:

    # Build list of all days in the current window
    available_days = []
    current_day = datetime.strptime(win_start_str, "%Y-%m-%d")
    end_day = datetime.strptime(win_end_str, "%Y-%m-%d")

    while current_day <= end_day:
        available_days.append(current_day)
        current_day = current_day + timedelta(days=1) #timedelta is used to add one day to the current_day variable
                                                      #in each iteration of the loop

    window_rows = []
    #This loop allows us to pick data from every hour of the day across the entire window
    for hour in range(24):
        #Pick random day from this window for this hour
        random_day = random.choice(available_days)

        #Build one-hour timestamp range
        ts_start = random_day.strftime(f"%Y-%m-%dT{hour:02d}:00:00")

        if hour < 23:
            ts_end = random_day.strftime(f"%Y-%m-%dT{hour + 1:02d}:00:00")
        else:
            ts_end = (random_day + timedelta(days=1)).strftime("%Y-%m-%dT00:00:00")

        hour_filter = (
            f"trip_start_timestamp >= '{ts_start}' "
            f"AND trip_start_timestamp < '{ts_end}'"
        )

        params = {
            "$select": "trip_start_timestamp,trip_miles,trip_seconds,fare,pickup_community_area,dropoff_community_area",
            "$where": hour_filter,
            "$limit": rows_per_hour,
        }

        response = requests.get(url, params=params)

        data = response.json()
        window_rows.extend(data)
        print(
            f"  {win_start_str} hour {hour:02d} ({random_day.strftime('%Y-%m-%d')}): {len(data):,} rows"
        )

        time.sleep(0.2)

    print(f"Window {win_start_str}: {len(window_rows):,} rows across all 24 hours")
    all_rides.extend(window_rows)
    print(f"Total across all windows so far: {len(all_rides):,}\n")

df = pd.DataFrame(all_rides)
print(f"\nTotal rows fetched: {len(df):,}")

#This loop extracts the hour from the trip_start_timestamp
# and counts how many trips started in each hour across the entire dataset.
if len(df) > 0:
    df["_hour"] = pd.to_datetime(df["trip_start_timestamp"]).dt.hour
    print("\nHourly distribution:")
    print(df["_hour"].value_counts().sort_index().to_string())
    df.drop(columns=["_hour"], inplace=True)

df.to_csv("DDR/data/chicago_rideshare_q4_2025.csv", index=False)
print("Saved to DDR/data/chicago_rideshare_q4_2025.csv")


# Weather Data

weather_url = "https://archive-api.open-meteo.com/v1/archive"

weather_params = {
    "latitude": 41.8781,
    "longitude": -87.6298,
    "start_date": "2025-10-01",
    "end_date": "2025-12-31",
    "hourly": ["temperature_2m", "precipitation", "windspeed_10m", "weathercode"],
    "timezone": "America/Chicago",
}

weather_response = requests.get(weather_url, params=weather_params)
weather_json = weather_response.json()

hourly = weather_json["hourly"]
weather_df = pd.DataFrame(
    {
        "timestamp": hourly["time"],
        "temperature_c": hourly["temperature_2m"],
        "precipitation": hourly["precipitation"],
        "windspeed": hourly["windspeed_10m"],
        "weather_code": hourly["weathercode"],
    }
)


def weather_label(code): #This is to convert the numeric weather codes into human-readable labels
    if code == 0:
        return "Clear sky"
    elif code in (1, 2, 3):
        return "Partly cloudy"
    elif code in (45, 48):
        return "Fog"
    elif code in (51, 53, 55):
        return "Drizzle"
    elif code in (56, 57):
        return "Freezing drizzle"
    elif code in (61, 63, 65):
        return "Rain"
    elif code in (66, 67):
        return "Freezing rain"
    elif code in (71, 73, 75):
        return "Snow"
    elif code == 77:
        return "Snow grains"
    elif code in (80, 81, 82):
        return "Rain showers"
    elif code in (85, 86):
        return "Snow showers"
    elif code in (95,):
        return "Thunderstorm"
    elif code in (96, 99):
        return "Thunderstorm with hail"
    else:
        return "Unknown"


weather_df["weather_label"] = weather_df["weather_code"].apply(weather_label)

print(weather_df.head())
print(f"\nTotal hourly rows: {len(weather_df):,}")

weather_df.to_csv("DDR/data/raw_weather.csv", index=False)
print("Saved to DDR/data/raw_weather.csv")


# Events Data (Two Sources)

# Source 1: Chicago Special Events Permits (SODA API)
# Days with 3 or more permits are flagged as major

permits_url = "https://data.cityofchicago.org/resource/xgse-8eg7.json"

#We are just pulling the date, event details, and venue for all permits in the 3-month window (Q4 2025).
# The major event flag is derived later based on the count of permits per day.
permits_params = {
    "$where": "date >= '2025-10-01T00:00:00' AND date < '2026-01-01T00:00:00'",
    "$limit": 5000,
}

permits_response = requests.get(permits_url, params=permits_params)
permits_df = pd.DataFrame(permits_response.json())

print(f"Special Events Permits fetched: {len(permits_df):,}")

# Extract date as YYYY-MM-DD
permits_df["date"] = pd.to_datetime(permits_df["date"]).dt.strftime("%Y-%m-%d")

# Flag major permit days
permits_per_day = permits_df["date"].value_counts()
permit_major_days = list(permits_per_day[permits_per_day >= 3].index)

permits_df["is_major_event"] = permits_df["date"].isin(permit_major_days).astype(int)

# Keep common columns for combined events output
permits_df["source"] = "permits"
permits_df["event_name"] = permits_df["event_details"]
permits_df["venue_name"] = permits_df["venue"]


# Source 2 - Ticketmaster Discovery API
# Flag major if venue capacity is above 5000

tm_api_key = os.environ.get("TICKETMASTER_API_KEY", "")

tm_url = "https://app.ticketmaster.com/discovery/v2/events.json"

all_tm_events = []

page = 0
while True:
    tm_params = {
        "apikey": tm_api_key,
        "city": "Chicago",
        "startDateTime": "2025-10-01T00:00:00Z",
        "endDateTime": "2025-12-31T23:59:59Z",
        "size": 200,
        "page": page,
    }

    tm_response = requests.get(tm_url, params=tm_params)
    tm_json = tm_response.json()

    # Ticketmaster keeps their events under _embedded.events
    events_list = tm_json["_embedded"]["events"]
    if len(events_list) == 0:
        break

    for ev in events_list:
        event_date = ev["dates"]["start"]["localDate"]
        event_name = ev["name"]
        
        venues = ev["_embedded"]["venues"]
        venue_name = venues[0]["name"]
        venue_capacity = int(venues[0]["capacity"])

        all_tm_events.append(
            {
                "date": event_date,
                "event_name": event_name,
                "venue_name": venue_name,
                "venue_capacity": venue_capacity,
            }
        )

    #Check pagination and stop when all pages are done
    total_pages = tm_json["page"]["totalPages"]
    page += 1
    print(
        f"Ticketmaster page {page}/{total_pages}: {len(all_tm_events):,} events so far"
    )

    if page >= total_pages:
        break
    time.sleep(0.2)

tm_df = pd.DataFrame(all_tm_events)
print(f"Ticketmaster events fetched: {len(tm_df):,}")

#Flag major event day if any event has venue capacity above 5000
if len(tm_df) > 0:
    tm_df["is_major_event"] = (tm_df["venue_capacity"] > 5000).astype(int)
else:
    tm_df = pd.DataFrame(
        columns=["date", "event_name", "venue_name", "venue_capacity", "is_major_event"]
    )
tm_df["source"] = "ticketmaster"


#Combine both sources into one dataframe
permits_combined = permits_df[
    ["date", "event_name", "venue_name", "source", "is_major_event"]
].copy()
tm_combined = tm_df[
    ["date", "event_name", "venue_name", "source", "is_major_event"]
].copy()

events_df = pd.concat([permits_combined, tm_combined], ignore_index=True)

# Build one day-level flag using max across both sources
daily_major = events_df.groupby("date", as_index=False)["is_major_event"].max()
daily_major.rename(columns={"is_major_event": "day_has_major_event"}, inplace=True)

# Attach daily major flag back to event rows
events_df = events_df.merge(daily_major, on="date", how="left")
print(events_df.head())
print(f"\nTotal combined events: {len(events_df):,}")
print(f"Major events flagged: {events_df['is_major_event'].sum():,}")
print(f"Days with major event: {daily_major['day_has_major_event'].sum():,}")

events_df.to_csv("DDR/data/raw_events.csv", index=False)
print("Saved to DDR/data/raw_events.csv")


# Demographics Data
demo_url = "https://data.cityofchicago.org/resource/kn9c-c2s2.json"

demo_params = {
    "$select": "ca,community_area_name,per_capita_income_,percent_households_below_poverty,hardship_index",
    "$limit": 100,
}

demo_response = requests.get(demo_url, params=demo_params)
demographics_df = pd.DataFrame(demo_response.json())

demographics_df.rename(
    columns={
        "ca": "community_area",
        "per_capita_income_": "per_capita_income",
        "percent_households_below_poverty": "pct_below_poverty",
        "hardship_index": "hardship_index",
    },
    inplace=True,
)

demographics_df["community_area"] = pd.to_numeric(
    demographics_df["community_area"], errors="coerce"
)
demographics_df["per_capita_income"] = pd.to_numeric(
    demographics_df["per_capita_income"], errors="coerce"
)
demographics_df["pct_below_poverty"] = pd.to_numeric(
    demographics_df["pct_below_poverty"], errors="coerce"
)
demographics_df["hardship_index"] = pd.to_numeric(
    demographics_df["hardship_index"], errors="coerce"
)

demographics_df.sort_values("community_area", inplace=True)
demographics_df.reset_index(drop=True, inplace=True)

print(demographics_df.head(10))
print(
    f"\nCommunity areas with income data: {demographics_df['per_capita_income'].notna().sum()} / 77"
)

demographics_df.to_csv("DDR/data/raw_demographics.csv", index=False)
print("Saved to DDR/data/raw_demographics.csv")
