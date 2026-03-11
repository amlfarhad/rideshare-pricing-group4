import requests
import pandas as pd
import time


#---------- Section 1: Chicago Rideshare Data ----------

url = "https://data.cityofchicago.org/resource/6dvr-xwnh.json"

#Pulling 75K rows from each month so we get representative coverage across Q1
#rather than just a dense slice from the first few days of January
months = [
    ("2025-01-01T00:00:00", "2025-02-01T00:00:00"),
    ("2025-02-01T00:00:00", "2025-03-01T00:00:00"),
    ("2025-03-01T00:00:00", "2025-04-01T00:00:00"),
]

all_rides = []

for start, end in months:
    offset = 0
    month_rows = []

    while len(month_rows) < 75000:
        params = {
            "$select": "trip_start_timestamp,trip_miles,trip_seconds,fare,pickup_community_area,dropoff_community_area",
            "$where": f"trip_start_timestamp >= '{start}' AND trip_start_timestamp < '{end}'",
            "$order": "trip_start_timestamp ASC",
            "$limit": 50000,
            "$offset": offset
        }

        success = False
        for attempt in range(1, 6):
            try:
                response = requests.get(url, params=params, timeout=120)
                if response.status_code == 200:
                    success = True
                    break
                print(f"HTTP {response.status_code} on attempt {attempt}/5. Retrying in 10s")
            except requests.exceptions.ReadTimeout:
                print(f"Timeout on attempt {attempt}/5. Retrying in 10s")
            time.sleep(10)

        if not success:
            print(f"Giving up at offset {offset} for month starting {start}")
            break

        data = response.json()
        if not data:
            break

        month_rows.extend(data)
        offset += 50000
        print(f"Month {start[:7]}: {len(month_rows):,} rows so far")

        if len(month_rows) >= 75000:
            month_rows = month_rows[:75000]
            print(f"Hit 75K cap for {start[:7]}, moving to next month")
            break

    all_rides.extend(month_rows)
    print(f"Total across all months so far: {len(all_rides):,}")

df = pd.DataFrame(all_rides)
print(f"\nTotal rows fetched: {len(df):,}")

df.to_csv("DDR/data/chicago_rideshare_q1_2025.csv", index=False)
print("Saved to DDR/data/chicago_rideshare_q1_2025.csv")


#---------- Section 2: Weather Data ----------

weather_url = "https://archive-api.open-meteo.com/v1/archive"

weather_params = {
    "latitude": 41.8781,
    "longitude": -87.6298,
    "start_date": "2025-01-01",
    "end_date": "2025-03-31",
    "hourly": ["temperature_2m", "precipitation", "windspeed_10m", "weathercode"],
    "timezone": "America/Chicago",
}

weather_response = requests.get(weather_url, params=weather_params, timeout=60)
weather_response.raise_for_status()
weather_json = weather_response.json()

hourly = weather_json["hourly"]
weather_df = pd.DataFrame({
    "timestamp": hourly["time"],
    "temperature_c": hourly["temperature_2m"],
    "precipitation": hourly["precipitation"],
    "windspeed": hourly["windspeed_10m"],
    "weather_code": hourly["weathercode"],
})

def weather_label(code):
    #Map WMO weather interpretation codes to readable strings
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


#---------- Section 3: Events Data ----------

#choosechicago.com is JavaScript-rendered so BeautifulSoup can't parse it
#Using the Chicago Park District Event Permits from the data portal instead
events_url = "https://data.cityofchicago.org/resource/pk66-w54g.json"

events_params = {
    "$select": "event_description,reservation_start_date,park_facility_name,event_type",
    "$where": "reservation_start_date >= '2025-01-01T00:00:00' AND reservation_start_date < '2025-04-01T00:00:00'",
    "$limit": 5000
}

events_response = requests.get(events_url, params=events_params, timeout=60)
events_response.raise_for_status()
events_df = pd.DataFrame(events_response.json())

events_df["date"] = pd.to_datetime(events_df["reservation_start_date"]).dt.strftime("%Y-%m-%d")

major_venues = {
    "united center", "wrigley field", "guaranteed rate field",
    "soldier field", "allstate arena", "credit union 1 arena",
    "sears centre", "now arena", "chicago theatre", "grant park",
    "millennium park", "navy pier"
}

#Flag days with 3+ permitted events or events at major venues
events_per_day = events_df["date"].value_counts()
high_volume_days = set(events_per_day[events_per_day >= 15].index)

def is_major_venue(venue):
    if pd.isna(venue):
        return False
    v = venue.lower()
    return any(mv in v for mv in major_venues)

events_df["is_major_event"] = (
    events_df["date"].isin(high_volume_days) | events_df["park_facility_name"].apply(is_major_venue)
).astype(int)

print(events_df.head())
print(f"\nTotal events: {len(events_df):,}")
print(f"Major events flagged: {events_df['is_major_event'].sum():,}")

events_df.to_csv("DDR/data/raw_events.csv", index=False)
print("Saved to DDR/data/raw_events.csv")


#---------- Section 4: Demographics Data ----------

#Using the Chicago Socioeconomic Indicators dataset which already has income data
#at the community area level - no Census API key or spatial join needed
demo_url = "https://data.cityofchicago.org/resource/kn9c-c2s2.json"

demo_params = {
    "$select": "ca,community_area_name,per_capita_income_,percent_households_below_poverty,hardship_index",
    "$limit": 100
}

demo_response = requests.get(demo_url, params=demo_params, timeout=60)
demo_response.raise_for_status()
demographics_df = pd.DataFrame(demo_response.json())

demographics_df.rename(columns={
    "ca": "community_area",
    "per_capita_income_": "per_capita_income",
    "percent_households_below_poverty": "pct_below_poverty",
    "hardship_index": "hardship_index"
}, inplace=True)

demographics_df["community_area"] = pd.to_numeric(demographics_df["community_area"], errors="coerce")
demographics_df["per_capita_income"] = pd.to_numeric(demographics_df["per_capita_income"], errors="coerce")
demographics_df["pct_below_poverty"] = pd.to_numeric(demographics_df["pct_below_poverty"], errors="coerce")
demographics_df["hardship_index"] = pd.to_numeric(demographics_df["hardship_index"], errors="coerce")

demographics_df.sort_values("community_area", inplace=True)
demographics_df.reset_index(drop=True, inplace=True)

print(demographics_df.head(10))
print(f"\nCommunity areas with income data: {demographics_df['per_capita_income'].notna().sum()} / 77")

demographics_df.to_csv("DDR/data/raw_demographics.csv", index=False)
print("Saved to DDR/data/raw_demographics.csv")
