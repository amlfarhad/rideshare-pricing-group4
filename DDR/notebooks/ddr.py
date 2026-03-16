import requests
import pandas as pd
import time
import os


# ---------- Section 1: Chicago Rideshare Data ----------

url = "https://data.cityofchicago.org/resource/6dvr-xwnh.json"

# Each month is split into three 10-day windows (early, mid, late) and we pull
# 25,000 rows from each window, giving us 75,000 per month and 225,000 total.
# This avoids the front-loading problem of pulling 75K rows in chronological order,
# which would give us mostly the first 10-12 days of each month.
windows = [
    # October
    ("2025-10-01T00:00:00", "2025-10-11T00:00:00"),
    ("2025-10-11T00:00:00", "2025-10-21T00:00:00"),
    ("2025-10-21T00:00:00", "2025-11-01T00:00:00"),
    # November
    ("2025-11-01T00:00:00", "2025-11-11T00:00:00"),
    ("2025-11-11T00:00:00", "2025-11-21T00:00:00"),
    ("2025-11-21T00:00:00", "2025-12-01T00:00:00"),
    # December
    ("2025-12-01T00:00:00", "2025-12-11T00:00:00"),
    ("2025-12-11T00:00:00", "2025-12-21T00:00:00"),
    ("2025-12-21T00:00:00", "2026-01-01T00:00:00"),
]

all_rides = []

for start, end in windows:
    offset = 0
    window_rows = []

    while len(window_rows) < 25000:
        params = {
            "$select": "trip_start_timestamp,trip_miles,trip_seconds,fare,pickup_community_area,dropoff_community_area",
            "$where": f"trip_start_timestamp >= '{start}' AND trip_start_timestamp < '{end}'",
            "$limit": 25000,
            "$offset": offset,
        }

        success = False
        for attempt in range(1, 6):
            try:
                response = requests.get(url, params=params, timeout=120)
                if response.status_code == 200:
                    success = True
                    break
                print(
                    f"HTTP {response.status_code} on attempt {attempt}/5. Retrying in 10s"
                )
            except requests.exceptions.ReadTimeout:
                print(f"Timeout on attempt {attempt}/5. Retrying in 10s")
            time.sleep(10)

        if not success:
            print(f"Giving up at offset {offset} for window starting {start}")
            break

        data = response.json()
        if not data:
            break

        window_rows.extend(data)
        offset += 25000
        print(f"Window {start[:10]}: {len(window_rows):,} rows so far")

        if len(window_rows) >= 25000:
            window_rows = window_rows[:25000]
            print(f"Hit 25K cap for window {start[:10]}, moving to next window")
            break

    all_rides.extend(window_rows)
    print(f"Total across all windows so far: {len(all_rides):,}")

df = pd.DataFrame(all_rides)
print(f"\nTotal rows fetched: {len(df):,}")

df.to_csv("DDR/data/chicago_rideshare_q4_2025.csv", index=False)
print("Saved to DDR/data/chicago_rideshare_q4_2025.csv")


# ---------- Section 2: Weather Data ----------

weather_url = "https://archive-api.open-meteo.com/v1/archive"

weather_params = {
    "latitude": 41.8781,
    "longitude": -87.6298,
    "start_date": "2025-10-01",
    "end_date": "2025-12-31",
    "hourly": ["temperature_2m", "precipitation", "windspeed_10m", "weathercode"],
    "timezone": "America/Chicago",
}

weather_response = requests.get(weather_url, params=weather_params, timeout=60)
weather_response.raise_for_status()
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


def weather_label(code):
    # Map WMO weather interpretation codes to readable strings
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


# ---------- Section 3: Events Data (Two Sources) ----------

# Source 1: Chicago Special Events Permits (SODA API)
# Pulls permitted events for Q4 2025 and flags days with 3+ permits as major.

permits_url = "https://data.cityofchicago.org/resource/xgse-8eg7.json"

permits_params = {
    "$where": "date >= '2025-10-01T00:00:00' AND date < '2026-01-01T00:00:00'",
    "$limit": 5000,
}

permits_response = requests.get(permits_url, params=permits_params, timeout=60)
permits_response.raise_for_status()
permits_df = pd.DataFrame(permits_response.json())

print(f"Special Events Permits fetched: {len(permits_df):,}")

# extract YYYY-MM-DD from the date field
permits_df["date"] = pd.to_datetime(permits_df["date"]).dt.strftime("%Y-%m-%d")

# flag days with 3+ permits as major
permits_per_day = permits_df["date"].value_counts()
permit_major_days = set(permits_per_day[permits_per_day >= 3].index)

permits_df["is_major_event"] = permits_df["date"].isin(permit_major_days).astype(int)

# keep a standard set of columns for the combined output
permits_df["source"] = "permits"
permits_df["event_name"] = permits_df.get("event_details", pd.Series(dtype=str))
permits_df["venue_name"] = permits_df.get("venue", pd.Series(dtype=str))


# Source 2: Ticketmaster Discovery API
# Pulls all Chicago events for Q4 2025 with pagination, flags days that have
# any event at a venue with 5000+ capacity (or 5000+ upcoming events as proxy).

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

    tm_response = requests.get(tm_url, params=tm_params, timeout=60)
    tm_response.raise_for_status()
    tm_json = tm_response.json()

    # Ticketmaster nests events under _embedded.events
    embedded = tm_json.get("_embedded", {})
    events_list = embedded.get("events", [])
    if not events_list:
        break

    for ev in events_list:
        event_date = ev.get("dates", {}).get("start", {}).get("localDate", "")
        event_name = ev.get("name", "")

        # pull venue info from _embedded.venues[0]
        venue_name = ""
        venue_capacity = 0
        venues = ev.get("_embedded", {}).get("venues", [])
        if venues:
            venue_name = venues[0].get("name", "")
            # use actual capacity if available, otherwise fall back to
            # upcomingEvents._total as a proxy for venue size
            venue_capacity = venues[0].get("capacity", 0) or 0
            if venue_capacity == 0:
                upcoming = venues[0].get("upcomingEvents", {})
                venue_capacity = upcoming.get("_total", 0) or 0
            venue_capacity = int(venue_capacity)

        all_tm_events.append(
            {
                "date": event_date,
                "event_name": event_name,
                "venue_name": venue_name,
                "venue_capacity": venue_capacity,
            }
        )

    # check pagination - stop when we've fetched all pages
    page_info = tm_json.get("page", {})
    total_pages = page_info.get("totalPages", 1)
    page += 1
    print(
        f"Ticketmaster page {page}/{total_pages}: {len(all_tm_events):,} events so far"
    )

    if page >= total_pages:
        break

    # respect Ticketmaster rate limit (5 req/sec)
    time.sleep(0.25)

tm_df = pd.DataFrame(all_tm_events)
print(f"Ticketmaster events fetched: {len(tm_df):,}")

# flag a day as major if any event has venue capacity > 5000
if len(tm_df) > 0:
    tm_df["is_major_event"] = (tm_df["venue_capacity"] > 5000).astype(int)
else:
    tm_df = pd.DataFrame(
        columns=["date", "event_name", "venue_name", "venue_capacity", "is_major_event"]
    )

tm_df["source"] = "ticketmaster"


# Combine both sources into a single events dataframe
# keep: date, event_name, venue_name, source, is_major_event
permits_combined = permits_df[
    ["date", "event_name", "venue_name", "source", "is_major_event"]
].copy()
tm_combined = tm_df[
    ["date", "event_name", "venue_name", "source", "is_major_event"]
].copy()

events_df = pd.concat([permits_combined, tm_combined], ignore_index=True)

# produce a single daily flag: take max of per-event flags across both sources
# this means a day is major if EITHER source flagged it
daily_major = events_df.groupby("date")["is_major_event"].max().reset_index()
daily_major.rename(columns={"is_major_event": "day_has_major_event"}, inplace=True)

# map the daily flag back onto each event row so raw_events.csv has the info
events_df = events_df.merge(daily_major, on="date", how="left")

# keep the per-event is_major_event column (used by merge.py to collapse into day_has_major_event)
# merge.py does: groupby("date")["is_major_event"].max() -> rename to day_has_major_event

print(events_df.head())
print(f"\nTotal combined events: {len(events_df):,}")
print(f"Major events flagged: {events_df['is_major_event'].sum():,}")
print(f"Days with major event: {daily_major['day_has_major_event'].sum():,}")

events_df.to_csv("DDR/data/raw_events.csv", index=False)
print("Saved to DDR/data/raw_events.csv")


# ---------- Section 4: Demographics Data ----------
demo_url = "https://data.cityofchicago.org/resource/kn9c-c2s2.json"

demo_params = {
    "$select": "ca,community_area_name,per_capita_income_,percent_households_below_poverty,hardship_index",
    "$limit": 100,
}

demo_response = requests.get(demo_url, params=demo_params, timeout=60)
demo_response.raise_for_status()
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
