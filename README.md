# Group 4: Rideshare Dynamic Pricing
## BAX 422 (DDR) + BAX 452 (ML&AI) | UC Davis MSBA | Winter 2026

**About:** This project analyzes the drivers of rideshare dynamic pricing in Chicago. By integrating City of Chicago SODA API data with historical weather, local event permits, and neighborhood socioeconomic indicators, we build a robust data pipeline (DDR) to fuel machine learning models (ML) that predict pricing surges and identify non-linear relationships between environment and fare.


**Research Question:** What actually drives rideshare dynamic pricing in Chicago? Is it just supply/demand, or do weather, local events, and neighborhood demographics play a hidden role?

## Project Structure
```
DDR/          ← Data pipeline, scraping, cleaning, schema (BAX 422 report)
  data/       ← Raw CSVs (gitignored if large)
  notebooks/  ← Scraping + cleaning notebooks

ML/           ← Modeling, validation, interpretation (BAX 452 report)
  data/       ← Points to chicago_rides_master.csv
  notebooks/  ← One notebook per model/section
```

## Data Sources
- **Chicago TNP Rides:** data.cityofchicago.org (SODA API) - 75K trips per month, Jan-Mar 2025
- **Weather:** open-meteo.com (Historical Weather API) - hourly temperature, precipitation, wind, weather code
- **Events:** data.cityofchicago.org (Chicago Park District Event Permits, SODA API)
- **Demographics:** data.cityofchicago.org (Chicago Socioeconomic Indicators by Community Area, SODA API) - per capita income, poverty rate, hardship index for all 77 community areas

## Master Dataset
All four sources are joined into `chicago_rides_master.csv`.  
DDR produces it. ML consumes it. Reports are fully independent.

## Team
Amal · Tanmay · Swapna · Hanzhi
