# Group 4: Rideshare Dynamic Pricing
## BAX 422 (DDR) + BAX 452 (ML&AI) | UC Davis MSBA | Winter 2026

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
- **Chicago TNP Rides:** data.cityofchicago.org (SODA API)
- **Weather:** open-meteo.com (Historical Weather API, free, no key needed)
- **Events:** choosechicago.com (scraped with BeautifulSoup)
- **Demographics:** api.census.gov (ACS 5-Year Estimates, Table B19013)

## Master Dataset
All four sources are joined into `chicago_rides_master.csv`.  
DDR produces it. ML consumes it. Reports are fully independent.

## Team
Amal · Tanmay · Swapna · Hanzhi
