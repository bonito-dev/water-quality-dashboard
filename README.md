# Water Quality Monitoring Dashboard

🌍 **Live dashboard:** https://water-quality-dashboard-bonito-dev.streamlit.app/

A data engineering portfolio project analysing water quality and WASH (Water, Sanitation and Hygiene) trends in Kenya using publicly available data.
Built by a Data Engineer in Training (ALX) with a background in Industrial Chemistry and Water Quality Laboratory Practice.

---

## Project Overview

Water quality data for Kenya exists across multiple international and government sources, but it is scattered, inconsistently formatted, and difficult to interpret without domain knowledge. This project builds a structured data pipeline that ingests, cleans, and stores the data in a relational database, ready for analysis and visualisation.

The dashboard answers one core question:
> *How have key water access and quality indicators changed across Kenya from 2000 to the present, and where are the most critical gaps?*

---

## Architecture

WHO/UNICEF JMP ──┐
├──► Python ingestion ──► PostgreSQL (Supabase) ──► Streamlit Dashboard
World Bank API ──┘         │
└──► Airflow DAG (orchestration)

**Stack**

| Layer | Tool |
|---|---|
| Ingestion | Python, requests, pandas |
| Storage | PostgreSQL (hosted on Supabase) |
| Orchestration | Apache Airflow (in progress) |
| Dashboard | Streamlit (in progress) |
| Version control | Git + GitHub |

---

## Data Sources

### WHO/UNICEF Joint Monitoring Programme (JMP)
- Household WASH estimates for Kenya, 2000–2030
- Covers drinking water service ladder: piped, basic, limited, unimproved, surface water
- Split by Total, Rural, and Urban population
- Years beyond 2025 are JMP regression projections, flagged as `is_estimated = TRUE`
- Access: direct CSV download from `washdata.org`

### World Bank Open Data
- Three indicators with reliable Kenya time-series data:
  - Basic sanitation services — % of population (2000–2024)
  - Water stress — freshwater withdrawal as % of available resources (2000–2022)
  - WASH mortality rate — per 100,000 population (2019 only)
- Access: downloaded as CSV from data.worldbank.org due to API instability

### Known Data Gaps
- Safely managed drinking water (`SH.H2O.SMDW.ZS`) is absent from World Bank Kenya data due to incomplete national reporting. JMP's `pct_at_least_basic_water`
  series is used as the closest available proxy.
- Kenya Open Data Portal (`opendata.go.ke`) was assessed but excluded as a pipeline source due to platform instability. County boundary GeoJSON from the ArcGIS   Hub version will be used for map visualisation in the dashboard.

---

## Database Schema

Star schema with five tables in PostgreSQL:

- `dim_geography` — national and county-level geographies
- `dim_indicator` — metadata for every tracked metric, including units and source
- `dim_data_source` — source registry with methodology notes
- `dim_threshold` — WHO and KEBS safety thresholds per indicator
- `fact_indicator_value` — 700 rows of observed and projected values
- `pipeline_run` — full audit log of every ingestion run

---

## Domain Context

This project benefits from direct laboratory experience. Key indicators are interpreted against the WHO Guidelines for Drinking-water Quality and the Kenya Bureau
of Standards (KEBS) thresholds:

- **Surface water use** above 5% of the population signals high contamination
  risk — surface water sources are vulnerable to faecal, agricultural, and
  industrial pollution
- **Basic water access** below 80% indicates a significant service gap;
  below 50% is a humanitarian threshold
- **Water stress** above 25% of available freshwater resources signals
  water scarcity; above 70% is severe stress
- Kenya's current water stress level is approximately 33% —
  above the scarcity threshold and rising

---

## Project Structure

water-quality-dashboard/
├── data/
│   ├── raw/           # Downloaded source files (not committed)
│   └── processed/     # Cleaned outputs ready for ingestion
├── notebooks/
│   ├── 01_data_exploration.ipynb   # Source exploration and validation
│   └── 02_database_setup.ipynb     # Connection testing
├── src/
│   ├── db.py                # Database connection and schema creation
│   ├── seed.py              # Dimension table seeding
│   ├── ingest_jmp.py        # JMP ingestion script
│   └── ingest_worldbank.py  # World Bank ingestion script
├── .env                     # Local credentials (not committed)
├── .gitignore
├── requirements.txt
└── README.md

---

## Progress

- [x] Project structure and virtual environment
- [x] Data exploration — JMP and World Bank sources assessed and cleaned
- [x] PostgreSQL schema designed and deployed (Supabase)
- [x] Dimension tables seeded — geography, indicators, sources, thresholds
- [x] JMP ingestion script — 651 rows loaded
- [x] World Bank ingestion script — 49 rows loaded
- [x] Pipeline audit log — all runs tracked in pipeline_run table
- [x] Streamlit dashboard — 4 charts, summary metrics, domain annotations
- [x] Deployed live on Streamlit Community Cloud
- [ ] Airflow DAG for orchestration and scheduling
- [ ] Final README update

---

## Setup Instructions

**Prerequisites:** Python 3.11+, Git

```bash
# Clone the repository
git clone https://github.com/YOUR_USERNAME/water-quality-dashboard.git
cd water-quality-dashboard

# Create and activate virtual environment
python -m venv venv
venv\Scripts\activate        # Windows
source venv/bin/activate     # Mac/Linux

# Install dependencies
pip install -r requirements.txt

# Configure environment
cp .env.example .env
# Edit .env with your database credentials

# Create database tables
python src/db.py

# Seed dimension tables
python src/seed.py

# Run ingestion
python src/ingest_jmp.py
python src/ingest_worldbank.py
```

---

## Author

Industrial Chemistry Graduate | ALX Data Engineering Programme  
Water quality laboratory experience (water testing and analysis)