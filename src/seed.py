import os
from dotenv import load_dotenv
from db import get_connection

load_dotenv()

def seed_geography():
    conn = get_connection()
    cur = conn.cursor()

    # Insert Kenya as the national record first
    cur.execute("""
        INSERT INTO dim_geography (name, level, iso3_code)
        VALUES ('Kenya', 'national', 'KEN')
        ON CONFLICT DO NOTHING
        RETURNING geo_id;
    """)
    result = cur.fetchone()

    if result:
        kenya_id = result[0]
        print(f"Inserted Kenya with geo_id={kenya_id}")
    else:
        cur.execute("SELECT geo_id FROM dim_geography WHERE iso3_code = 'KEN'")
        kenya_id = cur.fetchone()[0]
        print(f"Kenya already exists with geo_id={kenya_id}")

    conn.commit()
    cur.close()
    conn.close()
    return kenya_id


def seed_indicators():
    conn = get_connection()
    cur = conn.cursor()

    indicators = [
        # JMP indicators
        ("pct_improved_water_total",       "Improved water access (total)",                    "drinking_water", "%",       "jmp"),
        ("pct_improved_water_rural",       "Improved water access (rural)",                    "drinking_water", "%",       "jmp"),
        ("pct_improved_water_urban",       "Improved water access (urban)",                    "drinking_water", "%",       "jmp"),
        ("pct_at_least_basic_water_total", "At least basic water access (total)",              "drinking_water", "%",       "jmp"),
        ("pct_at_least_basic_water_rural", "At least basic water access (rural)",              "drinking_water", "%",       "jmp"),
        ("pct_at_least_basic_water_urban", "At least basic water access (urban)",              "drinking_water", "%",       "jmp"),
        ("pct_limited_water_total",        "Limited water access (total)",                     "drinking_water", "%",       "jmp"),
        ("pct_limited_water_rural",        "Limited water access (rural)",                     "drinking_water", "%",       "jmp"),
        ("pct_limited_water_urban",        "Limited water access (urban)",                     "drinking_water", "%",       "jmp"),
        ("pct_unimproved_water_total",     "Unimproved water access (total)",                  "drinking_water", "%",       "jmp"),
        ("pct_unimproved_water_rural",     "Unimproved water access (rural)",                  "drinking_water", "%",       "jmp"),
        ("pct_unimproved_water_urban",     "Unimproved water access (urban)",                  "drinking_water", "%",       "jmp"),
        ("pct_surface_water_total",        "Surface water use (total)",                        "drinking_water", "%",       "jmp"),
        ("pct_surface_water_rural",        "Surface water use (rural)",                        "drinking_water", "%",       "jmp"),
        ("pct_surface_water_urban",        "Surface water use (urban)",                        "drinking_water", "%",       "jmp"),
        ("pct_piped_water_total",          "Piped water access (total)",                       "drinking_water", "%",       "jmp"),
        ("pct_piped_water_rural",          "Piped water access (rural)",                       "drinking_water", "%",       "jmp"),
        ("pct_piped_water_urban",          "Piped water access (urban)",                       "drinking_water", "%",       "jmp"),
        # World Bank indicators
        ("SH.STA.BASS.ZS",                "Basic sanitation services (% of population)",      "sanitation",     "%",       "world_bank"),
        ("ER.H2O.FWST.ZS",               "Water stress: freshwater withdrawal (%)",           "water_resources","% ",      "world_bank"),
        ("SH.STA.WASH.P5",               "Mortality from unsafe WASH (per 100k)",             "water_quality",  "per_100k","world_bank"),
        # Population
        ("population_thousands_total",    "Total population (thousands)",                      "drinking_water", "1000s",   "jmp"),
        ("population_thousands_rural",    "Rural population (thousands)",                      "drinking_water", "1000s",   "jmp"),
        ("population_thousands_urban",    "Urban population (thousands)",                      "drinking_water", "1000s",   "jmp"),
    ]

    inserted = 0
    for code, name, category, unit, source_system in indicators:
        cur.execute("""
            INSERT INTO dim_indicator (code, name, category, unit, source_system)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (code) DO NOTHING;
        """, (code, name, category, unit, source_system))
        if cur.rowcount:
            inserted += 1

    conn.commit()
    cur.close()
    conn.close()
    print(f"Indicators seeded: {inserted} new, {len(indicators) - inserted} already existed")


def seed_data_sources():
    conn = get_connection()
    cur = conn.cursor()

    sources = [
        (
            "WHO/UNICEF JMP",
            "https://washdata.org/data/country/KEN/household/download",
            "JMP household estimates 2000-2030. Regression-based. Years beyond current are projections."
        ),
        (
            "World Bank Open Data",
            "https://api.worldbank.org/v2/country/KE/indicator/",
            "World Development Indicators. Downloaded as CSV due to API instability."
        ),
    ]

    inserted = 0
    for name, url, notes in sources:
        cur.execute("""
            INSERT INTO dim_data_source (name, base_url, methodology_notes)
            VALUES (%s, %s, %s)
            ON CONFLICT DO NOTHING;
        """, (name, url, notes))
        if cur.rowcount:
            inserted += 1

    conn.commit()
    cur.close()
    conn.close()
    print(f"Data sources seeded: {inserted} new")


def seed_thresholds():
    conn = get_connection()
    cur = conn.cursor()

    # Get indicator IDs we need
    cur.execute("SELECT indicator_id, code FROM dim_indicator")
    indicator_map = {row[1]: row[0] for row in cur.fetchall()}

    thresholds = [
        # Basic water access — WHO SDG targets
        ("pct_at_least_basic_water_total", "WHO", 80,  None, "warning",  "Below 80% signals significant service gap"),
        ("pct_at_least_basic_water_total", "WHO", 50,  None, "critical", "Below 50% is a humanitarian threshold"),

        # Surface water use — your chemistry background: surface water = high contamination risk
        ("pct_surface_water_total",        "WHO", None, 5,   "warning",  "Above 5% surface water use is a health risk"),
        ("pct_surface_water_total",        "WHO", None, 15,  "critical", "Above 15% indicates critical access failure"),

        # WASH mortality
        ("SH.STA.WASH.P5",                "WHO", None, 10,  "warning",  "Above 10 per 100k warrants investigation"),
        ("SH.STA.WASH.P5",                "WHO", None, 25,  "critical", "Above 25 per 100k is a public health emergency"),

        # Water stress — FAO thresholds you'll know from chemistry/environmental context
        ("ER.H2O.FWST.ZS",               "WHO", None, 25,  "warning",  "Above 25% = water stressed"),
        ("ER.H2O.FWST.ZS",               "WHO", None, 70,  "critical", "Above 70% = severely water stressed"),
    ]

    inserted = 0
    skipped = 0
    for code, authority, min_val, max_val, severity, notes in thresholds:
        ind_id = indicator_map.get(code)
        if not ind_id:
            print(f"  Warning: indicator '{code}' not found — skipping threshold")
            skipped += 1
            continue
        cur.execute("""
            INSERT INTO dim_threshold
                (indicator_id, authority, min_value, max_value, severity, notes)
            VALUES (%s, %s, %s, %s, %s, %s);
        """, (ind_id, authority, min_val, max_val, severity, notes))
        inserted += 1

    conn.commit()
    cur.close()
    conn.close()
    print(f"Thresholds seeded: {inserted} inserted, {skipped} skipped")


if __name__ == "__main__":
    print("Seeding dimension tables...\n")
    seed_geography()
    seed_indicators()
    seed_data_sources()
    seed_thresholds()
    print("\nAll dimension tables seeded.")