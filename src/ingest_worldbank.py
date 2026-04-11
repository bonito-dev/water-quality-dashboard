import os
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone
from dotenv import load_dotenv
from db import get_connection

load_dotenv()

PROCESSED = Path("data/processed")


def get_lookup_maps(cur):
    cur.execute("SELECT geo_id FROM dim_geography WHERE iso3_code = 'KEN'")
    geo_id = cur.fetchone()[0]

    cur.execute("SELECT indicator_id, code FROM dim_indicator WHERE source_system = 'world_bank'")
    indicator_map = {row[1]: row[0] for row in cur.fetchall()}

    cur.execute("SELECT source_id FROM dim_data_source WHERE name = 'World Bank Open Data'")
    source_id = cur.fetchone()[0]

    return geo_id, indicator_map, source_id


def load_worldbank():
    conn = get_connection()
    cur = conn.cursor()

    geo_id, indicator_map, source_id = get_lookup_maps(cur)

    cur.execute("""
        INSERT INTO pipeline_run (source_id, started_at, status)
        VALUES (%s, %s, 'running')
        RETURNING run_id;
    """, (source_id, datetime.now(timezone.utc)))
    run_id = cur.fetchone()[0]
    conn.commit()
    print(f"Pipeline run started — run_id={run_id}")

    try:
        df = pd.read_csv(PROCESSED / "wb_kenya_long.csv")
        print(f"Loaded {len(df)} rows from wb_kenya_long.csv")

        # Check what columns we have
        print("Columns:", df.columns.tolist())

        rows_inserted = 0
        rows_skipped = 0

        for _, row in df.iterrows():
            year = int(row["year"])

            # Match indicator code to database
            indicator_code = row.get("indicator_code")
            if pd.isna(indicator_code):
                rows_skipped += 1
                continue

            indicator_id = indicator_map.get(indicator_code)
            if not indicator_id:
                rows_skipped += 1
                continue

            value = row.get("value")
            if pd.isna(value):
                rows_skipped += 1
                continue

            try:
                cur.execute("""
                    INSERT INTO fact_indicator_value
                        (indicator_id, geo_id, source_id, year,
                         value, value_rural, value_urban, is_estimated)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (indicator_id, geo_id, source_id, year)
                    DO NOTHING;
                """, (
                    indicator_id, geo_id, source_id, year,
                    float(value),
                    None,   # World Bank aggregates don't split rural/urban here
                    None,
                    False   # All World Bank values are observed, not projected
                ))

                if cur.rowcount:
                    rows_inserted += 1
                else:
                    rows_skipped += 1

            except Exception as e:
                print(f"  Row error (year={year}, indicator={indicator_code}): {e}")
                rows_skipped += 1

        conn.commit()

        cur.execute("""
            UPDATE pipeline_run
            SET status = 'success',
                completed_at = %s,
                rows_ingested = %s
            WHERE run_id = %s;
        """, (datetime.now(timezone.utc), rows_inserted, run_id))
        conn.commit()

        print(f"\nWorld Bank ingestion complete:")
        print(f"  Inserted : {rows_inserted}")
        print(f"  Skipped  : {rows_skipped}")
        print(f"  Run ID   : {run_id}")

    except Exception as e:
        cur.execute("""
            UPDATE pipeline_run
            SET status = 'failed',
                completed_at = %s,
                error_message = %s
            WHERE run_id = %s;
        """, (datetime.now(timezone.utc), str(e), run_id))
        conn.commit()
        raise

    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    load_worldbank()