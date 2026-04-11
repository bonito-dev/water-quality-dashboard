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

    cur.execute("SELECT indicator_id, code FROM dim_indicator WHERE source_system = 'jmp'")
    indicator_map = {row[1]: row[0] for row in cur.fetchall()}

    cur.execute("SELECT source_id FROM dim_data_source WHERE name = 'WHO/UNICEF JMP'")
    source_id = cur.fetchone()[0]

    return geo_id, indicator_map, source_id


def load_jmp():
    conn = get_connection()
    cur = conn.cursor()

    # Log pipeline run start
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
        df = pd.read_csv(PROCESSED / "jmp_kenya_clean.csv")
        print(f"Loaded {len(df)} rows from jmp_kenya_clean.csv")

        rows_inserted = 0
        rows_skipped = 0

        for _, row in df.iterrows():
            year = int(row["year"])
            is_estimated = bool(row["is_estimated"])

            for indicator_code, indicator_id in indicator_map.items():
                if indicator_code not in df.columns:
                    continue

                value = row.get(indicator_code)
                if pd.isna(value):
                    continue

                # For rural/urban splits, check if companion columns exist
                rural_col = indicator_code.replace("_total", "_rural")
                urban_col = indicator_code.replace("_total", "_urban")

                value_rural = row.get(rural_col) if rural_col in df.columns else None
                value_urban = row.get(urban_col) if urban_col in df.columns else None

                if pd.isna(value_rural):
                    value_rural = None
                if pd.isna(value_urban):
                    value_urban = None

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
                        float(value_rural) if value_rural is not None else None,
                        float(value_urban) if value_urban is not None else None,
                        is_estimated
                    ))

                    if cur.rowcount:
                        rows_inserted += 1
                    else:
                        rows_skipped += 1

                except Exception as e:
                    print(f"  Row error (year={year}, indicator={indicator_code}): {e}")
                    rows_skipped += 1

        conn.commit()

        # Update pipeline run as success
        cur.execute("""
            UPDATE pipeline_run
            SET status = 'success',
                completed_at = %s,
                rows_ingested = %s
            WHERE run_id = %s;
        """, (datetime.now(timezone.utc), rows_inserted, run_id))
        conn.commit()

        print(f"\nJMP ingestion complete:")
        print(f"  Inserted : {rows_inserted}")
        print(f"  Skipped  : {rows_skipped}")
        print(f"  Run ID   : {run_id}")

    except Exception as e:
        # Log failure to pipeline_run
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
    load_jmp()