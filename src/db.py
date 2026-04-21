import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def get_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        sslmode=os.getenv("DB_SSLMODE", "require")
    )

def get_sqlalchemy_url():
    return (
        f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
        f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
        f"?sslmode={os.getenv('DB_SSLMODE', 'require')}"
    )

def create_tables():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS dim_geography (
            geo_id        SERIAL PRIMARY KEY,
            name          VARCHAR(100) NOT NULL,
            level         VARCHAR(20)  NOT NULL CHECK (level IN ('national', 'county')),
            iso3_code     CHAR(3),
            parent_geo_id INT REFERENCES dim_geography(geo_id),
            created_at    TIMESTAMPTZ DEFAULT NOW()
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS dim_indicator (
            indicator_id  SERIAL PRIMARY KEY,
            code          VARCHAR(100) NOT NULL UNIQUE,
            name          TEXT         NOT NULL,
            category      VARCHAR(50)  CHECK (category IN (
                              'drinking_water', 'sanitation',
                              'hygiene', 'water_quality', 'water_resources')),
            unit          VARCHAR(30),
            source_system VARCHAR(30)  CHECK (source_system IN (
                              'world_bank', 'jmp', 'wasreb', 'sdg6', 'manual')),
            description   TEXT,
            created_at    TIMESTAMPTZ DEFAULT NOW()
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS dim_data_source (
            source_id         SERIAL PRIMARY KEY,
            name              VARCHAR(100) NOT NULL,
            base_url          TEXT,
            last_fetched_at   TIMESTAMPTZ,
            methodology_notes TEXT,
            created_at        TIMESTAMPTZ DEFAULT NOW()
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS dim_threshold (
            threshold_id  SERIAL PRIMARY KEY,
            indicator_id  INT         NOT NULL REFERENCES dim_indicator(indicator_id),
            authority     VARCHAR(20) NOT NULL CHECK (authority IN ('WHO', 'KEBS', 'EPA')),
            min_value     NUMERIC,
            max_value     NUMERIC,
            severity      VARCHAR(10) NOT NULL CHECK (severity IN ('safe', 'warning', 'critical')),
            notes         TEXT,
            created_at    TIMESTAMPTZ DEFAULT NOW()
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS fact_indicator_value (
            value_id      SERIAL PRIMARY KEY,
            indicator_id  INT         NOT NULL REFERENCES dim_indicator(indicator_id),
            geo_id        INT         NOT NULL REFERENCES dim_geography(geo_id),
            source_id     INT         NOT NULL REFERENCES dim_data_source(source_id),
            year          SMALLINT    NOT NULL CHECK (year BETWEEN 2000 AND 2030),
            value         NUMERIC,
            value_rural   NUMERIC,
            value_urban   NUMERIC,
            is_estimated  BOOLEAN     DEFAULT FALSE,
            loaded_at     TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE (indicator_id, geo_id, source_id, year)
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_run (
            run_id         SERIAL PRIMARY KEY,
            source_id      INT         NOT NULL REFERENCES dim_data_source(source_id),
            started_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            completed_at   TIMESTAMPTZ,
            status         VARCHAR(20) NOT NULL CHECK (status IN (
                               'running', 'success', 'failed', 'partial')),
            rows_ingested  INT         DEFAULT 0,
            error_message  TEXT
        );
    """)

    conn.commit()
    cur.close()
    conn.close()
    print("All tables created successfully.")

if __name__ == "__main__":
    create_tables()