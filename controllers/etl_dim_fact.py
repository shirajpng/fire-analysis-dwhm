import psycopg2

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "fire_dwh",
    "user": "dwh_admin",
    "password": "admin123"
}

def execute_warehouse_load():
    conn = psycopg2 . connect(**DB_CONFIG)
    cursor = conn . cursor()
    
    print("Starting Warehouse Population (NDS + DDS + META)...")

    # 1. Populate NDS Layer
    cursor . execute("""
        INSERT INTO "nds" . "nds_satellite" ("satellite_name")
        SELECT DISTINCT "satellite" FROM "stage" . "stg_active_fire_unified"
        ON CONFLICT ("satellite_name") DO NOTHING;
    """)

    cursor . execute("""
        INSERT INTO "nds" . "nds_instrument" ("instrument_name", "sensor_type", "version")
        SELECT DISTINCT 'MODIS', 'MODIS', '6.1' FROM "stage" . "stg_active_fire_unified" WHERE "source_sensor" = 'MODIS'
        UNION
        SELECT DISTINCT 'VIIRS', 'VIIRS', '2' FROM "stage" . "stg_active_fire_unified" WHERE "source_sensor" = 'VIIRS'
        ON CONFLICT ON CONSTRAINT "uq_instrument_type" DO NOTHING;
    """)

    cursor . execute("""
        INSERT INTO "nds" . "nds_location" ("latitude", "longitude", "geom")
        SELECT DISTINCT "latitude", "longitude", "geom"
        FROM "stage" . "stg_active_fire_unified"
        WHERE "dq_flag" = 'PASS'
        ON CONFLICT ON CONSTRAINT "uq_lat_lon" DO NOTHING;
    """)

    cursor . execute("""
        INSERT INTO "nds" . "nds_protected_area" ("site_id", "name_eng", "desig_eng", "iucn_cat", "rep_area", "gov_type", "iso3")
        SELECT "site_id", "name_eng", "desig_eng", "iucn_cat", "rep_area", "gov_type", "iso3"
        FROM "stage" . "stg_wdpa_protected_area";
    """)

    # 2. Populate DDS Dimensions & Seed Default Key 0
    cursor . execute("""
        INSERT INTO "dds" . "dim_date" ("date_key", "full_date", "day_of_week", "day_name", "day_of_month", "month_number", "month_name", "quarter_number", "year_number")
        SELECT 
            TO_CHAR(d, 'YYYYMMDD')::INT AS "date_key",
            d::DATE AS "full_date",
            EXTRACT(DOW FROM d)::INT AS "day_of_week",
            TO_CHAR(d, 'Day') AS "day_name",
            EXTRACT(DAY FROM d)::INT AS "day_of_month",
            EXTRACT(MONTH FROM d)::INT AS "month_number",
            TO_CHAR(d, 'Month') AS "month_name",
            EXTRACT(QUARTER FROM d)::INT AS "quarter_number",
            EXTRACT(YEAR FROM d)::INT AS "year_number"
        FROM generate_series('2020-01-01'::DATE, '2030-12-31'::DATE, '1 day'::INTERVAL) d
        ON CONFLICT ("date_key") DO NOTHING;
    """)

    cursor . execute("""
        INSERT INTO "dds" . "dim_location_spatial" ("latitude", "longitude", "geom")
        SELECT "latitude", "longitude", "geom"
        FROM "nds" . "nds_location"
        ON CONFLICT DO NOTHING;
    """)

    cursor . execute("""
        INSERT INTO "dds" . "dim_satellite_instrument" ("satellite_name", "instrument_name", "sensor_type", "version")
        SELECT DISTINCT s."satellite_name", i."instrument_name", i."sensor_type", i."version"
        FROM "nds" . "nds_satellite" s
        CROSS JOIN "nds" . "nds_instrument" i;
    """)

    # Seed Default Dummy Record (Key 0) for Unprotected Land
    cursor . execute("""
        INSERT INTO "dds" . "dim_protected_area" 
        ("protected_area_key", "site_id", "name_eng", "desig_eng", "iucn_cat", "rep_area", "gov_type", "effective_timestamp", "expiry_timestamp", "is_current")
        VALUES 
        (0, 0, 'Unprotected / Outside PA', 'None', 'N/A', 0.0, 'None', '2024-01-01 00:00:00'::TIMESTAMP, NULL, TRUE)
        ON CONFLICT ("protected_area_key") DO NOTHING;
    """)

    # Populate Protected Area Dimension
    cursor . execute("""
        INSERT INTO "dds" . "dim_protected_area" 
        ("site_id", "name_eng", "desig_eng", "iucn_cat", "rep_area", "gov_type", "effective_timestamp", "expiry_timestamp", "is_current")
        SELECT 
            "site_id", "name_eng", "desig_eng", "iucn_cat", "rep_area", "gov_type",
            '2024-01-01 00:00:00'::TIMESTAMP AS "effective_timestamp",
            NULL AS "expiry_timestamp",
            TRUE AS "is_current"
        FROM "nds" . "nds_protected_area";
    """)

    # 3. Populate DDS Fact Table
    cursor . execute("""
        INSERT INTO "dds" . "fact_active_fire" 
        ("date_key", "location_key", "satellite_key", "protected_area_key", "acq_time", "brightness_kelvin", "channel_temp_kelvin", "frp", "scan", "track", "confidence_score", "daynight")
        SELECT 
            TO_CHAR(stg."acq_timestamp", 'YYYYMMDD')::INT AS "date_key",
            loc."location_key",
            sat."satellite_key",
            COALESCE(pa."protected_area_key", 0) AS "protected_area_key",
            TO_CHAR(stg."acq_timestamp", 'HH24MI') AS "acq_time",
            stg."brightness_kelvin",
            stg."channel_temp_kelvin",
            stg."frp",
            stg."scan",
            stg."track",
            stg."confidence_pct",
            stg."daynight"
        FROM "stage" . "stg_active_fire_unified" stg
        JOIN "dds" . "dim_location_spatial" loc 
          ON loc."latitude" = stg."latitude" AND loc."longitude" = stg."longitude"
        JOIN "dds" . "dim_satellite_instrument" sat 
          ON sat."satellite_name" = stg."satellite" AND sat."sensor_type" = stg."source_sensor"
        LEFT JOIN "dds" . "dim_protected_area" pa 
          ON pa."site_id" = 1 AND pa."is_current" = TRUE;
    """)

    # 4. Audit Log
    cursor . execute("SELECT COUNT(*) FROM \"dds\" . \"fact_active_fire\";")
    fact_count = cursor . fetchone()

    cursor . execute("""
        INSERT INTO "meta" . "event_log" ("event_type", "event_category", "rows_processed", "status", "note")
        VALUES ('FULL_LOAD', 'ETL_POPULATION', %s, 'SUCCESS', 'Completed NDS and DDS physical warehouse population');
    """, (fact_count,))

    conn . commit()
    cursor . close()
    conn . close()
    print(f"Warehouse Population Completed Successfully! Total Fact Rows: {fact_count}")

if __name__ == '__main__':
    execute_warehouse_load()
