import pandas as pd
import psycopg2
import time

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "fire_dwh",
    "user": "dwh_admin",
    "password": "admin123"
}

def execute_step4_spatial_olap():
    conn = psycopg2 . connect(**DB_CONFIG)
    cursor = conn . cursor()
    
    print("Executing Step 4: Spatial Point-in-Polygon Join & OLAP Benchmarking...")
    
    # 1. Execute PostGIS Point-in-Polygon Join
    start_time = time . time()
    cursor . execute("""
        UPDATE "dds" . "fact_active_fire" f
        SET "protected_area_key" = COALESCE(pa."protected_area_key", 0)
        FROM "dds" . "dim_location_spatial" loc
        LEFT JOIN "dds" . "dim_protected_area" pa 
          ON ST_Within(loc."geom", pa."geom") AND pa."is_current" = TRUE
        WHERE f."location_key" = loc."location_key";
    """)
    conn . commit()
    duration = time . time() - start_time
    print(f"Spatial Point-in-Polygon Join completed in {duration:.3f} seconds.")

    # 2. Benchmark Query 2
    cursor . execute("""
        SELECT 
    sat."sensor_type",
    sat."satellite_name",
    COUNT(f."fire_key") AS "detection_count",
    ROUND(AVG(f."frp"), 2) AS "avg_frp",
    ROUND(AVG(f."brightness_kelvin"), 2) AS "avg_sensor_brightness",
    ROUND(AVG(f."confidence_score"), 1) AS "avg_confidence_score"
FROM "dds" . "fact_active_fire" f
JOIN "dds" . "dim_satellite_instrument" sat ON f."satellite_key" = sat."satellite_key"
GROUP BY sat."sensor_type", sat."satellite_name"
ORDER BY "detection_count" DESC;
    """)
    results = cursor . fetchall()
    print(f"\nOLAP Query 1 Results ({len(results)} rows returned):")
    for row in results[:5]:
        print("  ", row)
        
    # save to file
    column_names = [description[0] for description in cursor.description]
    df = pd.DataFrame(results, columns=column_names)
    df.to_csv("olap_query_results/results.csv", index=False)

    # 3. Record Audit Metadata Event
    cursor . execute("""
        INSERT INTO "meta" . "event_log" ("event_type", "event_category", "rows_processed", "status", "note")
        VALUES ('SPATIAL_OLAP_BENCHMARK', 'WEEK_4_MILESTONE', %s, 'SUCCESS', %s);
    """, (len(results), f"Point-in-polygon join executed in {duration:.2f}s"))
    conn . commit()

    cursor . close()
    conn . close()
    print("\nStep 4 Spatial Processing Completed Successfully!")

if __name__ == '__main__':
    execute_step4_spatial_olap()
