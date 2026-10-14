import os
import psycopg2
from datetime import datetime

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "fire_dwh",
    "user": "dwh_admin",
    "password": "admin123"
}

def parse_and_clean_csv(file_path):
    conn = psycopg2 . connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    with open(file_path, 'r', encoding='latin1') as f:
        lines = f.readlines()
    
    current_section = None
    headers = []
    
    for line in lines:
        line_str = line . strip()

        # checking empty lines
        if not line_str or line_str.replace(',', '') == '':
            continue
        
            
        parts = [p.strip() for p in line_str.split(',')]
        
        # Capture Header Row
        if not headers and ('latitude' in parts or 'SITE_TYPE' in parts):
            headers = parts
            continue
            
        if headers and len(parts) >= len(headers):
            row_dict = dict(zip(headers, parts[:len(headers)]))
            
            if 'instrument' in row_dict.keys():
                if row_dict['instrument'] == 'MODIS':
                    ingest_modis_row(cursor, row_dict)
                elif row_dict['instrument'] == 'VIIRS':
                    ingest_viirs_row(cursor, row_dict)
            elif 'SITE_TYPE' in row_dict.keys():
                if row_dict['SITE_TYPE'] == 'PA':
                    row_dict["TYPE"] = row_dict.pop('ï»¿TYPE')
                    print(row_dict["REP_AREA"])
                    ingest_wdpa_row(cursor, row_dict)

    conn.commit()
    
    # Run Unified Staging Harmonization & Data Quality Transformation
    transform_and_unify_staging(cursor)
    conn.commit()
    
    cursor.close()
    conn.close()
    print("Successfully ingested and cleansed raw sources into Stage layer.")

def ingest_modis_row(cursor, row):
    sql = """
        INSERT INTO "stage"."stg_modis_fire" 
        ("latitude", "longitude", "brightness", "scan", "track", "acq_date", "acq_time", "satellite", "instrument", "confidence", "version", "bright_t31", "frp", "daynight", "type")
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    try:
        acq_dt = datetime.strptime(row['acq_date'], "%Y-%m-%d") . date()
        cursor.execute(sql, (
            float(row['latitude']), float(row['longitude']), float(row['brightness']),
            float(row['scan']), float(row['track']), acq_dt, row['acq_time'],
            row['satellite'], row['instrument'], int(row['confidence']),
            row['version'], float(row['bright_t31']), float(row['frp']),
            row['daynight'], int(row['type'])
        ))
    except Exception:
        pass

def ingest_viirs_row(cursor, row):
    sql = """
        INSERT INTO "stage"."stg_viirs_fire" 
        ("latitude", "longitude", "bright_ti4", "scan", "track", "acq_date", "acq_time", "satellite", "instrument", "confidence", "version", "bright_ti5", "frp", "daynight", "type")
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    try:
        acq_dt = datetime . strptime(row['acq_date'], "%Y-%m-%d") . date()
        cursor.execute(sql, (
            float(row['latitude']), float(row['longitude']), float(row['bright_ti4']),
            float(row['scan']), float(row['track']), acq_dt, row['acq_time'],
            row['satellite'], row['instrument'], row['confidence'],
            row['version'], float(row['bright_ti5']), float(row['frp']),
            row['daynight'], int(row['type'])
        ))
    except Exception:
        pass

def ingest_wdpa_row(cursor, row):
    sql = """
        INSERT INTO "stage"."stg_wdpa_protected_area" 
        ("site_id", "name_eng", "name_local", "desig_eng", "iucn_cat", "realm", "rep_area", "gis_area", "status", "status_yr", "gov_type", "iso3", "geom_type")
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    try:
        cursor.execute(sql, (
            int(row['SITE_ID']), row['NAME_ENG'], row['NAME'], row['DESIG_ENG'],
            row['IUCN_CAT'], row['REALM'], float(row['REP_AREA'] or 0),
            float(row['GIS_AREA'] or 0), row['STATUS'], int(row['STATUS_YR'] or 0),
            row['GOV_TYPE'], row['ISO3'], row['TYPE']
        ))
    except Exception:
        pass

def transform_and_unify_staging(cursor):
    # Unify MODIS into stage.stg_active_fire_unified with PostGIS Geometries
    cursor.execute("""
        INSERT INTO "stage"."stg_active_fire_unified" 
        ("source_sensor", "latitude", "longitude", "acq_timestamp", "brightness_kelvin", "channel_temp_kelvin", "frp", "scan", "track", "confidence_pct", "satellite", "daynight", "fire_type", "geom", "dq_flag")
        SELECT 
            'MODIS', "latitude", "longitude",
            ("acq_date" + (LPAD("acq_time", 4, '0') || ':00')::INTERVAL) AS "acq_timestamp",
            "brightness" AS "brightness_kelvin",
            "bright_t31" AS "channel_temp_kelvin",
            "frp", "scan", "track", "confidence" AS "confidence_pct",
            "satellite", "daynight", "type",
            ST_SetSRID(ST_MakePoint("longitude", "latitude"), 4326) AS "geom",
            CASE 
                WHEN "latitude" BETWEEN 5.0 AND 21.0 AND "longitude" BETWEEN 97.0 AND 106.0 THEN 'PASS'
                ELSE 'FAIL_SPATIAL_BOUNDS'
            END AS "dq_flag"
        FROM "stage" . "stg_modis_fire";
    """)

    # Unify VIIRS into stage.stg_active_fire_unified with Standardized Confidence Scores
    cursor . execute("""
        INSERT INTO "stage" . "stg_active_fire_unified" 
        ("source_sensor", "latitude", "longitude", "acq_timestamp", "brightness_kelvin", "channel_temp_kelvin", "frp", "scan", "track", "confidence_pct", "satellite", "daynight", "fire_type", "geom", "dq_flag")
        SELECT 
            'VIIRS', "latitude", "longitude",
            ("acq_date" + (LPAD("acq_time", 4, '0') || ':00')::INTERVAL) AS "acq_timestamp",
            "bright_ti4" AS "brightness_kelvin",
            "bright_ti5" AS "channel_temp_kelvin",
            "frp", "scan", "track",
            CASE 
                WHEN "confidence" = 'h' THEN 90
                WHEN "confidence" = 'n' THEN 50
                WHEN "confidence" = 'l' THEN 20
                ELSE 50
            END AS "confidence_pct",
            "satellite", "daynight", "type",
            ST_SetSRID(ST_MakePoint("longitude", "latitude"), 4326) AS "geom",
            CASE 
                WHEN "latitude" BETWEEN 5.0 AND 21.0 AND "longitude" BETWEEN 97.0 AND 106.0 THEN 'PASS'
                ELSE 'FAIL_SPATIAL_BOUNDS'
            END AS "dq_flag"
        FROM "stage" . "stg_viirs_fire";
    """)

if __name__ == '__main__':
    # parse_and_clean_csv(os.path.join('datasets/2024_yearly_summary/modis', 'modis_2024_Thailand.csv'))
    # parse_and_clean_csv(os.path.join('datasets/2024_yearly_summary/viirs-jpss1', 'viirs-jpss1_2024_Thailand.csv'))
    # parse_and_clean_csv(os.path.join('datasets/2024_yearly_summary', 'viirs-snpp_2024_Thailand.csv'))

    # parse_and_clean_csv(os.path.join('datasets/DL_FIRE_J1V-C2_795811', 'fire_archive_J1V-C2_795811.csv'))
    # parse_and_clean_csv(os.path.join('datasets/DL_FIRE_M-C61_795810', 'fire_archive_M-C61_795810.csv'))
    # parse_and_clean_csv(os.path.join('datasets/DL_FIRE_SV-C2_795812', 'fire_archive_SV-C2_795812.csv'))

    parse_and_clean_csv(os.path.join('datasets/WDPA_Aug2026_Public_csv', 'WDPA_Aug2026_Public_csv.csv'))

