import psycopg2
import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from scipy.spatial import cKDTree
from scipy.sparse import csr_matrix
from scipy.sparse . csgraph import connected_components

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "fire_dwh",
    "user": "dwh_admin",
    "password": "admin123"
}

def run_machine_learning_pipeline():

    conn = None 
    cursor = None
    try:
        conn = psycopg2 . connect(**DB_CONFIG)
        cursor = conn.cursor()
        print("--- 1. Extracting Active Fire Observations for ML Mining ---")
        query = """
            SELECT 
                f . "fire_key",
                f . "brightness_kelvin",
                f . "channel_temp_kelvin",
                f . "frp",
                f . "confidence_score",
                loc . "latitude",
                loc . "longitude"
            FROM "dds" . "fact_active_fire" f
            JOIN "dds" . "dim_location_spatial" loc ON f . "location_key" = loc . "location_key";
        """
        cursor . execute(query)
        column_names = [col_desc[0] for col_desc in cursor . description] 
        # Fetch all result rows
        raw_rows = cursor.fetchall()
        
        if not raw_rows:
            print("No records found in dds.fact_active_fire for machine learning analysis.")
            cursor.close()
            conn.close()
            return

        # Convert cursor results into a Pandas DataFrame 
        df = pd . DataFrame(raw_rows, columns=column_names) 
        print(f"Loaded {len(df)} fact records for clustering and anomaly detection.")

        # Ensure numeric data types for analytical features 
        numeric_cols = ["brightness_kelvin", "channel_temp_kelvin", "frp", "confidence_score", "latitude", "longitude"] 
        for col in numeric_cols: 
            df[col] = pd . to_numeric(df[col], errors='coerce').fillna(0.0)
        # -------------------------------------------------------------------------
        # 2. SPATIAL DENSITY CLUSTERING (HDBSCAN / Spatial Graph)
        # -------------------------------------------------------------------------
        print("\n--- 2. Executing Spatial Density Clustering ---")
        # Convert geographic coordinates to metric scale (1° Lat ~ 111,000m, 1° Lon ~ 107,000m)
        coords_m = np . column_stack([
            df["latitude"] . values * 111000.0,
            df["longitude"] . values * 107000.0
        ])

        # 1.5 km Spatial Clustering Radius
        tree = cKDTree(coords_m)
        candidate_pairs = tree . query_pairs(r=1500.0)

        n_points = len(df)
        if candidate_pairs:
            rows = [p[0] for p in candidate_pairs] + [p[1] for p in candidate_pairs]
            cols = [p[1] for p in candidate_pairs] + [p[0] for p in candidate_pairs]
            data = np . ones(len(rows), dtype=int)
            graph = csr_matrix((data, (rows, cols)), shape=(n_points, n_points))
            _, cluster_labels = connected_components(csgraph=graph, directed=False)
        else:
            cluster_labels = np . arange(n_points)

        df["cluster_label"] = cluster_labels
        num_clusters = len(np . unique(cluster_labels))
        print(f"Spatial Clustering Identified {num_clusters} Physical Fire Clusters.")

        # -------------------------------------------------------------------------
        # 3. ISOLATION FOREST ANOMALY DETECTION
        # -------------------------------------------------------------------------
        print("\n--- 3. Training Isolation Forest Anomaly Detector ---")
        feature_cols = ["brightness_kelvin", "channel_temp_kelvin", "frp", "confidence_score"]
        X = df[feature_cols] . fillna(0.0) . values

        # Train Isolation Forest (5% contamination threshold for abnormal spikes)
        model = IsolationForest(n_estimators=100, contamination=0.05, random_state=42)
        model . fit(X)

        # Predictions: -1 indicates anomaly, 1 indicates normal
        predictions = model . predict(X)
        anomaly_scores = model . decision_function(X) # Lower score = higher anomaly

        df["is_anomaly"] = predictions == -1
        df["anomaly_score"] = np . round(anomaly_scores, 4)
        df["anomaly_reason"] = np . where(
            df["is_anomaly"], 
            "Abnormal Thermal Radiative Energy / Extreme Intensity Spike", 
            "Normal Active Fire Observation"
        )

        num_anomalies = df["is_anomaly"] . sum()
        print(f"Isolation Forest Detected {num_anomalies} Thermal Anomaly Incidents.")

        # -------------------------------------------------------------------------
        # 4. STORE RESULTS IN ML_FIRE_ANOMALIES
        # -------------------------------------------------------------------------
        cursor = conn . cursor()
        
        # Clean previous ML outputs
        cursor . execute('TRUNCATE TABLE "dds" . "ml_fire_anomalies";')

        insert_sql = """
            INSERT INTO "dds" . "ml_fire_anomalies"
            ("fire_key", "cluster_label", "anomaly_score", "is_anomaly", "anomaly_reason")
            VALUES (%s, %s, %s, %s, %s);
        """
        
        for _, r in df . iterrows():
            cursor . execute(insert_sql, (
                int(r["fire_key"]),
                int(r["cluster_label"]),
                float(r["anomaly_score"]),
                bool(r["is_anomaly"]),
                str(r["anomaly_reason"])
            ))

        # Log Metadata Event
        cursor . execute("""
            INSERT INTO "meta" . "event_log" ("event_type", "event_category", "rows_processed", "status", "note")
            VALUES ('ML_ANOMALY_DETECTION', 'MACHINE_LEARNING', %s, 'SUCCESS', %s);
        """, (len(df), f"Clustered {num_clusters} events and flagged {num_anomalies} anomalies"))

        conn . commit()
        cursor . close()
        conn . close()
        print("\nMachine Learning Pipeline Completed and Results Persisted to ml_fire_anomalies!")
    except Exception as e: 
        print(f"Error during execution: {e}") 
        if conn: 
            conn . rollback() 
    finally: 
        # Safe closing prevents UnboundLocalError 
        if cursor: 
            cursor . close() 
        if conn: 
            conn . close()

if __name__ == '__main__':
    run_machine_learning_pipeline()
