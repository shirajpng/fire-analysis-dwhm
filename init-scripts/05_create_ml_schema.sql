-- ============================================================================
-- STEP 5: MACHINE LEARNING ANOMALY & CLUSTER STORAGE TABLE
-- ============================================================================

CREATE TABLE IF NOT EXISTS "dds" . "ml_fire_anomalies" (
    "anomaly_id" SERIAL PRIMARY KEY,
    "fire_key" BIGINT REFERENCES "dds" . "fact_active_fire"("fire_key"),
    "cluster_label" INT NOT NULL,
    "anomaly_score" NUMERIC(6,4) NOT NULL,
    "is_anomaly" BOOLEAN NOT NULL,
    "anomaly_reason" VARCHAR(100),
    "created_at" TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS "idx_ml_fire_key" ON "dds" . "ml_fire_anomalies" ("fire_key");
CREATE INDEX IF NOT EXISTS "idx_ml_is_anomaly" ON "dds" . "ml_fire_anomalies" ("is_anomaly");
