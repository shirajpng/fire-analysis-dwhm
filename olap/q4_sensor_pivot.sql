-- Q4  Pivot: sensors against confidence class and day/night
--
-- Question:   How do the five satellites differ in what they detect over
--             Thailand: confidence mix, day/night split, fire radiative power?
-- Operation:  pivot. Confidence class and day/night (junk dimension) become
--             columns with FILTER; satellites (and SP/NRT product) are rows.
--             Replaces the old "MODIS vs VIIRS" and "day vs night" queries.
-- Dimensions: dim_satellite_instrument, dim_detection_flag (junk: daynight,
--             confidence_class, dq_pass), dim_location (in_thailand)
-- Filter:     detections in Thailand that pass every DQ rule
-- Note:       confidence_class (low / nominal / high) is the only confidence
--             both sensors have: VIIRS delivers no percentage.

SELECT s.sensor_type,
       s.satellite_name,
       s.product,
       count(*)                                                              AS detections,
       round(100.0 * count(*) FILTER (WHERE q.confidence_class = 'high')    / count(*), 1) AS pct_high,
       round(100.0 * count(*) FILTER (WHERE q.confidence_class = 'nominal') / count(*), 1) AS pct_nominal,
       round(100.0 * count(*) FILTER (WHERE q.confidence_class = 'low')     / count(*), 1) AS pct_low,
       round(100.0 * count(*) FILTER (WHERE q.daynight = 'D')               / count(*), 1) AS pct_day,
       round(avg(f.frp_mw) FILTER (WHERE q.daynight = 'D'), 2)              AS avg_frp_day_mw,
       round(avg(f.frp_mw) FILTER (WHERE q.daynight = 'N'), 2)              AS avg_frp_night_mw,
       round(avg(f.brightness_k), 1)                                         AS avg_brightness_k
  FROM dds.fact_active_fire f
  JOIN dds.dim_satellite_instrument s USING (satellite_key)
  JOIN dds.dim_detection_flag q       USING (detection_flag_key)
  JOIN dds.dim_location l             USING (location_key)
 WHERE l.in_thailand
   AND q.dq_pass
 GROUP BY s.sensor_type, s.satellite_name, s.product
 ORDER BY s.sensor_type, s.satellite_name, s.product;
