-- Q1  Roll-up / drill-down along the location hierarchy, by fire year
--
-- Question:   How many fires does Thailand have per fire year (Aug-Jul), and
--             how does that break down by province and district?
-- Operation:  ROLLUP (fire year -> province -> district). One result holds every
--             level: grand total, per fire year, per province, per district.
--             `level` says which: 'district', 'province', 'fire year', 'all'.
-- Dimensions: dim_date (fire_year_label), dim_location (province, district),
--             dim_detection_flag (dq_pass)
-- Filter:     detections in Thailand that pass every DQ rule

SELECT CASE grouping(d.fire_year_label, l.province_name, l.district_name)
            WHEN 0 THEN 'district'
            WHEN 1 THEN 'province'
            WHEN 3 THEN 'fire year'
            ELSE 'all' END                       AS level,
       d.fire_year_label,
       l.province_name,
       l.district_name,
       count(*)                                  AS fires,
       round(avg(f.frp_mw), 2)                   AS avg_frp_mw
  FROM dds.fact_active_fire f
  JOIN dds.dim_date d           USING (date_key)
  JOIN dds.dim_location l       USING (location_key)
  JOIN dds.dim_detection_flag q USING (detection_flag_key)
 WHERE l.in_thailand
   AND q.dq_pass
 GROUP BY ROLLUP (d.fire_year_label, l.province_name, l.district_name)
 ORDER BY d.fire_year_label NULLS FIRST, l.province_name NULLS FIRST, l.district_name NULLS FIRST;
