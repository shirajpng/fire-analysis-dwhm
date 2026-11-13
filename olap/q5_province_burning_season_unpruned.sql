-- Q5 (unpruned variant), for the partition-pruning benchmark only
--
-- Identical to q5_province_burning_season.sql except that the year is filtered
-- on dim_date.year_number instead of on the fact's partition key. The planner
-- cannot turn a filter on a dimension into a date_key range at plan time, so it
-- scans all fact partitions. olap_benchmark.py checks that both return the same
-- rows.

SELECT coalesce(d.month_name, 'Jan-Apr')      AS month,
       coalesce(l.district_name, 'all')       AS district,
       count(*)                               AS fires,
       round(sum(f.frp_mw), 0)                AS total_frp_mw,
       count(*) FILTER (WHERE f.protected_area_key > 0) AS in_protected_area
  FROM dds.fact_active_fire f
  JOIN dds.dim_date d           USING (date_key)
  JOIN dds.dim_location l       USING (location_key)
  JOIN dds.dim_detection_flag q USING (detection_flag_key)
 WHERE d.year_number = 2024                          -- dimension only: no pruning
   AND d.is_burning_season
   AND l.province_name = 'Chiang Mai'
   AND q.dq_pass
 GROUP BY ROLLUP ((d.month_number, d.month_name), l.district_name)
 ORDER BY d.month_number NULLS FIRST, l.district_name NULLS FIRST;
