-- Q5  Slice + drill-down: Chiang Mai, burning season 2024, by month and district
--
-- Question:   In the 2024 burning season (Jan-Apr), when and where did Chiang Mai
--             burn?
-- Operation:  slice on one province and one year; drill down from the season
--             total to months, and from each month to districts (ROLLUP).
-- Dimensions: dim_date (year, month, is_burning_season), dim_location,
--             dim_detection_flag (dq_pass)
-- Benchmark:  the year is filtered on the fact's partition key (date_key), so
--             the planner reads only the 2024 partition. The same query with the
--             year filtered on dim_date only is
--             q5_province_burning_season_unpruned.sql: same result, every
--             partition scanned.

SELECT coalesce(d.month_name, 'Jan-Apr')      AS month,
       coalesce(l.district_name, 'all')       AS district,
       count(*)                               AS fires,
       round(sum(f.frp_mw), 0)                AS total_frp_mw,
       count(*) FILTER (WHERE f.protected_area_key > 0) AS in_protected_area
  FROM dds.fact_active_fire f
  JOIN dds.dim_date d           USING (date_key)
  JOIN dds.dim_location l       USING (location_key)
  JOIN dds.dim_detection_flag q USING (detection_flag_key)
 WHERE f.date_key BETWEEN 20240101 AND 20241231      -- partition key: prunes to 2024
   AND d.is_burning_season
   AND l.province_name = 'Chiang Mai'
   AND q.dq_pass
 GROUP BY ROLLUP ((d.month_number, d.month_name), l.district_name)
 ORDER BY d.month_number NULLS FIRST, l.district_name NULLS FIRST;
