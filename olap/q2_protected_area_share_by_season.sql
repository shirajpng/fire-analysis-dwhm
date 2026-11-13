-- Q2  Slice / dice: share of Thai fires inside protected areas (RQ2)
--
-- Question:   What share of Thai fires falls inside a Designated or Inscribed
--             protected area, per fire year and Thai season, and how much more
--             falls only inside a Proposed one?
-- Operation:  dice on location (Thailand) and protected-area status, sliced by
--             fire year x season; ROLLUP adds the per-fire-year and overall rows.
-- Dimensions: dim_date (fire_year_label, thai_season), dim_location (in_thailand),
--             dim_protected_area (status of the park version valid on the
--             detection date: SCD Type 2, point in time)
-- Filter:     same definition as the official RQ2 table in
--             docs/etl_protected_area.md (in Thailand, flagged rows included), so
--             the 'all' row reconciles with it: 4,418,597 fires, 25.3% designated.

SELECT coalesce(d.fire_year_label, 'all')          AS fire_year,
       coalesce(d.thai_season, 'all')              AS thai_season,
       count(*)                                    AS thai_fires,
       count(*) FILTER (WHERE p.status IN ('Designated', 'Inscribed'))  AS in_designated_pa,
       round(100.0 * count(*) FILTER (WHERE p.status IN ('Designated', 'Inscribed'))
             / count(*), 1)                        AS pct_designated,
       count(*) FILTER (WHERE p.status = 'Proposed')                    AS only_in_proposed_pa,
       round(100.0 * count(*) FILTER (WHERE p.status = 'Proposed')
             / count(*), 2)                        AS pct_proposed_only
  FROM dds.fact_active_fire f
  JOIN dds.dim_date d           USING (date_key)
  JOIN dds.dim_location l       USING (location_key)
  JOIN dds.dim_protected_area p USING (protected_area_key)
 WHERE l.in_thailand
 GROUP BY ROLLUP (d.fire_year_label, d.thai_season)
 ORDER BY d.fire_year_label NULLS LAST,
          CASE d.thai_season WHEN 'Cool' THEN 1 WHEN 'Hot' THEN 2 WHEN 'Rainy' THEN 3 ELSE 4 END;
