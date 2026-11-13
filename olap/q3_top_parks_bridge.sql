-- Q3  Many-to-many through the bridge: the 10 protected areas with most fires
--
-- Question:   Which protected areas burn most, and how much of that is shared
--             with an overlapping park?
-- Operation:  roll-up over the multi-valued protected-area dimension. A fire
--             inside two overlapping parks has two bridge rows:
--               fires_in_park   count(*): each park gets its full count
--               fires_allocated sum(allocation_weight): the fire is split
--                               1/n between its parks, so summed over all
--                               parks it equals the number of fires (no
--                               double counting)
--               fires_primary   fires whose fact row points at this park
--             Versions of one park (SCD Type 2) are rolled up to the site.
-- Dimensions: bridge_fire_protected_area, dim_protected_area, dim_date
-- Filter:     all fires in a park (the bridge only holds fires in Thailand)

SELECT p.site_id,
       (array_agg(p.name_eng  ORDER BY p.version_no DESC))[1]  AS park_name,
       (array_agg(p.desig_eng ORDER BY p.version_no DESC))[1]  AS designation,
       (array_agg(p.status    ORDER BY p.version_no DESC))[1]  AS current_status,
       count(DISTINCT p.protected_area_key)                    AS versions_hit,
       count(*)                                                AS fires_in_park,
       round(sum(b.allocation_weight), 1)                      AS fires_allocated,
       count(*) FILTER (WHERE b.is_primary)                    AS fires_primary,
       count(*) FILTER (WHERE b.allocation_weight < 1)         AS fires_shared_with_other_park,
       min(d.full_date)                                        AS first_fire,
       max(d.full_date)                                        AS last_fire
  FROM dds.bridge_fire_protected_area b
  JOIN dds.dim_protected_area p USING (protected_area_key)
  JOIN dds.dim_date d           USING (date_key)
 GROUP BY p.site_id
 ORDER BY fires_in_park DESC, p.site_id
 LIMIT 10;
