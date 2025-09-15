-- Databricks notebook source
-- MAGIC %md
-- MAGIC https://community.databricks.com/t5/technical-blog/top-10-queries-to-use-with-system-tables/ba-p/82331#toc-hId-1368139511

-- COMMAND ----------

SELECT
    IFNULL(request_params.full_name_arg, 'Non-specific') AS `TABLE ACCESSED`
FROM system.access.audit
WHERE user_identity.email = 'juan.lamadrid@databricks.com'
    AND action_name IN ('commandSubmit','getTable')
GROUP BY IFNULL(request_params.full_name_arg, 'Non-specific')
ORDER BY COUNT(*) DESC
LIMIT 25

-- COMMAND ----------

SELECT
    COUNT(*),
    user_identity.email
FROM
    system.access.audit
WHERE
    action_name IN ('commandSubmit', 'getTable')
    AND request_params.full_name_arg = 'ademianczuk.fitbit.b_health_tracker_data'
GROUP BY user_identity.email
ORDER BY COUNT(*) DESC
LIMIT 10

-- COMMAND ----------

SELECT *
FROM 
  system.compute.clusters 
WHERE 
  delete_time IS NOT NULL;

-- COMMAND ----------

-- https://docs.databricks.com/en/admin/system-tables/audit-logs.html
SELECT *
FROM
    system.access.audit
WHERE
 service_name = "clusters"
 and contains(action_name, "permanentDelete")
--  and user_identity.email == "juan.lamadrid@databricks.com"
 and user_identity.email == "System-User"
--  and request_params.cluster_id == "1204-154528-kjygk3aw"
-- ORDER BY event_time DESC
LIMIT 100

-- COMMAND ----------


-- 1204-154528-kjygk3aw

SELECT *
FROM 
  system.compute.clusters c
WHERE 
  c.delete_time IS NOT NULL;

  -- c.cluster_id = "1204-154528-kjygk3aw"

-- COMMAND ----------


