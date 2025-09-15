-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Billing
-- MAGIC - https://docs.databricks.com/en/admin/system-tables/billing.html
-- MAGIC
-- MAGIC - serverless specific: https://docs.databricks.com/en/admin/system-tables/serverless-billing.html
-- MAGIC
-- MAGIC - https://learn.microsoft.com/en-us/azure/databricks/admin/system-tables/serverless-billing (includes Dashboard)
-- MAGIC
-- MAGIC
-- MAGIC # Run
-- MAGIC - This notebook is entirelly core; rec'd running against serverless core warehouse for best experience.
-- MAGIC

-- COMMAND ----------

select 
  u.usage_date, 
  u.sku_name, 
  u.billing_origin_product, 
  u.usage_quantity, 
  u.usage_type,
  u.usage_metadata, 
  u.custom_tags, 
  u.product_features,
  * 
from system.billing.usage u
where u.sku_name like '%SERVERLESS%' 
  and u.product_features.is_serverless -- for serverless only
  -- and u.billing_origin_product IN ("NOTEBOOKS","JOBS","INTERACTIVE")
  and u.identity_metadata.run_as = "juan.lamadrid@databricks.com"
ORDER BY u.usage_date DESC

-- COMMAND ----------

-- Report on DBUs consumed by a particular user
-- https://docs.databricks.com/en/admin/system-tables/serverless-billing.html#report-on-dbus-consumed-by-a-particular-user

SELECT
  usage_metadata.job_id,
  usage_metadata.notebook_id,
  SUM(usage_quantity) as total_dbu
FROM
  system.billing.usage
WHERE
  identity_metadata.run_as = 'juan.lamadrid@databricks.com'
  and billing_origin_product in ('JOBS','INTERACTIVE')
  and product_features.is_serverless -- SERVERLESS
  and usage_unit = 'DBU'
  and usage_date >= DATEADD(day, -30, current_date)
GROUP BY
  1,2
ORDER BY
  total_dbu DESC


-- COMMAND ----------

SELECT
    we.workspace_id,
    we.warehouse_id,
    we.event_time,
    TIMESTAMPDIFF(MINUTE, we.event_time, CURRENT_TIMESTAMP()) / 60.0 AS running_hours,
    we.cluster_count
FROM
    system.compute.warehouse_events we
WHERE
    we.event_type = 'RUNNING'
    AND NOT EXISTS (
        SELECT 1
        FROM compute.warehouse_events we2
        WHERE we2.warehouse_id = we.warehouse_id
        AND we2.event_time > we.event_time
    )

-- COMMAND ----------


