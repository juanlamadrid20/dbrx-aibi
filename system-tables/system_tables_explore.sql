-- Databricks notebook source
-- MAGIC %md
-- MAGIC Enable
-- MAGIC -- https://docs.databricks.com/en/administration-guide/system-tables/index.html#enable

-- COMMAND ----------

select distinct sku_name from system.billing.usage

-- COMMAND ----------

select distinct billing_origin_product from system.billing.usage

-- COMMAND ----------

select distinct(u.usage_type)
from system.billing.usage u

-- COMMAND ----------

select distinct(u.record_type)
from system.billing.usage u

-- COMMAND ----------

select distinct(u.usage_unit)
from system.billing.usage u

-- COMMAND ----------

select * from system.pipelines.dlt_updates

-- COMMAND ----------

SELECT
  sku_name,
  usage_date,
  SUM(usage_quantity) AS `DBUs`
FROM
  system.billing.usage
WHERE
  usage_metadata.dlt_pipeline_id = "522c57a1-49bd-41e0-a3fd-8d6b1498bbcd"
GROUP BY
  sku_name, usage_date


-- COMMAND ----------

DESCRIBE EXTENDED juan_dev.data_eng.customer_ingestion;

-- COMMAND ----------


