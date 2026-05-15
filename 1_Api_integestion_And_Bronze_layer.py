# Databricks notebook source
import requests
import json
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd

# COMMAND ----------

spark.sql("CREATE CATALOG IF NOT EXISTS workspace")
spark.sql("Create schema if not exists workspace.default")
spark.sql("CREATE VOLUME if not exists workspace.default.cricket_api_project")

base_path = "/Volumes/workspace/default/cricket_api_project"



# COMMAND ----------

API_KEY = '7289d520-0f43-4ad2-aa58-0b25e5f9d9ea'
# Cricket API endpoint
api_url = f"https://api.cricapi.com/v1/currentMatches?apikey={API_KEY}&offset=0"

response = requests.get(api_url)
response.raise_for_status()
api_data = response.json()
print(api_data.keys())

print(json.dumps(api_data, indent=4)[:2000])



# COMMAND ----------

raw_file_path = f"{base_path}/current_matches_raw.json"

with open(raw_file_path, "w") as file:
    json.dump(api_data, file, indent=4)

print("RAW API data is saved at:", raw_file_path)

# COMMAND ----------

bronze_data = {
    "source_api": api_url,
    "raw_json": json.dumps(api_data),
    "ingestion_time": None
}



bronze_schema = StructType([
    StructField("source_api", StringType(), True),
    StructField("raw_json", StringType(), True),
    StructField("ingestion_time", TimestampType(), True)
])

bronze_df = spark.createDataFrame([bronze_data], schema = bronze_schema) \
    .withColumn("ingestion_time", current_timestamp())

display(bronze_df)

# COMMAND ----------

bronze_df.write\
    .format('delta')\
    .mode('overwrite')\
    .saveAsTable("workspace.default.cricket_bronze_current_matches")

print("BRONZE TABLE CREATED SUCCESSFULLY")

# COMMAND ----------


