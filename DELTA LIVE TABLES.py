# Databricks notebook source
import dlt
from pyspark.sql.functions import upper, concat_ws, lit
from pyspark.sql.types import StructType, IntegerType, StringType


# COMMAND ----------

# 1. Define the schema manually
employee_schema = StructType() \
    .add("empid", IntegerType()) \
    .add("empname", StringType()) \
    .add("country", StringType()) \
    .add("designation", StringType()) \
    .add("dept", StringType())

# 2. Create a raw DLT table from streaming data
@dlt.table(
  name="raw_employee_data",
  comment="Raw employee records loaded from JSON files."
)
def load_raw_employee_data():
    return (spark.readStream
            .format("cloudFiles")  # Auto Loader inside DLT
            .option("cloudFiles.format", "json")
            .schema(employee_schema)
            .load("/mnt/input-data/streaming-data/")
    )


# COMMAND ----------

@dlt.table(
  name="cleaned_employee_data",
  comment="Cleaned employee records with transformations applied."
)
def transform_cleaned_employee_data():
    return (dlt.read_stream("raw_employee_data")
            .withColumn("full_info", concat_ws(" - ", "empname", "designation"))
            .withColumn("country_upper", upper("country"))
            .withColumn("source", lit("dlt_streaming"))
            .filter("empid IS NOT NULL AND empname IS NOT NULL")
    )
