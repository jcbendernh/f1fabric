# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "6bf4b81b-355e-44e1-88b4-cce27a9c8e44",
# META       "default_lakehouse_name": "f1",
# META       "default_lakehouse_workspace_id": "bf299896-4b8a-4eb1-8d24-aa6495f01734",
# META       "known_lakehouses": [
# META         {
# META           "id": "6bf4b81b-355e-44e1-88b4-cce27a9c8e44"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Bronze to Silver - All Tables
# This notebook transforms the .csv data in bronze volume to Delta tables in the silverdb 

# MARKDOWN ********************

# - Set the bronze variable path to match your bronze file path. 
# - Set the silver variable path to match your LAKEHOUSE.SCHEMA for your silver environment.

# CELL ********************

bronze_file_path = "Files/bronze/"
silver_catalog_schema = "f1.silver"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Circuits

# CELL ********************

volume_file_path = bronze_file_path + "circuits.csv"

df_circuits = (
    spark.read
    .format("csv")
    .option("header", True)
    .option("inferSchema", True)
    .load(volume_file_path)
)

display(df_circuits)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, when, lower, trim

country_variants = [
    "usa", "united states", "u.s.", "u.s.a", "us", "united states of america"
]

df_circuits = df_circuits.withColumn(
    "country",
    when(
        lower(trim(col("country"))).isin(country_variants), "USA"
    ).otherwise(col("country"))
)

display(df_circuits)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_circuits.write.format("delta").mode("overwrite").saveAsTable(silver_catalog_schema + ".circuits")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Constructor Results

# CELL ********************

volume_file_path = bronze_file_path + "constructor_results.csv"

df_constructor_results = (
    spark.read
    .format("csv")
    .option("header", True)
    .option("inferSchema", True)
    .load(volume_file_path)
)

display(df_constructor_results)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_constructor_results.write.format("delta").mode("overwrite").saveAsTable(silver_catalog_schema + ".constructor_results")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Constructor Standings

# CELL ********************

volume_file_path = bronze_file_path + "constructor_standings.csv"

df_constructor_standings = (
    spark.read
    .format("csv")
    .option("header", True)
    .option("inferSchema", True)
    .load(volume_file_path)
)

display(df_constructor_standings)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_constructor_standings.write.format("delta").mode("overwrite").saveAsTable(silver_catalog_schema + ".constructor_standings")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Constructors

# CELL ********************

volume_file_path = bronze_file_path + "constructors.csv"

df_constructors = (
    spark.read
    .format("csv")
    .option("header", True)
    .option("inferSchema", True)
    .load(volume_file_path)
)

display(df_constructors)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_constructors.write.format("delta").mode("overwrite").saveAsTable(silver_catalog_schema + ".constructors")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Driver Standings

# CELL ********************

volume_file_path = bronze_file_path + "driver_standings.csv"

df_driver_standings = (
    spark.read.format("csv")
    .option("header", True)
    .option("inferSchema", True)
    .load(volume_file_path)
)

display(df_driver_standings)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_driver_standings.write.format("delta").mode("overwrite").saveAsTable(silver_catalog_schema + ".driver_standings")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Drivers

# CELL ********************

volume_file_path = bronze_file_path + "drivers.csv"

df_drivers = (
    spark.read.format("csv")
    .option("header", True)
    .option("inferSchema", True)
    .load(volume_file_path)
)

display(df_drivers)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_drivers.write.format("delta").mode("overwrite").saveAsTable(silver_catalog_schema + ".drivers")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Lap Times

# CELL ********************

volume_file_path = bronze_file_path + "lap_times.csv"

df_lap_times = (
    spark.read.format("csv")
    .option("header", True)
    .option("inferSchema", True)
    .load(volume_file_path)
)

display(df_lap_times)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_lap_times.write.format("delta").mode("overwrite").saveAsTable(silver_catalog_schema + ".lap_times")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Pit Stops

# CELL ********************

volume_file_path = bronze_file_path + "pit_stops.csv"

df_pit_stops = (
    spark.read.format("csv")
    .option("header", True)
    .option("inferSchema", True)
    .load(volume_file_path)
)

display(df_pit_stops)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_pit_stops.write.format("delta").mode("overwrite").saveAsTable(silver_catalog_schema + ".pit_stops")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Qualifying

# CELL ********************

volume_file_path = bronze_file_path + "qualifying.csv"

df_qualifying = (
    spark.read.format("csv")
    .option("header", True)
    .option("inferSchema", True)
    .load(volume_file_path)
)

display(df_qualifying)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_qualifying.write.format("delta").mode("overwrite").saveAsTable(silver_catalog_schema + ".qualifying")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Races

# CELL ********************

volume_file_path = bronze_file_path + "races.csv"

df_races = (
    spark.read.format("csv")
    .option("header", True)
    .option("inferSchema", True)
    .load(volume_file_path)
)

display(df_races)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_races.write.format("delta").mode("overwrite").saveAsTable(silver_catalog_schema + ".races")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Results

# CELL ********************

volume_file_path = bronze_file_path + "results.csv"

df_results = (
    spark.read.format("csv")
    .option("header", True)
    .option("inferSchema", True)
    .option("nullValue", "\\N")
    .load(volume_file_path)
)

from pyspark.sql.functions import col

df_results = df_results.withColumn("grid", col("grid").cast("integer"))

display(df_results)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_results.write.format("delta").mode("overwrite").saveAsTable(silver_catalog_schema + ".results")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seasons

# CELL ********************

volume_file_path = bronze_file_path + "seasons.csv"

df_seasons = (
    spark.read.format("csv")
    .option("header", True)
    .option("inferSchema", True)
    .load(volume_file_path)
)

display(df_seasons)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_seasons.write.format("delta").mode("overwrite").saveAsTable(silver_catalog_schema + ".seasons")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Sprint Results

# CELL ********************

volume_file_path = bronze_file_path + "sprint_results.csv"

df_sprint_results = (
    spark.read.format("csv")
    .option("header", True)
    .option("inferSchema", True)
    .load(volume_file_path)
)

display(df_sprint_results)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_sprint_results.write.format("delta").mode("overwrite").saveAsTable(silver_catalog_schema + ".sprint_results")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Status

# CELL ********************

volume_file_path = bronze_file_path + "status.csv"

df_status = (
    spark.read.format("csv")
    .option("header", True)
    .option("inferSchema", True)
    .load(volume_file_path)
)

display(df_status)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_status.write.format("delta").mode("overwrite").saveAsTable(silver_catalog_schema + ".status")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
