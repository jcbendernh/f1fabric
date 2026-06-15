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

# ## Notebook Overview
# 
# This notebook performs the **Bronze ➜ Silver** transformation for the Formula 1 dataset.
# 
# - **Input (Bronze):** CSV files stored in the default Lakehouse under `Files/bronze/`.
# - **Output (Silver):** Delta tables written to the Lakehouse schema defined in the `silver_schema` variable (default: `f1.silver`).
# - **Pattern:** For each source CSV, the notebook:
#   - Reads the file from the bronze folder using Spark
#   - Applies light data-quality and type-casting transformations where needed
#   - Writes the result as a managed Delta table in the Silver layer.
# 
# > To repoint the notebook to a different environment, update:
# > - `bronze_file_path` – to match your Bronze files location
# > - `silver_schema` – to match your `<LAKEHOUSE>.<SCHEMA>` target for Silver tables.
# 
# 
# ## Silver Tables Created
# 
# This notebook materializes the following **Silver** Delta tables in the schema defined by `silver_schema` (e.g. `f1.silver`):
# 
# - `circuits` – Circuit reference data with normalized `country` values (e.g., USA variants standardized to `USA`).
# - `constructor_results` – Constructor results per race.
# - `constructor_standings` – Constructor standings with `position` stored as integer.
# - `constructors` – Constructor reference/master data.
# - `driver_standings` – Driver standings per season and race.
# - `drivers` – Driver reference/master data.
# - `lap_times` – Lap times per race, driver, and lap.
# - `pit_stops` – Pit stop events with `milliseconds` cast to integer.
# - `qualifying` – Qualifying session results.
# - `races` – Race calendar and metadata.
# - `results` – Race results with `grid` cast to integer and `"\\N"` treated as nulls.
# - `seasons` – Season reference data.
# - `sprint_results` – Sprint race results with `statusId` cast to integer.
# - `status` – Status lookup table (e.g., finished, accident, etc.).
# 
# Each table is stored in **Delta** format and can be queried via Spark or SQL using the fully qualified name:
# 
# ```sql
# SELECT TOP 10 *
# FROM <lakehouse_name>.silver.<table_name>;
# ```


# MARKDOWN ********************

# - Set the bronze variable path to match your bronze file path. 
# - Set the silver variable path to match your LAKEHOUSE.SCHEMA for your silver environment.

# CELL ********************

bronze_file_path = "Files/bronze/"
silver_schema = "f1.silver"

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

df_circuits.write.format("delta").mode("overwrite").saveAsTable(silver_schema + ".circuits")

spark.sql(f"REFRESH TABLE {silver_schema}.circuits")

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

df_constructor_results.write.format("delta").mode("overwrite").saveAsTable(silver_schema + ".constructor_results")

spark.sql(f"REFRESH TABLE {silver_schema}.constructor_results")

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

from pyspark.sql.functions import col

# Ensure position is an integer field
df_constructor_standings = df_constructor_standings.withColumn("position", col("position").cast("int"))

display(df_constructor_standings)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_constructor_standings.write.format("delta").mode("overwrite").saveAsTable(silver_schema + ".constructor_standings")

spark.sql(f"REFRESH TABLE {silver_schema}.constructor_standings")

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

df_constructors.write.format("delta").mode("overwrite").saveAsTable(silver_schema + ".constructors")

spark.sql(f"REFRESH TABLE {silver_schema}.constructors")

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

df_driver_standings.write.format("delta").mode("overwrite").saveAsTable(silver_schema + ".driver_standings")

spark.sql(f"REFRESH TABLE {silver_schema}.driver_standings")

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

df_drivers.write.format("delta").mode("overwrite").saveAsTable(silver_schema + ".drivers")

spark.sql(f"REFRESH TABLE {silver_schema}.drivers")

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

df_lap_times.write.format("delta").mode("overwrite").saveAsTable(silver_schema + ".lap_times")

spark.sql(f"REFRESH TABLE {silver_schema}.lap_times")

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
    spark.read
    .format("csv")
    .option("header", True)
    .option("inferSchema", True)
    .load(volume_file_path)
)

from pyspark.sql.functions import col

# Cast milliseconds column to integer
df_pit_stops = df_pit_stops.withColumn("milliseconds", col("milliseconds").cast("int"))

display(df_pit_stops)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_pit_stops.write.format("delta").mode("overwrite").saveAsTable(silver_schema + ".pit_stops")

spark.sql(f"REFRESH TABLE {silver_schema}.pit_stops")

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

df_qualifying.write.format("delta").mode("overwrite").saveAsTable(silver_schema + ".qualifying")

spark.sql(f"REFRESH TABLE {silver_schema}.qualifying")

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

df_races.write.format("delta").mode("overwrite").saveAsTable(silver_schema + ".races")

spark.sql(f"REFRESH TABLE {silver_schema}.races")

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

df_results.write.format("delta").mode("overwrite").saveAsTable(silver_schema + ".results")

spark.sql(f"REFRESH TABLE {silver_schema}.results")

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

df_seasons.write.format("delta").mode("overwrite").saveAsTable(silver_schema + ".seasons")

spark.sql(f"REFRESH TABLE {silver_schema}.seasons")

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

from pyspark.sql.functions import col

# Ensure statusId is an integer field
df_sprint_results = df_sprint_results.withColumn("statusId", col("statusId").cast("int"))

display(df_sprint_results)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_sprint_results.write.format("delta").mode("overwrite").saveAsTable(silver_schema + ".sprint_results")

spark.sql(f"REFRESH TABLE {silver_schema}.sprint_results")

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

df_status.write.format("delta").mode("overwrite").saveAsTable(silver_schema + ".status")

spark.sql(f"REFRESH TABLE {silver_schema}.status")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
