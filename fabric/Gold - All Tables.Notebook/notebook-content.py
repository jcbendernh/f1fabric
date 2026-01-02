# Fabric notebook source


# MARKDOWN ********************

# # Silver to Gold - All Tables
# This notebook curates the data in the silver table to the gold layer 

# MARKDOWN ********************

# Set the variable paths to match your CATALOG.SCHEMA for both your silver and gold environment.

# CELL ********************

silver_catalog_schema = "catadb360dev.f1silver"
gold_catalog_schema = "catadb360dev.f1gold"

# MARKDOWN ********************

# ## Circuits Table

# CELL ********************

df_circuits = spark.table(silver_catalog_schema + ".circuits")
df_circuits = df_circuits.drop("alt", "url", "circuitRef")
display(df_circuits)

# CELL ********************

df_circuits.write.mode("overwrite").saveAsTable(gold_catalog_schema + ".circuits")

# MARKDOWN ********************

# ## Constructor Results Table

# CELL ********************

from pyspark.sql.functions import col

df_constructor_results = spark.table(silver_catalog_schema + ".constructor_results")
df_races = spark.table(silver_catalog_schema + ".races").select("raceId", col("name").alias("race"), "year")
df_constructors = spark.table(silver_catalog_schema + ".constructors").select("constructorId", col("name").alias("constructor"))
df_constructor_results = df_constructor_results.join(df_races, on="raceId", how="left")
df_constructor_results = df_constructor_results.join(df_constructors, on="constructorId", how="left")
df_constructor_results = df_constructor_results.drop("raceId", "constructorId", "status")
display(df_constructor_results)

# CELL ********************

df_constructor_results.write.mode("overwrite").saveAsTable(gold_catalog_schema + ".constructor_results")

# MARKDOWN ********************

# ## Constructor Standings

# CELL ********************

df_constructor_standings = spark.table(silver_catalog_schema + ".constructor_standings")
df_races = spark.table(silver_catalog_schema + ".races").select("raceId", col("name").alias("race"), "year", "date")
df_constructors = spark.table(silver_catalog_schema + ".constructors").select("constructorId", col("name").alias("constructor"))
df_constructor_standings = df_constructor_standings.join(df_races, on="raceId", how="left")
df_constructor_standings = df_constructor_standings.join(df_constructors, on="constructorId", how="left")
df_constructor_standings = df_constructor_standings.drop("raceId", "constructorId", "positionText")
display(df_constructor_standings)

# CELL ********************

df_constructor_standings.write.mode("overwrite").saveAsTable(gold_catalog_schema + ".constructor_standings")

# MARKDOWN ********************

# ## Constructors

# CELL ********************

df_constructors = spark.table(silver_catalog_schema + ".constructors")
df_constructors = df_constructors.drop("constructorRef", "url")
display(df_constructors)

# CELL ********************

df_constructors.write.mode("overwrite").saveAsTable(gold_catalog_schema + ".constructors")

# MARKDOWN ********************

# ## Driver Standings

# CELL ********************

from pyspark.sql.functions import col, concat, lit

df_driver_standings = spark.table(silver_catalog_schema + ".driver_standings")
df_races = spark.table(silver_catalog_schema + ".races").select(
    "raceId",
    col("name").alias("race"),
    "year",
    "date"
)
df_drivers = spark.table(silver_catalog_schema + ".drivers").select(
    "driverId",
    concat(col("forename"), lit(" "), col("surname")).alias("driver")
)
df_driver_standings = df_driver_standings.join(df_races, on="raceId", how="left")
df_driver_standings = df_driver_standings.join(df_drivers, on="driverId", how="left")
df_driver_standings = df_driver_standings.drop("raceId", "driverId", "positionText")
display(df_driver_standings)

# CELL ********************

df_driver_standings.write.mode("overwrite").saveAsTable(gold_catalog_schema + ".driver_standings")

# MARKDOWN ********************

# ## Drivers

# CELL ********************

from pyspark.sql.functions import concat, col, lit

df_drivers = spark.table(silver_catalog_schema + ".drivers")
df_drivers = df_drivers.withColumn("driver", concat(col("forename"), lit(" "), col("surname")))
df_drivers = df_drivers.drop("code", "url", "driverRef", "forename", "surname")
display(df_drivers)

# CELL ********************

df_drivers.write.mode("overwrite").saveAsTable(gold_catalog_schema + ".drivers")

# MARKDOWN ********************

# ## Lap Times

# CELL ********************

df_lap_times = spark.table(silver_catalog_schema + ".lap_times")
df_races = spark.table(silver_catalog_schema + ".races").select("raceId", col("name").alias("race"), "year", "date")
df_drivers = spark.table(silver_catalog_schema + ".drivers").select(
    "driverId",
    concat(col("forename"), lit(" "), col("surname")).alias("driver")
)
df_lap_times = df_lap_times.join(df_races, on="raceId", how="left")
df_lap_times = df_lap_times.join(df_drivers, on="driverId", how="left")
df_lap_times = df_lap_times.withColumn("seconds", col("milliseconds") / 1000)
df_lap_times = df_lap_times.drop("driverId", "raceId", "driverRef", "forename", "surname", "time", "milliseconds")
display(df_lap_times)

# CELL ********************

df_lap_times.write.mode("overwrite").saveAsTable(gold_catalog_schema + ".lap_times")

# MARKDOWN ********************

# ## Pit Stops

# CELL ********************

from pyspark.sql.functions import col, concat, lit, expr

df_pit_stops = spark.table(silver_catalog_schema + ".pit_stops")
df_races = spark.table(silver_catalog_schema + ".races").select(
    "raceId",
    col("name").alias("race"),
    "year"
)
df_drivers = spark.table(silver_catalog_schema + ".drivers").select(
    "driverId",
    concat(col("forename"), lit(" "), col("surname")).alias("driver")
)
df_pit_stops = df_pit_stops.join(df_races, on="raceId", how="left")
df_pit_stops = df_pit_stops.join(df_drivers, on="driverId", how="left")

df_pit_stops = df_pit_stops.withColumn("seconds", col("milliseconds") / 1000)
df_pit_stops = df_pit_stops.drop("driverId", "raceId", "milliseconds", "duration")
display(df_pit_stops)

# CELL ********************

df_pit_stops.write.mode("overwrite").saveAsTable(gold_catalog_schema + ".pit_stops")

# MARKDOWN ********************

# ## Qualifying

# CELL ********************

from pyspark.sql.functions import col, concat, lit

df_qualifying = spark.table(silver_catalog_schema + ".qualifying")
df_races = spark.table(silver_catalog_schema + ".races").select(
    "raceId",
    col("name").alias("race"),
    "year"
)
df_drivers = spark.table(silver_catalog_schema + ".drivers").select(
    "driverId",
    concat(col("forename"), lit(" "), col("surname")).alias("driver")
)
df_constructors = spark.table(silver_catalog_schema + ".constructors").select("constructorId", col("name"))
df_qualifying = df_qualifying.join(df_races, on="raceId", how="left")
df_qualifying = df_qualifying.join(df_drivers, on="driverId", how="left")
df_qualifying = df_qualifying.join(df_constructors, on="constructorId", how="left")

df_qualifying = df_qualifying.withColumnRenamed("name", "constructor")
df_qualifying = df_qualifying.drop("constructorId","driverId", "raceId", "q1", "q2", "q3")
display(df_qualifying)

# CELL ********************

df_qualifying.write.mode("overwrite").saveAsTable(gold_catalog_schema + ".qualifying")

# MARKDOWN ********************

# ## Races

# CELL ********************

from pyspark.sql.functions import col, concat, lit

df_races = spark.table(silver_catalog_schema + ".races")
df_circuits = spark.table(silver_catalog_schema + ".circuits").select("circuitId", col("name").alias("circuit"),"country")
df_races = df_races.join(df_circuits, on="circuitId", how="left")

df_races = df_races.drop("circuitId", "raceId", "url", "fp1_date", "fp1_time", "fp2_date", "fp2_time", "fp3_date", "fp3_time", "quali_date", "quali_time", "sprint_date", "sprint_time")

display(df_races)

# CELL ********************

df_races.write.mode("overwrite").saveAsTable(gold_catalog_schema + ".races")

# MARKDOWN ********************

# ## Results

# CELL ********************

from pyspark.sql.functions import col, concat, lit

df_results = spark.table(silver_catalog_schema + ".results")
df_races = spark.table(silver_catalog_schema + ".races").select(
    "raceId",
    col("name").alias("race"),
    "year"
)
df_drivers = spark.table(silver_catalog_schema + ".drivers").select(
    "driverId",
    concat(col("forename"), lit(" "), col("surname")).alias("driver")
)
df_constructors = spark.table(silver_catalog_schema + ".constructors").select("constructorId", col("name").alias("constructorName"))
df_status = spark.table(silver_catalog_schema + ".status").select("statusId", "status")

df_results = df_results.join(df_races, on="raceId", how="left")
df_results = df_results.join(df_drivers, on="driverId", how="left")
df_results = df_results.join(df_constructors, on="constructorId", how="left")
df_results = df_results.join(df_status, on="statusId", how="left")

df_results = df_results.drop("constructorId","driverId","raceId","positionText","statusId")

display(df_results)

# CELL ********************

df_results.write.mode("overwrite").saveAsTable(gold_catalog_schema + ".results")

# MARKDOWN ********************

# ## Seasons

# CELL ********************

df_seasons = spark.table(silver_catalog_schema + ".seasons")
df_seasons = df_seasons.drop("url")

display(df_seasons)

# CELL ********************

df_seasons.write.mode("overwrite").saveAsTable(gold_catalog_schema + ".seasons")

# MARKDOWN ********************

# ## Sprint Results

# CELL ********************

df_sprint_results = spark.table(silver_catalog_schema + ".sprint_results")
df_races = spark.table(silver_catalog_schema + ".races").select(
    "raceId",
    col("name").alias("race"),
    "year"
)
df_drivers = spark.table(silver_catalog_schema + ".drivers").select(
    "driverId",
    concat(col("forename"), lit(" "), col("surname")).alias("driver")
)
df_constructors = spark.table(silver_catalog_schema + ".constructors").select("constructorId", col("name").alias("constructorName"))
df_status = spark.table(silver_catalog_schema + ".status").select("statusId", "status")

df_sprint_results = df_sprint_results.join(df_races, on="raceId", how="left")
df_sprint_results = df_sprint_results.join(df_drivers, on="driverId", how="left")
df_sprint_results = df_sprint_results.join(df_constructors, on="constructorId", how="left")
df_sprint_results = df_sprint_results.join(df_status, on="statusId", how="left")
df_sprint_results = df_results.drop("constructorId","driverId","raceId","positionText","statusId")

display(df_sprint_results)

# CELL ********************

df_sprint_results.write.mode("overwrite").saveAsTable(gold_catalog_schema + ".sprint_results")

# MARKDOWN ********************

# ## Table Maintenance

# CELL ********************


# CELL ********************

# spark.sql(
#     f"ALTER TABLE {gold_catalog_schema}.constructor_standings SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name')"
# )
# spark.sql(
#     f"ALTER TABLE {gold_catalog_schema}.constructor_standings DROP COLUMNS (positionText)"
# )
