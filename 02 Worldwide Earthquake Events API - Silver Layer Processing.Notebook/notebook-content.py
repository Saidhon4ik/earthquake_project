# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "2b8ac475-1859-43af-b4ff-4e4a7ee3cd24",
# META       "default_lakehouse_name": "earthquakes_lakehouse",
# META       "default_lakehouse_workspace_id": "15c4e734-f99e-4278-b77e-7df37270f462",
# META       "known_lakehouses": [
# META         {
# META           "id": "2b8ac475-1859-43af-b4ff-4e4a7ee3cd24"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "df4aabcc-4e27-9732-40bb-d433af724d68",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Worldwide Earthquake Events API - Silver Layer Processing

# CELL ********************

from pyspark.sql.functions import col
from pyspark.sql.types import TimestampType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.option("multiline", "true").json(
    "Files/*_earthquake_data.json"
)

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = df.select(
    'id',
    col('geometry.coordinates').getItem(0).alias('longitude'),
    col('geometry.coordinates').getItem(1).alias('latitude'),
    col('geometry.coordinates').getItem(2).alias('elevation'),
    col('properties.title').alias('title'),
    col('properties.place').alias('place_description'),
    col('properties.sig').alias('sig'),
    col('properties.mag').alias('mag'),
    col('properties.magType').alias('magType'),
    col('properties.time').alias('time'),
    col('properties.updated').alias('updated')
)

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = (
    df
    .withColumn('time', col('time') / 1000)
    .withColumn('updated', col('updated') / 1000)
    .withColumn('time', col('time').cast(TimestampType()))
    .withColumn('updated', col('updated').cast(TimestampType()))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.write.mode('overwrite').saveAsTable(
    'earthquake_events_silver'
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_df = spark.table('earthquake_events_silver')

print("Total rows:", silver_df.count())
print("Unique IDs:", silver_df.select("id").distinct().count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import sum, when

silver_df.select(
    *[
        sum(when(col(c).isNull(), 1).otherwise(0)).alias(c)
        for c in silver_df.columns
    ]
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
