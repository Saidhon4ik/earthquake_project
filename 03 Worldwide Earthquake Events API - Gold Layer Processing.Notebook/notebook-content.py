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

# # Worldwide Earthquake Events API - Gold Layer Processing

# CELL ********************

import reverse_geocoder as rg

from pyspark.sql.functions import when, col, udf
from pyspark.sql.types import StringType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.table("earthquake_events_silver")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

coordinates = (39.3576, 73.2495)
rg.search(coordinates)[0].get('cc')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_country_code(lat, lon):
    coordinates = (float(lat), float(lon))
    return rg.search(coordinates)[0].get('cc')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

get_country_code_udf = udf(
    get_country_code, StringType()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_with_location = df.withColumn(
    "country_code",
    get_country_code_udf(col("latitude"), col("longitude"))
)

display(df_with_location)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_with_location_sig_class = df_with_location.withColumn(
    "sig_class",
    when(col("sig") < 100, "Low")
    .when((col("sig") >= 100) & (col("sig") < 500), "Moderate")
    .otherwise("High")
)

display(df_with_location_sig_class)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_with_location_sig_class.groupBy("sig_class").count().show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Total rows:", df_with_location_sig_class.count())
print("Unique IDs:", df_with_location_sig_class.select("id").distinct().count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_with_location_sig_class.write.mode("overwrite").saveAsTable(
    "earthquake_events_gold"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gold_df = spark.read.table("earthquake_events_gold")

print("Gold rows:", gold_df.count())
print("Gold unique IDs:", gold_df.select("id").distinct().count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
