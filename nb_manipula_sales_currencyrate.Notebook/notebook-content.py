# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "ac49af91-a3f5-401f-a1dc-b3f3e1331e04",
# META       "default_lakehouse_name": "lh_bronce_landing",
# META       "default_lakehouse_workspace_id": "c0bfc68c-cd73-4638-8992-79a395feb6c6",
# META       "known_lakehouses": [
# META         {
# META           "id": "ac49af91-a3f5-401f-a1dc-b3f3e1331e04"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ## Manipular el fichero parquet para tranformarlo en tabla

# CELL ********************

 # 1) leer desde pandas el fichero parquet

import pandas as pd
df_cr_pandas = pd.read_parquet("/lakehouse/default/Files/Sales/currencyrate")

df_cr_pandas = df_cr_pandas.drop(columns=["modifieddate"])

# display(df_cr_pandas)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 2) transformar de pandas a spark

from pyspark.sql import functions as F

df_cr = spark.createDataFrame(df_cr_pandas)
# display(df_cr)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#castear el dataframe generar sus tipos de datos 

df_cr = (df_cr
  .withColumn("currencyrateid", F.col("currencyrateid").cast("int"))
  .withColumn("currencyratedate", F.col("currencyratedate").cast("date"))
  .withColumn("fromcurrencycode", F.col("fromcurrencycode").cast("string"))
  .withColumn("tocurrencycode", F.col("tocurrencycode").cast("string"))
  .withColumn("averagerate", F.col("averagerate").cast("double"))
  .withColumn("endofdayrate", F.col("endofdayrate").cast("double"))
)
# display(df_cr)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

 # 3) guardar de spark a tabla en el lakehouse

df_cr.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("lh_bronce_landing.sales_currencyrate")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#verificacion

spark.sql("SELECT * FROM lh_bronce_landing.sales_currencyrate")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
