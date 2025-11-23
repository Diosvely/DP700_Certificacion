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
# META         },
# META         {
# META           "id": "02784cce-d0c3-4ad4-a4d6-ca81a0d8105e"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ## PASO 1_A Leemos los ficheros spark que estan en la carpeta humanresuorces y los materializamos en dataframes(materializar parquet en tablas) 

# CELL ********************

# Leer los ficheros parquet que son el resultado de la importacion anterior 
# lo que esta en rojo ruta relativa de spark que se copia de aqui al lado en file del lakehause
df_department = spark.read.parquet("Files/humanresouces/department")

df_employeedpthis = spark.read.parquet("Files/humanresouces/employeedepartmenthistory")

df_employeepayhistory = spark.read.parquet("Files/humanresouces/employeepayhistory")

df_jobcandidate = spark.read.parquet("Files/humanresouces/jobcandidate")

display( df_department)
display( df_employeedpthis)
display( df_employeepayhistory)
display( df_jobcandidate)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Guardar los dadtaframes como tables (los materialiaz en tablas) 
df_department.write.format("delta").mode("overwrite").saveAsTable("lh_bronce_landing.hr_department")
df_employeedpthis.write.format("delta").mode("overwrite").saveAsTable("lh_bronce_landing.hr_employeedepartmenthistory")
df_employeepayhistory.write.format("delta").mode("overwrite").saveAsTable("lh_bronce_landing.hr_employeepayhistory")
df_jobcandidate.write.format("delta").mode("overwrite").saveAsTable("lh_bronce_landing.hr_jobcandidate")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## PASO1_B la tabla shift que es mas compleja porque tiene un formato de fechahora y spark no lo lee spark por eso hay que implementar este codigo para pasarla de archivo tabla

# CELL ********************

# No podemos utilizar este codigo igual que arriba porque da un error por el tipo de formato fechahora por ende hay que hacerlo con python y la libreria de pandas
#df_shift = spark.read.parquet("Files/humanresouces/shift") 

# Paso 1 lee y limpia 

import pandas as pd

from pyspark.sql import functions as F

# hay que copiar la ruta completa de api de archivo
df_shift_pandas = pd.read_parquet("/lakehouse/default/Files/humanresouces/shift")
# Eliminar una columna del datafreame
df_shift_pandas = df_shift_pandas.drop(columns=["modifieddate"])


# Paso 2 Normaliza tipo : time -> string esto pasa a texto con ese formato
# para esto utilizamos un ciclo 
for c in ["starttime","endtime"]:
    df_shift_pandas[c] = df_shift_pandas[c].astype(str)  # "HH:MM:SS"

# Paso3 pasar de pandas a Spark 
df_shift = spark.createDataFrame(df_shift_pandas)

# Paso4 Cast finales 

df_shift = (df_shift
  .withColumn("shiftid", F.col("shiftid").cast("int"))
  .withColumn("name",  F.col("name").cast("string"))
  .withColumn("starttime", F.col("starttime").cast("string"))
  .withColumn("endtime", F.col("endtime").cast("string"))

)

# 5) Guarda como tabla Delta en Bronze

spark.sql("CREATE SCHEMA IF NOT EXISTS lh_bronce_landing")

(df_shift.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("lh_bronce_landing.hr_shift")
)

# Verificación que se guardo

spark.sql("SELECT * FROM lh_bronce_landing.hr_shift").show()



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## PASO 2 Creación de tablas de hechos y Dimenciones para guardarlas en SILVER

# CELL ********************

# Crear tabla de Hechos
# Este codigo SQL lo puedo crear en el Servidor normal y pasar esta consulta aqui 
df_ft_costos_rrhh = spark.sql(
"""
SELECT
edh.businessentityid,
  edh.departmentid,
  edh.shiftid,
  eph.ratechangedate,
  eph.rate,
  eph.payfrequency,
  CASE WHEN eph.payfrequency = 1 THEN eph.rate / 30   -- Mensual
      WHEN eph.payfrequency = 2 THEN eph.rate / 15   -- Quincenal
    ELSE eph.rate
  END AS costo_dia
FROM lh_bronce_landing.hr_employeedepartmenthistory edh
LEFT JOIN lh_bronce_landing.hr_employeepayhistory eph
ON edh.businessentityid = eph.businessentityid
ORDER BY
edh.businessentityid,
eph.ratechangedate;

"""
)

display(df_ft_costos_rrhh)

###########

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Guardar la tabla de hechos

df_ft_costos_rrhh.write.format("delta").mode("overwrite").saveAsTable("Lh_Silver_refined.ft_costos_rrhh_dia_turno_depto_business_entity")

############

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # PASO 2 Dimensiones

# CELL ********************

#/* Dimension Trabajadores */

df_dim_trabajador = spark.sql(

"""

SELECT

  businessentityid,

  MIN(startdate) AS fechainicio,

  MAX(enddate) AS fechatermino

FROM lh_bronce_landing.hr_employeedepartmenthistory

GROUP BY businessentityid

"""

)

display(df_dim_trabajador)

##########

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#creamos la tabla de dimension departamento

df_dim_departamento = spark.sql(

"""

SELECT

  departmentid,

  name AS departmentname,

  groupname

FROM lh_bronce_landing.hr_department;

"""

)

display(df_dim_departamento)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************


# CELL ********************

#Creamos Dimension Turno

df_dim_turno = spark.sql(

"""

SELECT
  shiftid,
  name AS shiftname,
  starttime,
  endtime
FROM lh_bronce_landing.hr_shift;

"""

)

display(df_dim_turno)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Guardar en la tabla las dimensiones

# CELL ********************

# Guardamos las 3 dimensiones en las tablas capa siver

df_dim_departamento.write.mode("overwrite").saveAsTable("lh_Silver_refined.dim_departamento")
df_dim_trabajador.write.mode("overwrite").saveAsTable("lh_Silver_refined.dim_trabajador")
df_dim_turno.write.mode("overwrite").saveAsTable("lh_Silver_refined.dim_turno")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Notas Aparte Mias 

# CELL ********************

# Codigo para leer una tabla para saber que tiene 
df_revisar = spark.read.table("lh_bronce_landing.hr_department")
df_revisar   # para saber el tipo de datos 
display(df_revisar) # para ver los datos

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
