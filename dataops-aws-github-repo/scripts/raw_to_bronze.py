"""
Glue Job: Raw -> Bronze
Demo DataOps AWS

Objetivo:
- Ler arquivos CSV da camada raw
- Padronizar nomes de colunas
- Adicionar metadados operacionais
- Gravar em Parquet na camada bronze

Parâmetros esperados:
--DATA_LAKE_BUCKET
--DATA_DATE
--ENVIRONMENT
"""

import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F


args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "DATA_LAKE_BUCKET", "DATA_DATE", "ENVIRONMENT"]
)

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

bucket = args["DATA_LAKE_BUCKET"]
data_date = args["DATA_DATE"]
env = args["ENVIRONMENT"]

input_path = f"s3://{bucket}/raw/vendas/data_ref={data_date}/"
output_path = f"s3://{bucket}/bronze/vendas/data_ref={data_date}/"

df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(input_path)
)

df_bronze = (
    df
    .withColumn("data_ref", F.lit(data_date))
    .withColumn("environment", F.lit(env))
    .withColumn("ingestion_ts", F.current_timestamp())
    .withColumn("source_system", F.lit("pdv-demo"))
)

(
    df_bronze.write
    .mode("overwrite")
    .parquet(output_path)
)

job.commit()
