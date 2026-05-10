"""
Glue Job: Silver -> Gold
Demo DataOps AWS

Objetivo:
- Agregar indicadores de negócio
- Gerar camada gold para consumo analítico
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

input_path = f"s3://{bucket}/silver/vendas/data_ref={data_date}/"
output_path = f"s3://{bucket}/gold/vendas_kpis/data_ref={data_date}/"

df = spark.read.parquet(input_path)

df_gold = (
    df.groupBy("canal")
    .agg(
        F.countDistinct("id_transacao").alias("qtde_transacoes"),
        F.sum("valor_total").alias("receita_total"),
        F.avg("valor_total").alias("ticket_medio"),
        F.sum("qtd_itens").alias("itens_vendidos")
    )
    .withColumn("data_ref", F.lit(data_date))
    .withColumn("gold_processed_ts", F.current_timestamp())
)

(
    df_gold.write
    .mode("overwrite")
    .parquet(output_path)
)

job.commit()
