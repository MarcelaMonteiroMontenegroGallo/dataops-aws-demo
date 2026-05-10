"""
Glue Job: Bronze -> Silver
Demo DataOps AWS

Objetivo:
- Ler dados bronze
- Aplicar tipagem e padronizações
- Criar dataset silver pronto para regras DQDL
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

input_path = f"s3://{bucket}/bronze/vendas/data_ref={data_date}/"
output_path = f"s3://{bucket}/silver/vendas/data_ref={data_date}/"

df = spark.read.parquet(input_path)

df_silver = (
    df
    .withColumn("id_transacao", F.col("id_transacao").cast("string"))
    .withColumn("id_loja", F.col("id_loja").cast("string"))
    .withColumn("canal", F.upper(F.trim(F.col("canal").cast("string"))))
    .withColumn("valor_total", F.regexp_replace(F.col("valor_total").cast("string"), ",", ".").cast("double"))
    .withColumn("qtd_itens", F.col("qtd_itens").cast("int"))
    .withColumn("ncm", F.col("ncm").cast("string"))
    .withColumn("cnpj_loja", F.regexp_replace(F.col("cnpj_loja").cast("string"), "[^0-9]", ""))
    .withColumn("data_venda", F.col("data_venda").cast("string"))
    .withColumn("silver_processed_ts", F.current_timestamp())
    .select(
        "id_transacao",
        "id_loja",
        "canal",
        "valor_total",
        "qtd_itens",
        "ncm",
        "cnpj_loja",
        "data_venda",
        "silver_processed_ts"
    )
)

(
    df_silver.write
    .mode("overwrite")
    .parquet(output_path)
)

job.commit()
