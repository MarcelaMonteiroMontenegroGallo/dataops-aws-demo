# DataOps na AWS — Demo com CI/CD

Este repositório demonstra uma arquitetura de referência para DataOps na AWS usando:

- Amazon S3 com padrão Medallion
- AWS Glue Jobs
- AWS Glue Data Quality com DQDL
- AWS Step Functions com `Choice` e `Catch`
- AWS Lambda para gates de validação
- Amazon SNS para alertas
- Amazon EventBridge para agendamento
- Amazon CloudWatch Dashboard
- AWS CodePipeline + CodeBuild para CI/CD opcional
- CloudFormation como IaC

## Estrutura

```text
.
├── templates/
│   └── dataops-aws-demo-cloudformation.yaml
├── scripts/
│   ├── raw_to_bronze.py
│   ├── bronze_to_silver.py
│   └── silver_to_gold.py
├── tests/
│   └── test_jobs.py
├── sample-data/
│   └── vendas/data_ref=2026-05-09/vendas.csv
├── .github/
│   └── workflows/validate.yml
├── buildspec.yml
├── .gitignore
└── README.md
```

## O que essa demo mostra

A demo representa um pipeline DataOps com:

1. Raw imutável no S3.
2. Promoção para Bronze.
3. Transformação para Silver.
4. Regras de qualidade com DQDL.
5. Promoção para Gold.
6. Business Gate para validar comportamento de KPI.
7. Alertas operacionais via SNS.
8. CI/CD para versionar e promover mudanças.

## Deploy da stack runtime

```bash
aws cloudformation deploy \
  --template-file templates/dataops-aws-demo-cloudformation.yaml \
  --stack-name energeticos-dataops-dev \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides \
    ProjectName=energeticos-dataops \
    Environment=dev \
    CreateCICD=false
```

## Upload dos scripts Glue

Depois da stack criada, pegue o output `ArtifactBucketName` e envie os scripts:

```bash
ARTIFACT_BUCKET=<artifact-bucket-name>

aws s3 cp scripts/raw_to_bronze.py s3://$ARTIFACT_BUCKET/scripts/raw_to_bronze.py
aws s3 cp scripts/bronze_to_silver.py s3://$ARTIFACT_BUCKET/scripts/bronze_to_silver.py
aws s3 cp scripts/silver_to_gold.py s3://$ARTIFACT_BUCKET/scripts/silver_to_gold.py
```

## Upload dos dados de exemplo

Pegue o output `DataLakeBucketName` e envie o CSV:

```bash
DATA_LAKE_BUCKET=<data-lake-bucket-name>

aws s3 cp sample-data/vendas/data_ref=2026-05-09/vendas.csv \
  s3://$DATA_LAKE_BUCKET/raw/vendas/data_ref=2026-05-09/vendas.csv
```

## Executar a Step Function

Use este input:

```json
{
  "data_date": "2026-05-09",
  "records": 1000,
  "revenue_delta_pct": 0
}
```

Para simular falha no Profile Gate:

```json
{
  "data_date": "2026-05-09",
  "records": 10,
  "revenue_delta_pct": 0
}
```

Para simular alerta de negócio:

```json
{
  "data_date": "2026-05-09",
  "records": 1000,
  "revenue_delta_pct": -45
}
```

## CI/CD opcional com CodePipeline

Para ativar o CI/CD, crie antes uma conexão GitHub em:

```text
AWS Console > Developer Tools > Connections
```

Depois faça deploy com:

```bash
aws cloudformation deploy \
  --template-file templates/dataops-aws-demo-cloudformation.yaml \
  --stack-name energeticos-dataops-cicd \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides \
    ProjectName=energeticos-dataops \
    Environment=dev \
    CreateCICD=true \
    GitHubConnectionArn=<ARN_DA_CONNECTION> \
    FullRepositoryId=sua-org/seu-repo \
    BranchName=main
```

## Validação local

```bash
pip install cfn-lint pytest
cfn-lint templates/*.yaml
pytest -q
```

## Observação

O template é demonstrativo e prioriza didática. Para produção, recomenda-se:

- separar contas por ambiente;
- restringir IAM com menor privilégio;
- adicionar KMS nos buckets;
- gravar incidentes em DynamoDB;
- usar aprovação manual entre ambientes;
- adicionar validação com Checkov/cfn-nag;
- implementar execução real do Glue Data Quality Evaluation Run no workflow.
