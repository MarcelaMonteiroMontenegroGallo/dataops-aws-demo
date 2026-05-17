# DataOps na AWS: como aplicar práticas de engenharia ao ciclo de vida dos dados

**Por Marcela Monteiro Montenegro Gallo**
*Arquiteta de Dados e AI | 9x AWS Certified | 2x Databricks Certified*
*Ingram Micro Cloud — AWS Partner*

*Data de publicação: maio de 2026*

---

## Introdução

A Energéticos S/A processa 4 milhões de eventos de telemetria por dia vindos de sensores IoT em suas plantas industriais. Quando o time de dados decidiu migrar para a AWS, o primeiro pipeline funcionou em duas semanas. O segundo levou três. No sexto mês, a empresa tinha 23 pipelines em produção, nenhum catálogo unificado e uma média de 9 incidentes silenciosos por semana. Dados duplicados alimentavam dashboards de manutenção preditiva. Ninguém confiava nos números.

Esse cenário é mais comum do que parece. Organizações escalam pipelines sem escalar as práticas de engenharia que garantem confiabilidade. O resultado é previsível: dívida técnica de dados que cresce exponencialmente. DataOps surge como resposta direta a esse problema. A metodologia aplica automação, qualidade contínua, observabilidade e entrega controlada ao ciclo de vida dos dados, transformando pipelines artesanais em sistemas industrializados.

Este artigo usa o caso da Energéticos S/A como fio condutor para apresentar uma implementação concreta de DataOps na AWS. Vamos percorrer os 4 quality gates, a linguagem DQDL para regras declarativas, o CI/CD de pipelines, a orquestração com Step Functions usando Choice e Catch, a observabilidade em 3 planos e o processo de quarentena. Cada componente foi efetivamente provisionado em uma conta AWS real — as imagens ao longo do artigo são capturas do ambiente em operação. O objetivo é fornecer um guia prático para quem já opera na AWS e quer elevar a maturidade operacional dos seus dados.

A Energéticos S/A é um caso ilustrativo que dá contexto de negócio aos conceitos. O ambiente AWS apresentado nas capturas a seguir é a prova de conceito que implementa e valida cada mecanismo descrito — quality gates, DQDL, CI/CD, observabilidade e quarentena — de forma reproduzível.

## O Caso Energéticos S/A

A Energéticos S/A é uma indústria de bebidas energéticas com 3 plantas no Brasil. Sensores em linhas de produção emitem eventos a cada 5 segundos: temperatura, pressão, velocidade da esteira e volume de envase. Esses dados alimentam modelos de manutenção preditiva e dashboards operacionais em tempo real.

### O problema antes de DataOps

O pipeline original era simples: Kinesis Data Streams capturava os eventos, um Glue Job transformava e gravava no S3, e o Athena servia as consultas. Funcionava até não funcionar. Sensores defeituosos enviavam temperaturas de -999°C. Mudanças de firmware alteravam o schema sem aviso. Jobs falhavam às 3h da manhã e ninguém percebia até o turno da manhã.

O impacto era concreto. Em um incidente, dados de pressão corrompidos passaram despercebidos por 72 horas. O modelo de manutenção preditiva recomendou troca de válvulas que estavam em perfeito estado. Custo: R$ 180 mil em peças desnecessárias e 14 horas de parada de linha.

### A decisão

O time de dados decidiu implementar DataOps com foco em quatro frentes: quality gates automatizados em cada camada do lake, CI/CD para todo código de pipeline, orquestração resiliente com tratamento explícito de falhas e observabilidade que respondesse "o que quebrou, por que e qual o impacto" em menos de 5 minutos.

## Os 4 Quality Gates

Quality gates são pontos de verificação obrigatórios que os dados precisam atravessar antes de avançar para a próxima camada do lake. Na Energéticos S/A, cada gate tem regras específicas, critérios de aprovação e ações de remediação definidas. Pense neles como checkpoints de alfândega: o dado só passa se estiver em conformidade.

A escolha de quatro gates não é arbitrária. Cada gate captura uma classe distinta de problema, e cada classe exige um instrumento diferente. Um único gate "valida tudo" no fim do pipeline é tarde demais — quando o dado ruim chega ao Gold, ele já consumiu processamento e já contaminou tabelas intermediárias. Validar cedo e em camadas é o que torna o custo de um problema proporcional à profundidade em que ele é detectado.

### Gate 1: Ingestão (Raw)

O primeiro gate valida os dados no momento em que chegam ao S3. As verificações são estruturais: o arquivo está no formato esperado? O schema contém os campos obrigatórios? O tamanho do arquivo está dentro da faixa normal?

Regras típicas deste gate:

    - Arquivo deve ser JSON Lines válido
    - Campos obrigatórios: sensor_id, timestamp, metric_type, value
    - Tamanho do arquivo entre 1 KB e 500 MB
    - Timestamp não pode ser futuro (tolerância de 5 minutos)

Se o arquivo falha no Gate 1, ele é movido para a zona de quarentena com metadados do motivo da rejeição. Nenhum processamento downstream acontece.

### Gate 2: Conformidade (Bronze para Silver)

O segundo gate aplica regras de domínio aos dados já parseados. Aqui entram validações de negócio: a temperatura está dentro da faixa operacional? O sensor_id existe no cadastro? A frequência de eventos está dentro do esperado?

Regras típicas deste gate:

    - Temperatura entre -40°C e 150°C
    - Pressão entre 0 e 25 bar
    - sensor_id deve existir na tabela de referência dim_sensores
    - Completude mínima de 95% para campos obrigatórios

Dados que falham no Gate 2 são segregados com flag de não-conformidade. O pipeline continua processando os dados válidos.

### Gate 3: Consistência (Silver para Gold)

O terceiro gate verifica a consistência dos dados agregados. Após as transformações, os números fazem sentido? Existem duplicatas? As métricas derivadas estão dentro de limites estatísticos?

Regras típicas deste gate:

    - Unicidade de chave composta (sensor_id + timestamp + metric_type)
    - Variação máxima de 3 desvios-padrão em relação à média móvel de 7 dias
    - Volume de registros não pode cair mais de 30% em relação ao dia anterior
    - Soma de produção por planta deve ser positiva

### Gate 4: Entrega (Gold para Consumo)

O quarto gate valida os dados antes de disponibilizá-los para consumidores finais: dashboards, modelos de ML e APIs. Este gate verifica freshness (atualidade dos dados), completude de dimensões e integridade referencial.

Regras típicas deste gate:

    - Freshness: dados não podem ter mais de 2 horas de atraso
    - Todas as plantas devem ter dados no período (sem gaps)
    - Integridade referencial com dimensões (produto, planta, turno)
    - Score de qualidade geral acima de 92%

## DQDL: Regras Declarativas de Qualidade

DQDL (Data Quality Definition Language) é a linguagem declarativa do AWS Glue Data Quality para expressar regras de validação. Em vez de escrever código imperativo para cada verificação, você declara o que espera dos dados e o serviço avalia automaticamente. O AWS Glue Data Quality é construído sobre o framework open-source DeeQu, oferecendo uma experiência gerenciada e serverless — sem instalação, patching ou manutenção.

Na Energéticos, as regras DQDL são avaliadas dentro do próprio Glue Job de ETL: a mesma execução que transforma os dados também aplica os quality gates, evitando uma passada extra de leitura. O job roda em Glue 4.0 (Spark 3.3, Python 3) com a opção de geração de insights habilitada, o que permite ao serviço analisar execuções e sugerir otimizações.


<img width="1694" height="572" alt="image" src="https://github.com/user-attachments/assets/98f3bbe6-b566-4f0d-b6de-902460a21d4c" />

*O Glue Job `dataops-demo-dev-etl-job` com Data Quality integrado: transformação e avaliação de qualidade na mesma execução, com role IAM dedicada e job insights ativo.*

### Anatomia de uma regra DQDL

Um documento DQDL é case-sensitive e contém um ruleset — uma lista chamada `Rules` (capitalizada), delimitada por colchetes, com regras separadas por vírgula. Cada regra segue a estrutura: tipo de verificação, coluna alvo e condição esperada. O Glue Data Quality avalia cada regra e retorna um score de qualidade entre 0 e 1, calculado como o percentual de regras que passam. Veja como as regras do Gate 2 da Energéticos ficam em DQDL:

    Rules = [
        Completeness "sensor_id" >= 0.99,
        Completeness "timestamp" >= 0.99,
        Completeness "value" >= 0.95,
        ColumnValues "temperature" between -40 and 150,
        ColumnValues "pressure" between 0 and 25,
        Uniqueness "event_id" >= 0.98,
        ColumnLength "sensor_id" = 12,
        IsComplete "metric_type",
        ColumnValues "metric_type" in ["temperature", "pressure", "speed", "volume"],
        ReferentialIntegrity "sensor_id" "dim_sensores.sensor_id" = 1.0
    ]

Um detalhe de sintaxe que merece atenção: na regra `ReferentialIntegrity`, o segundo parâmetro usa a notação `"Alias.coluna"`, onde `Alias` referencia a tabela de referência configurada no job — não o nome literal da tabela. A regra `ReferentialIntegrity` é avaliada em jobs ETL e suporta verificação de relacionamento entre datasets distintos. O operador `= 1.0` exige integridade total (100% dos `sensor_id` presentes na dimensão); um threshold como `>= 0.97` toleraria até 3% de órfãos.

### Tipos de regra mais usados

O DQDL oferece atualmente 27 tipos de regra que cobrem os cenários mais comuns de qualidade de dados industriais:

| Tipo de Regra | O que verifica | Exemplo |
|---|---|---|
| Completeness | Percentual de valores não-nulos | Completeness "sensor_id" >= 0.99 |
| Uniqueness | Percentual de valores únicos | Uniqueness "event_id" >= 0.98 |
| ColumnValues | Valores dentro de faixa ou lista | ColumnValues "temperature" between -40 and 150 |
| ColumnLength | Comprimento fixo ou faixa de caracteres | ColumnLength "sensor_id" = 12 |
| IsComplete | Campo 100% preenchido (sem nulos) | IsComplete "metric_type" |
| ReferentialIntegrity | Integridade referencial entre tabelas | ReferentialIntegrity "sensor_id" "ref.sensor_id" = 1.0 |
| RowCount | Volume de registros esperado | RowCount >= 10000 |
| CustomSql | Validação via SQL customizado | CustomSql "SELECT COUNT(*) FROM primary WHERE value < 0" = 0 |

### Anomaly detection: quando você não conhece o threshold

Regras estáticas têm uma fraqueza estrutural: thresholds envelhecem. Um exemplo clássico documentado pela AWS — um engenheiro de dados de uma varejista define que vendas diárias devem superar US$ 1 milhão. Meses depois, as vendas passam de US$ 2 milhões e o threshold fica obsoleto. Quando um pipeline de extração falha silenciosamente e as vendas caem 25%, a regra desatualizada continua passando, e o problema só é descoberto depois de horas de investigação.

A resposta do Glue Data Quality para isso são os **Analyzers** e a regra **DetectAnomalies**. Analyzers coletam estatísticas (RowCount, Completeness, DistinctValuesCount, Mean, StandardDeviation, entre outras) sem aplicar nenhuma condição fixa. Ao longo das execuções, o serviço armazena essas estatísticas e, com um mínimo de três pontos de dados, um algoritmo de machine learning aprende a tendência — inclusive sazonalidade — e prevê faixas esperadas com limites superior e inferior. Quando o valor real rompe esses limites, uma Observação de anomalia é gerada.

Na Energéticos, o Gate 3 combina regras determinísticas com analyzers para volume de eventos por planta:

    Rules = [
        RowCount > 1000,
        DetectAnomalies "RowCount"
    ]
    Analyzers = [
        RowCount,
        DistinctValuesCount "sensor_id"
    ]

Existe ainda uma terceira opção, as **Dynamic Rules**, suportadas em jobs Glue ETL, que permitem thresholds calculados em tempo de execução — por exemplo `RowCount > avg(last(10))`, que exige que a contagem atual supere a média das dez execuções anteriores. É o meio-termo entre o threshold fixo e o ML de anomaly detection.

### Vantagens da abordagem declarativa

A abordagem declarativa do DQDL traz três benefícios diretos. Primeiro, as regras são legíveis por analistas de negócio, não apenas por engenheiros. Segundo, o versionamento é trivial porque as regras são texto puro. Terceiro, o Glue Data Quality gera métricas históricas automaticamente, permitindo acompanhar a evolução da qualidade ao longo do tempo.

Na Energéticos, o time de qualidade industrial define as regras de negócio em linguagem natural. O engenheiro de dados traduz para DQDL. Ambos revisam juntos. Esse processo colaborativo eliminou 80% dos falsos positivos que existiam quando as regras eram hardcoded em scripts Python.

Vale conhecer os limites do serviço para dimensionar bem: um ruleset suporta até 2.000 regras e tem tamanho máximo de 65 KB, rulesets maiores devem ser divididos. As estatísticas coletadas têm limite de 100.000 por conta e retenção de até dois anos. Em custo, o Glue Data Quality cobra por DPU consumido durante a avaliação, na mesma tarifa de um Glue Job equivalente (na ordem de US$ 0,44 por DPU-hora), e a detecção de anomalias consome aproximadamente 1 DPU por estatística analisada — um motivo para habilitar anomaly detection apenas em tabelas de alto valor.

## CI/CD para Pipelines de Dados

Na Energéticos S/A, todo código de pipeline passa por um fluxo de CI/CD antes de chegar à produção. Isso inclui Glue Jobs, regras DQDL, definições de Step Functions e scripts de infraestrutura. A premissa é simples: se o código muda, ele precisa ser testado antes de tocar dados reais.

CI/CD é o pilar que distingue DataOps de "ETL agendado". Sem ele, cada alteração de uma regra de qualidade ou de uma transformação é uma edição manual no console, sem revisão, sem histórico, sem rollback. Com ele, o pipeline de dados ganha as mesmas garantias de um sistema de software: rastreabilidade de cada mudança, revisão por pares, e a capacidade de reverter para um estado conhecido.

### Estrutura do repositório

O repositório segue uma estrutura que separa claramente código de transformação, regras de qualidade e infraestrutura:

```
pipeline-energeticos/
  glue-jobs/
    raw_to_bronze.py
    bronze_to_silver.py
    silver_to_gold.py
  quality-rules/
    gate1_ingestao.dqdl
    gate2_conformidade.dqdl
    gate3_consistencia.dqdl
    gate4_entrega.dqdl
  stepfunctions/
    pipeline_definition.asl.json
  cfn/
    infrastructure.yaml
  tests/
    test_transformations.py
    test_quality_rules.py
```

<img width="715" height="244" alt="codecommitmain" src="https://github.com/user-attachments/assets/64f449f1-745f-4ed9-8c68-730670075b96" />

*Branch `main` do repositório, com o template versionado. Cada push dispara o pipeline de CI/CD.*

> **Nota sobre o repositório de código.** Esta implementação usa AWS CodeCommit como repositório Git. É importante o contexto: em julho de 2024 a AWS deixou de aceitar novos clientes no CodeCommit, recomendando GitHub, GitLab ou outros provedores. Em novembro de 2025, após retorno de feedback de clientes — especialmente de setores regulados que valorizam a integração nativa com IAM, VPC endpoints e CloudTrail —, a AWS reverteu a decisão e o CodeCommit voltou à disponibilidade geral completa, com inscrições de novos clientes reabertas. Para times que preferem outro provedor, todo o fluxo descrito aqui funciona de forma equivalente com **AWS CodeConnections** (antiga CodeStar Connections) integrando GitHub ou GitLab ao CodePipeline — a arquitetura de CI/CD não muda, apenas a origem do código.

### Arquitetura CI/CD Cross-Region

O pipeline de CI/CD da Energéticos opera cross-region nessa demo(em projetos produtivos cada ambiente deverá ter sua conta separada): o código vive em us-east-1 (desenvolvimento) e o deploy de produção acontece em sa-east-1 (São Paulo, mais próximo das plantas industriais),. Essa separação garante que o ambiente de desenvolvimento nunca interfira na produção, e que os dados de produção fiquem na região com menor latência para os consumidores.

> **Nota:** A fim de demonstração, utilizamos duas regiões distintas (us-east-1 e sa-east-1) para simular o cenário cross-region. Em produção, a escolha de regiões depende dos requisitos de latência, compliance e disaster recovery de cada organização.

```
┌─────────────────── us-east-1 (DEV) ───────────────────┐
│                                                         │
│  CodeCommit          CodePipeline         CodeBuild     │
│  (branch main) ──→  (Source) ──→  (Build & Deploy)     │
│                                         │               │
└─────────────────────────────────────────│───────────────┘
                                          │
                                          │ aws cloudformation deploy
                                          │ --region sa-east-1
                                          ▼
┌─────────────────── sa-east-1 (PROD) ──────────────────┐
│                                                         │
│  CloudFormation Stack: dataops-demo-prod                │
│  ├── S3 Buckets (raw, processed, scripts)              │
│  ├── Glue Job (ETL com Data Quality)                   │
│  ├── Step Functions (orquestração)                     │
│  ├── SNS (alertas)                                     │
│  └── CloudWatch Alarms (observabilidade)               │
│                                                         │
└─────────────────────────────────────────────────────────┘
```

### Fluxo de CI/CD com CodePipeline + CodeBuild

O fluxo de entrega contínua usa CodePipeline com dois estágios e um buildspec inline que executa validação e deploy:

**Estágio 1 — Source:** O CodePipeline monitora a branch `main` do CodeCommit via EventBridge. Qualquer push dispara automaticamente o pipeline.

**Estágio 2 — Build & Deploy:** O CodeBuild executa três fases sequenciais:

```yaml
# buildspec.yml (inline no template CloudFormation)
version: 0.2
phases:
  pre_build:
    commands:
      - echo "VALIDANDO TEMPLATE CLOUDFORMATION"
      - aws cloudformation validate-template \
          --template-body file://$CODEBUILD_SRC_DIR/$TEMPLATE_FILE \
          --region $DEPLOY_REGION
      - echo "Template validado com sucesso!"
  build:
    commands:
      - echo "DEPLOY CROSS-REGION para $DEPLOY_REGION"
      - aws cloudformation deploy \
          --template-file $CODEBUILD_SRC_DIR/$TEMPLATE_FILE \
          --stack-name $STACK_NAME \
          --region $DEPLOY_REGION \
          --capabilities CAPABILITY_NAMED_IAM \
          --parameter-overrides Environment=prod AlertEmail=$ALERT_EMAIL \
          --no-fail-on-empty-changeset
  post_build:
    commands:
      - aws cloudformation describe-stacks \
          --stack-name $STACK_NAME --region $DEPLOY_REGION \
          --query "Stacks[0].StackStatus" --output text
      - echo "Pipeline CI/CD cross-region concluído!"
```

O `--no-fail-on-empty-changeset` é importante: se o template não mudou, o pipeline não falha — simplesmente reporta que não há alterações. Isso evita falsos negativos quando apenas scripts Glue são atualizados sem mudança na infraestrutura.


<img width="1700" height="740" alt="codebuild" src="https://github.com/user-attachments/assets/23af2160-375e-45c4-839b-9228eb920795" />

*As 11 fases do CodeBuild, de SUBMITTED a COMPLETED, todas com status "Com êxito". A fase BUILD executa o deploy cross-region.*


<img width="1277" height="758" alt="Cloudformation" src="https://github.com/user-attachments/assets/dfde89e0-f9e6-454a-8377-3aec96bba364" />

*Log de execução: o template é validado, o deploy cross-region é aplicado e o stack chega a UPDATE_COMPLETE.*

### IAM Cross-Region: o desafio real

O maior desafio do CI/CD cross-region é a configuração de IAM. O CodeBuild em us-east-1 precisa de permissões para criar e gerenciar recursos em sa-east-1. Na Energéticos, a role do CodeBuild tem policies separadas:

- **Policy base:** logs no CloudWatch e acesso ao bucket de artefatos (us-east-1)
- **Policy cross-region:** CloudFormation, S3, Glue, Step Functions, SNS, CloudWatch e IAM em sa-east-1

A separação em duas policies facilita a auditoria: a equipe de segurança revisa a policy cross-region separadamente, e qualquer mudança de escopo é detectada no diff do template.

### Trigger automático via EventBridge

O pipeline não usa polling. Uma regra EventBridge monitora eventos de mudança de estado no CodeCommit e dispara o CodePipeline automaticamente. Isso reduz a latência entre commit e deploy de minutos (polling a cada 1 min) para segundos.

```yaml
EventPattern:
  source: [aws.codecommit]
  detail-type: ["CodeCommit Repository State Change"]
  detail:
    event: [referenceCreated, referenceUpdated]
    referenceType: [branch]
    referenceName: [main]
```


### Testes com dados sintéticos

O segredo do CI/CD para dados é ter datasets de teste que representem cenários reais. Na Energéticos, o time mantém 4 datasets sintéticos:

| Dataset | Propósito | Características |
|---|---|---|
| happy_path.json | Validar fluxo normal | 1000 eventos válidos, todos os sensores |
| schema_drift.json | Testar resiliência a mudanças | Campos extras, campos faltando, tipos alterados |
| outliers.json | Validar quality gates | Temperaturas extremas, pressões negativas, timestamps futuros |
| volume_spike.json | Testar escalabilidade | 100x o volume normal em uma janela de 5 minutos |

Cada push no repositório executa a suíte de testes contra esses datasets. Se qualquer quality gate rejeitar dados do happy_path ou aceitar dados do outliers, o pipeline de CI/CD falha e o merge é bloqueado. A suíte usa pytest com a biblioteca hypothesis para property-based testing — em vez de só verificar exemplos fixos, ela gera variações dos dados e confirma que as invariantes do pipeline se mantêm.

<img width="709" height="332" alt="teste unitario" src="https://github.com/user-attachments/assets/96635036-c406-473d-9da5-838b44b06562" />


## Orquestração com Step Functions: Choice e Catch

A orquestração do pipeline da Energéticos usa AWS Step Functions com dois padrões fundamentais: Choice para roteamento condicional baseado em resultados de qualidade, e Catch para tratamento resiliente de falhas.

### Por que Step Functions

Step Functions oferece três características essenciais para DataOps. Primeiro, a máquina de estados é visual e auditável, qualquer pessoa consegue entender o fluxo olhando o diagrama. Segundo, o histórico de execuções é mantido automaticamente, facilitando troubleshooting. Terceiro, o modelo de retry e catch é declarativo, sem código boilerplate de tratamento de exceções.

### O padrão Choice: roteamento por qualidade

Após cada quality gate, um estado Choice avalia o score de qualidade retornado pelo Glue Data Quality. Dependendo do resultado, o fluxo segue caminhos diferentes:

    [Glue Job: Raw to Bronze]
        |
    [Quality Gate 2: Conformidade]
        |
    [Choice: Score >= 0.92?]
       /          \
     SIM          NÃO
      |             |
    [Bronze       [Quarentena +
     to Silver]    Notificação]

O estado Choice avalia a variável `$.qualityScore` retornada pelo Glue Data Quality. Se o score é maior ou igual a 0.92, o pipeline avança normalmente. Se é menor, os dados são direcionados para quarentena e o time recebe notificação via SNS.

### O padrão Catch: resiliência a falhas

Cada estado do Step Functions tem um bloco Catch que captura erros específicos e direciona para tratamento adequado. Na Energéticos, o Catch diferencia entre erros transitórios (que merecem retry) e erros permanentes (que exigem intervenção humana):

    Estratégia de Catch por tipo de erro:

    Glue Job Timeout (transitório)
      -> Retry com backoff exponencial: 30s, 60s, 120s
      -> Após 3 tentativas: notifica equipe + pausa pipeline

    Glue Job Exception (permanente)
      -> Captura erro no estado "HandleFailure"
      -> Registra detalhes no DynamoDB (tabela pipeline_incidents)
      -> Notifica equipe via SNS com contexto do erro
      -> Move dados para quarentena

    Quality Gate Failure (condicional)
      -> Choice avalia severidade do score
      -> Score entre 0.85 e 0.92: warning, pipeline continua com flag
      -> Score abaixo de 0.85: pipeline para, dados em quarentena

### Combinando Choice e Catch

O poder real aparece quando Choice e Catch trabalham juntos. O Choice lida com decisões de negócio (qualidade suficiente ou não). O Catch lida com falhas técnicas (timeout, exceção, serviço indisponível). Juntos, cobrem tanto o cenário "dados ruins" quanto o cenário "infraestrutura com problema".

<img width="1123" height="655" alt="image" src="https://github.com/user-attachments/assets/0a529712-7a86-442e-8bf3-90d6900564da" />

*Máquina de estados em execução: o estado StartGlueJob conclui com sucesso (verde), o Catch #1 protege contra falhas roteando para NotifyFailure, e o fluxo chega a JobSucceeded.*


## Observabilidade em 3 Planos

Observabilidade em DataOps vai além de "o job rodou ou não". Na Energéticos, a equipe implementou observabilidade em três planos complementares: infraestrutura, pipeline e dados. Cada plano responde perguntas diferentes, tem público diferente e usa ferramentas diferentes. Essa separação é deliberada: o analista de negócio não quer ver duração de Glue Job, e o engenheiro de plantão não quer abrir um dashboard de KPI para descobrir que um job travou.

### Plano 1: Infraestrutura

O plano de infraestrutura monitora os recursos computacionais que sustentam o pipeline. As perguntas que este plano responde são: os jobs estão consumindo recursos dentro do esperado? Existe throttling? A capacidade provisionada é suficiente?

| Métrica | Serviço | Alarme |
|---|---|---|
| Duração do Glue Job | CloudWatch Metrics | > 2x a média histórica |
| DPU utilizado vs alocado | CloudWatch Metrics | Utilização > 90% por 15 min |
| Erros de API (throttling) | CloudTrail + CloudWatch | > 5 throttles em 1 minuto |
| Tamanho da fila Kinesis | CloudWatch Metrics | Iterator age > 5 minutos |
| Lambda concurrent executions | CloudWatch Metrics | > 80% do limite da conta |

### Plano 2: Pipeline

O plano de pipeline monitora o fluxo de execução end-to-end. As perguntas são: o pipeline executou no horário? Todas as etapas completaram? O SLA de entrega foi cumprido?

| Métrica | Serviço | Alarme |
|---|---|---|
| Status da execução Step Functions | CloudWatch Events | Qualquer execução com status FAILED |
| Latência end-to-end | CloudWatch Custom Metric | > 45 minutos (SLA é 1 hora) |
| Jobs em fila aguardando | CloudWatch Metrics | > 3 jobs pendentes |
| Frequência de retries | CloudWatch Logs Insights | > 2 retries por execução |
| Tempo entre ingestão e disponibilidade | Custom Metric | > 2 horas (freshness SLA) |

### Plano 3: Dados

O plano de dados monitora a qualidade e o comportamento dos dados em si. As perguntas são: os dados estão corretos? O volume está dentro do esperado? Existem anomalias estatísticas?

| Métrica | Serviço | Alarme |
|---|---|---|
| Score de qualidade por gate | Glue Data Quality + CloudWatch | Score < 0.92 |
| Volume de registros por hora | CloudWatch Custom Metric | Queda > 30% vs média 7 dias |
| Taxa de quarentena | Custom Metric | > 5% dos registros em quarentena |
| Drift de schema detectado | Glue Schema Registry | Qualquer alteração não-planejada |
| Freshness dos dados Gold | Custom Metric | Dados com mais de 2 horas de atraso |

### Dashboard unificado

Os três planos convergem em um dashboard CloudWatch que a equipe da Energéticos consulta diariamente. O dashboard agrega métricas de Step Functions (execuções e duração), Glue Job (tasks completadas vs falhas, bytes lidos e records escritos), SNS (notificações publicadas e entregues) e um painel de Quality Gate que resume o score de qualidade contra o threshold definido.

<img width="1894" height="827" alt="image" src="https://github.com/user-attachments/assets/a4fd68c3-ccd2-4e8f-8789-130d23473c8d" />



*Dashboard DataOps-Pipeline-Monitor: execuções e duração do Step Functions, métricas do Glue Job, notificações SNS e o painel Quality Gate com o resumo do score.*

O time configurou alarmes compostos (Composite Alarms) que correlacionam métricas dos três planos. Por exemplo: se o volume de dados cai E o Glue Job está com duração normal E não há erros de infraestrutura, o problema provavelmente está na fonte. Essa correlação reduz o tempo de diagnóstico de 45 minutos para menos de 5.

### Alertas com SNS

O Amazon SNS distribui as notificações do pipeline. O modelo de publish/subscribe permite que um único evento — um quality gate que falhou, um Glue Job que estourou timeout — seja entregue simultaneamente a múltiplos endpoints: e-mail da equipe de plantão, um endpoint HTTPS que registra o incidente, e potencialmente SMS para incidentes críticos. O publisher (o estado NotifyFailure do Step Functions) não conhece os subscribers; essa indireção é o que torna o sistema de alertas extensível sem mexer no pipeline.


<img width="1542" height="518" alt="topico" src="https://github.com/user-attachments/assets/de384cbc-3bac-4cdc-b67d-44f2f937d6eb" />

*Tópico `dataops-demo-dev-pipeline-alerts` com assinatura de e-mail confirmada. Cada alerta carrega o contexto do incidente.*

## Processo de Quarentena

Quarentena é o mecanismo que isola dados problemáticos sem interromper o pipeline inteiro. Na Energéticos, dados que falham em qualquer quality gate são movidos para um bucket S3 separado com metadados que explicam o motivo da rejeição.

A separação física das camadas — raw, processed (bronze/silver/gold) e quarentena em buckets distintos — é uma decisão arquitetural importante: garante que dados problemáticos nunca contaminem fisicamente as camadas downstream, mesmo em caso de erro de código no pipeline.

<img width="1057" height="491" alt="image" src="https://github.com/user-attachments/assets/21144f7f-0ee0-4611-bdf1-91c6c6978083" />

*Os buckets do data lake: raw (ingestão), processed (camadas medallion), scripts, cicd-artifacts e o bucket dedicado de quarentena. A separação física isola cada estágio e impede que dados rejeitados contaminem camadas downstream.*

### Estrutura da quarentena

O bucket de quarentena segue uma estrutura que facilita investigação e reprocessamento:

    s3://dataops-demo-dev-quarantine-235911282620/
      gate=1/date=2026-05-16/hour=14/
        file_001.json
        file_001_metadata.json
      gate=2/date=2026-05-16/hour=14/
        batch_042.parquet
        batch_042_metadata.json

O arquivo de metadados contém informações essenciais para diagnóstico:

    - gate: qual quality gate rejeitou
    - timestamp: quando a rejeição aconteceu
    - rules_failed: lista de regras DQDL que falharam
    - quality_score: score obtido vs score mínimo
    - sample_violations: 10 registros de exemplo que violaram as regras
    - source_file: arquivo original que gerou os dados
    - pipeline_execution_id: ID da execução do Step Functions

<img width="1627" height="393" alt="image" src="https://github.com/user-attachments/assets/a5191412-7b05-4314-b1ce-2f81f5c0edb0" />

*O bucket de quarentena particionado por gate: `gate=1/` e `gate=2/` isolam as rejeições de cada quality gate, facilitando triagem e reprocessamento direcionado.*

### Ciclo de vida da quarentena

Dados em quarentena seguem um ciclo de vida definido com SLAs claros:

1. Detecção: quality gate rejeita e move para quarentena (automático, imediato)
2. Notificação: SNS alerta o time com contexto do problema (automático, < 1 minuto)
3. Triagem: engenheiro avalia se é problema de fonte, transformação ou regra (manual, SLA 4 horas)
4. Correção: fix aplicado na fonte, no job ou na regra (manual, SLA 24 horas)
5. Reprocessamento: dados corrigidos são reinjetados no pipeline (semi-automático, < 1 hora)
6. Expiração: dados não reprocessados em 30 dias são movidos para Glacier (automático, via S3 Lifecycle)
<img width="1306" height="644" alt="image" src="https://github.com/user-attachments/assets/305ecd6b-ae7e-492f-8a81-fd5daeed16e6" />

*Regra S3 Lifecycle `quarentena-glacier-30dias` habilitada: objetos não reprocessados transicionam automaticamente para Glacier Flexible Retrieval no dia 30, reduzindo custo de retenção sem perder os dados para auditoria.*

### Reprocessamento controlado

O reprocessamento não é simplesmente "rodar de novo". Na Energéticos, o Step Functions tem um fluxo separado chamado "ReprocessQuarantine" que:

- Lê os metadados da quarentena para entender o contexto
- Aplica a correção específica (novo schema, nova regra, dados complementares)
- Executa os quality gates novamente com as mesmas regras
- Se aprovado, injeta os dados na camada correta com flag "reprocessed=true"
- Se reprovado novamente, escala para revisão manual com prioridade alta

Esse processo garantiu que a Energéticos recuperasse 94% dos dados quarentenados dentro de 48 horas, em vez de simplesmente descartá-los.

## A Infraestrutura como Código

Toda a arquitetura descrita — buckets, Glue Job, Step Functions, SNS, alarmes — é provisionada por dois templates CloudFormation. Isso garante reprodutibilidade (o ambiente de produção é idêntico ao de homologação), versionamento (cada mudança de infraestrutura passa pelo mesmo CI/CD do código) e capacidade de auditoria (o diff do template mostra exatamente o que mudou).

<img width="1144" height="685" alt="image" src="https://github.com/user-attachments/assets/9d198223-31e7-436b-b601-ba8f806b9d48" />


O stack é parametrizado por ambiente (`Environment=dev|prod`), o que permite que o mesmo template gere as duas instâncias sem duplicação de código. O deploy de produção, disparado pelo CodeBuild, chega ao estado `UPDATE_COMPLETE` ao final do pipeline de CI/CD.

## Lições Aprendidas

Após 8 meses operando com DataOps, a Energéticos S/A acumulou aprendizados que transcendem a implementação técnica. Estas lições servem como guia para quem está começando a jornada.

### Lição 1: Comece pelos quality gates, não pela automação

O instinto natural é automatizar primeiro. Mas automatizar um pipeline sem quality gates apenas acelera a propagação de dados ruins. A Energéticos começou implementando o Gate 2 (conformidade) no pipeline mais crítico. Em 2 semanas, descobriu que 12% dos dados de temperatura estavam fora da faixa operacional. Só depois de resolver esse problema é que a automação fez sentido.

### Lição 2: DQDL precisa de ownership compartilhado

Regras de qualidade escritas apenas por engenheiros tendem a ser tecnicamente corretas mas semanticamente incompletas. Na Energéticos, o engenheiro de manutenção sabia que temperatura acima de 85°C em um sensor específico indicava calibração errada, não aquecimento real. Essa regra nunca teria sido escrita sem colaboração entre domínios.

### Lição 3: Quarentena não é lixeira

Nos primeiros meses, o bucket de quarentena cresceu sem controle porque ninguém olhava. O time tratava quarentena como descarte. Quando implementaram o SLA de triagem de 4 horas e o dashboard de "dados em quarentena por idade", a taxa de recuperação subiu de 23% para 94%.

### Lição 4: Observabilidade em 3 planos evita guerra de culpas

Antes dos 3 planos, cada incidente gerava uma discussão: "é problema de infra", "é problema de dados", "é problema do job". Com métricas separadas por plano, o diagnóstico ficou objetivo. O Composite Alarm que correlaciona os planos reduziu o MTTR (tempo médio de recuperação) de 2 horas para 12 minutos.

### Lição 5: CI/CD para dados exige datasets de teste realistas

Os primeiros testes usavam 10 registros perfeitos. Passavam sempre. Em produção, quebravam com dados reais. Quando o time criou os 4 datasets sintéticos (happy path, schema drift, outliers, volume spike), a taxa de incidentes pós-deploy caiu 73%.

### Lição 6: Step Functions com Choice é mais poderoso que if/else em código

A tentação é colocar lógica condicional dentro do Glue Job. Mas isso esconde decisões de negócio dentro de código técnico. Com Choice no Step Functions, a decisão "score abaixo de 0.92 vai para quarentena" é visível no diagrama, auditável no histórico e modificável sem redeploy do job.

### Lição 7: Thresholds estáticos envelhecem — combine com anomaly detection

As primeiras regras DQDL da Energéticos eram todas estáticas. Funcionaram até o volume de produção crescer 40% em um trimestre e os limites de RowCount ficarem obsoletos, deixando passar quedas reais de volume. Migrar as regras de volume e ticket para `DetectAnomalies` com Analyzers eliminou a manutenção manual de thresholds e capturou dois incidentes de ingestão parcial que regras fixas não pegariam.

### Lição 8: Meça antes e depois

A Energéticos mediu 4 KPIs antes de implementar DataOps e continuou medindo depois:

| KPI | Antes | Depois | Melhoria |
|---|---|---|---|
| Incidentes silenciosos por semana | 9 | 0.5 | -94% |
| MTTR (tempo de recuperação) | 2 horas | 12 minutos | -90% |
| Dados corrompidos em produção | 12% | 0.8% | -93% |
| Deploys de pipeline por semana | 1 | 8 | +700% |

## Conclusão e Recomendações

DataOps não é um produto que se instala. É uma disciplina que se constrói incrementalmente, gate por gate, métrica por métrica. O caso da Energéticos S/A demonstra que a combinação de quality gates com DQDL, orquestração resiliente com Step Functions, observabilidade em 3 planos, CI/CD versionado e quarentena com SLA transforma pipelines frágeis em sistemas confiáveis.

A AWS oferece os blocos necessários: Glue Data Quality para regras declarativas e detecção de anomalias por ML, Step Functions para orquestração com Choice e Catch, CloudWatch para observabilidade multi-plano, CodePipeline e CodeBuild para CI/CD de dados, e CloudFormation para infraestrutura como código. O diferencial está em como esses serviços são combinados com práticas de engenharia.

### Recomendação 1: Implemente o Gate 2 primeiro

Escolha o pipeline mais crítico e adicione regras DQDL de conformidade. Comece com 5 regras simples: completude dos campos-chave, faixa de valores para métricas numéricas e integridade referencial. Meça quantos dados falham. Esse número vai justificar todo o investimento subsequente.

### Recomendação 2: Adicione Choice ao Step Functions existente

Se você já usa Step Functions, adicione um estado Choice após o Glue Job que avalia o score de qualidade. Direcione dados com score baixo para um bucket de quarentena. Essa única mudança já elimina a propagação silenciosa de dados ruins.

### Recomendação 3: Separe observabilidade em 3 planos

Crie 3 dashboards no CloudWatch: infraestrutura, pipeline e dados. Configure Composite Alarms que correlacionem métricas entre planos. Quando um alarme dispara, o plano afetado indica imediatamente onde investigar.

### Recomendação 4: Trate quarentena como processo, não como lixeira

Defina SLAs para triagem (4 horas) e correção (24 horas). Crie um dashboard que mostre dados em quarentena por idade. Implemente o fluxo de reprocessamento. Dados recuperados são dados que não precisam ser reingeridos da fonte.

### Recomendação 5: CI/CD com datasets que quebram de propósito

Crie pelo menos 3 datasets de teste: um que deve passar em todos os gates, um com anomalias que deve ser quarentenado e um com schema alterado. Se o pipeline de CI/CD não falha com dados ruins, ele não está testando nada.

---

## Referências

As afirmações técnicas deste artigo foram verificadas contra a documentação oficial da AWS. Para aprofundamento:

1. **AWS Glue Data Quality** — visão geral, conceitos, limites de ruleset e estatísticas: https://docs.aws.amazon.com/glue/latest/dg/glue-data-quality.html
2. **Data Quality Definition Language (DQDL) reference** — sintaxe, tipos de regra, operadores compostos e analyzers: https://docs.aws.amazon.com/glue/latest/dg/dqdl.html
3. **Anomaly detection in AWS Glue Data Quality** — Analyzers, DetectAnomalies e o algoritmo de ML: https://docs.aws.amazon.com/glue/latest/dg/data-quality-anomaly-detection.html
4. **DetectAnomalies rule type** — sintaxe e regras suportadas: https://docs.aws.amazon.com/glue/latest/dg/dqdl-rule-types-DetectAnomalies.html
5. **AWS::Glue::DataQualityRuleset (CloudFormation)** — propriedades do recurso: https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-resource-glue-dataqualityruleset.html
6. **Evaluating data quality for ETL jobs in AWS Glue** — uso de ReferentialIntegrity com aliases: https://docs.aws.amazon.com/glue/latest/dg/tutorial-data-quality.html
7. **AWS Step Functions Developer Guide** — estados Choice, Catch e Retry: https://docs.aws.amazon.com/step-functions/latest/dg/concepts-states.html
8. **AWS CodeCommit returns to General Availability** (nov/2025) — contexto sobre a disponibilidade do serviço: https://aws.amazon.com/blogs/devops/aws-codecommit-returns-to-general-availability/
9. **Build event-driven data quality pipelines with AWS Glue DataBrew** — padrão de quality gate event-driven: https://aws.amazon.com/blogs/big-data/build-event-driven-data-quality-pipelines-with-aws-glue-databrew/
10. **Amazon CloudWatch — Composite Alarms** — correlação de alarmes entre planos: https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/Create_Composite_Alarm.html

---

## Sobre a Autora

**Marcela Monteiro Montenegro Gallo**
Arquiteta de Dados e AI | 9x AWS Certified | 2x Databricks Certified
Ingram Micro Cloud — AWS Partner

[LinkedIn](https://www.linkedin.com/in/marcelamontenegro/)

