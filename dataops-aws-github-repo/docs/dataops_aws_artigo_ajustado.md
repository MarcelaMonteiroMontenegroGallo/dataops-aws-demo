# DataOps na AWS sem fórmula mágica: como a Energéticos/SA virou seu pipeline de varejo de "vai dar certo" para "está sob controle"

## Por que esse artigo existe

DataOps virou termo genérico em deck de venda. Em quase todo lugar você lê "DataOps é DevOps para dados", aparece um diagrama com setas saindo de S3, passando por Glue, parando em QuickSight, e nada disso responde à pergunta que importa:

**quando os dados quebram, em que ponto exatamente o time descobre?**

Se a resposta envolve um diretor recebendo print do Power BI no WhatsApp dizendo "isso aqui está estranho", o pipeline não é DataOps. É só ETL com sorte.

DataOps de verdade exige cinco pilares operacionais explícitos:

1. **Qualidade verificável**: testes contra regras de negócio, não apenas schema.
2. **Observabilidade em três planos**: pipeline, dados e negócio.
3. **Automação completa**: do ingresso à publicação, sem etapa manual.
4. **Governança rastreável**: linhagem, versão de schema, auditoria de quem mudou o quê.
5. **CI/CD completo**: versionamento, validação, testes, promoção entre ambientes e rollback controlado.

Faltando qualquer um deles, você tem ETL antigo com nome novo.

Este artigo apresenta uma implementação de referência de DataOps na AWS para uma empresa de varejo brasileira fictícia, **Energéticos/SA**, usando Amazon S3, AWS Glue, AWS Glue Data Quality, AWS Step Functions, Amazon EventBridge, Amazon SNS, AWS Lambda, Amazon CloudWatch, AWS CodePipeline, AWS CodeBuild e CloudFormation.

A ideia não é vender uma fórmula mágica. É mostrar uma arquitetura operável, auditável e demonstrável.

---

## O caso: Energéticos/SA

A **Energéticos/SA** é uma rede brasileira fictícia de varejo especializado em bebidas energéticas e funcionais, com 340 pontos de venda em capitais e cidades médias, operação multicanal e áreas de negócio consumindo dados diariamente: comercial, supply chain, marketing, financeiro e diretoria.

O time de dados enfrentava três problemas principais.

**Primeiro**, os dados de PDV chegavam de sistemas diferentes, em formatos diferentes, com horários variáveis e sem garantia de completude. Em várias ocasiões, uma rede inteira deixava de enviar dados e o problema só era percebido dias depois, quando algum indicador não fechava.

**Segundo**, erros de classificação fiscal e produto chegavam ao data lake e eram propagados para relatórios oficiais. O problema não era apenas técnico: dado errado podia virar risco regulatório, retrabalho e perda de confiança.

**Terceiro**, havia pressão por velocidade. Comercial queria dashboards atualizados em D+1 às 9h. Marketing queria cohort analysis no mesmo dia. Supply chain queria dados confiáveis para reposição. Pipeline batch que falha silenciosamente não sustenta esse nível de operação.

A meta da iniciativa de DataOps foi clara:

> reduzir o tempo médio de detecção de problemas em dados de dias para minutos, bloquear propagação de dados ruins e dar visibilidade explícita do que está rodando, falhando ou atrasado — sem depender de alguém abrir o console da AWS.

---

## Por que Glue ETL puro não basta

A primeira versão do pipeline era a clássica: Glue Jobs em PySpark, S3 particionado e Athena por cima. Funcionava, até deixar de funcionar.

O problema não era o Glue. O problema era achar que um job ETL bem escrito resolve tudo.

**Schema drift silencioso.** Uma origem começou a mandar `valor_unitario` como texto com vírgula em vez de ponto decimal. O processamento não quebrou de forma clara. Parte dos dados foi convertida, parte virou nulo, parte passou com comportamento inesperado. O dashboard continuou existindo, mas deixou de ser confiável.

**Testes de código não validavam dados reais.** O time tinha testes em datasets controlados, mas isso não garantia que os arquivos reais de produção estavam completos, coerentes e dentro do comportamento esperado.

**Alertas eram binários.** O CloudWatch avisava quando um job falhava. Mas não avisava quando o job terminava com sucesso processando 30% menos linhas que ontem. Sucesso aparente é o pior tipo de falha.

A solução não é “escrever mais PySpark”. A solução é desenhar um sistema de dados com portões de qualidade, observabilidade, governança e CI/CD.

---

## Arquitetura medallion com quatro portões de qualidade

A arquitetura segue o padrão medallion, mas com uma diferença importante: **Raw é imutável**.

O dado original sempre é persistido primeiro. Mesmo dado ruim precisa existir para auditoria, reprocessamento e rastreabilidade. Os gates não bloqueiam a chegada do dado bruto; eles bloqueiam a promoção para as próximas camadas.

Fluxo conceitual:

```text
Input
  │
  ▼
Raw imutável no S3
  │
  ▼
Profile Gate
  │
  ├── reprovou ──► Quarentena + alerta
  │
  ▼
Bronze
  │
  ▼
Quality Gate 1 — regras de negócio
  │
  ├── reprovou ──► Quarentena + alerta
  │
  ▼
Silver
  │
  ▼
Quality Gate 2 — Glue Data Quality / DQDL
  │
  ├── reprovou ──► Quarentena + alerta
  │
  ▼
Gold
  │
  ▼
Business Gate — validação semântica de KPIs
  │
  ├── desvio relevante ──► alerta executivo
  │
  ▼
Athena / QuickSight / consumo analítico
```

A decisão de usar quatro portões é deliberada:

- **Profile Gate** identifica problemas estruturais e volumétricos.
- **Quality Gate 1** valida regras de negócio.
- **Quality Gate 2** valida qualidade contínua usando DQDL.
- **Business Gate** compara KPIs com comportamento histórico e identifica desvios semanticamente suspeitos.

Esse último ponto é crucial: o dado pode estar tecnicamente correto e ainda assim estar errado para o negócio.

---

## Profiling: útil, mas não como muleta

Profiling pode ser usado para discovery, principalmente em cenários em que o time precisa entender rapidamente padrões, nulos, distribuições e anomalias iniciais nos datasets.

Mas, em uma arquitetura DataOps enterprise, profiling não deve ser tratado como única peça de qualidade.

A recomendação para a Energéticos/SA foi:

- usar profiling para entender comportamento dos dados;
- usar regras versionadas e automatizadas para validação contínua;
- manter AWS Glue Data Quality como engine principal para qualidade operacional;
- nunca depender de inspeção visual como etapa obrigatória de produção.

O profiling ajuda a descobrir o problema. O DataOps precisa impedir que ele se propague.

---

## Glue Data Quality e DQDL: qualidade como código

O AWS Glue Data Quality permite definir regras de qualidade com DQDL, uma linguagem declarativa para validar datasets.

Exemplo simplificado:

```text
Rules = [
  ColumnExists "id_transacao",
  IsComplete "id_transacao",
  IsComplete "id_loja",
  ColumnValues "canal" in ["LOJA","MARKETPLACE","B2B"],
  RowCount > 5000,
  Uniqueness "id_transacao" > 0.99,
  Mean "valor_total" between 30 and 500
]
```

A vantagem é que a regra deixa de estar escondida em código procedural e passa a ser um contrato explícito da tabela.

Na Energéticos/SA, cada dataset crítico tem seu conjunto de regras versionado junto ao repositório do pipeline. Mudou a regra? Passa por pull request. Passa por validação. Gera histórico. Entra no deploy controlado.

Isso é DataOps: qualidade como código.

---

## CI/CD completo para DataOps na AWS

Sem CI/CD, DataOps fica incompleto.

O pipeline de CI/CD da Energéticos/SA foi desenhado para tratar dados como produto de software. Não basta fazer deploy de script Glue; é necessário promover a solução inteira com segurança.

O fluxo proposto:

```text
Developer
  │
  ▼
GitHub Pull Request
  │
  ▼
Validação automática
  ├── lint de CloudFormation
  ├── validação de templates
  ├── testes unitários de jobs Glue
  ├── validação de DQDL
  ├── checagem de segurança/IAM
  └── empacotamento dos artefatos
  │
  ▼
Merge na branch principal
  │
  ▼
AWS CodePipeline
  │
  ▼
CodeBuild — Build & Validate
  │
  ▼
CloudFormation Change Set — DEV
  │
  ▼
Deploy DEV
  │
  ▼
Testes pós-deploy
  │
  ▼
Aprovação manual
  │
  ▼
CloudFormation Change Set — HML/PRD
  │
  ▼
Deploy controlado
```

O CI/CD cobre:

- infraestrutura como código;
- versionamento dos scripts Glue;
- versionamento das regras DQDL;
- versionamento dos workflows Step Functions;
- validação automática antes do deploy;
- promoção entre ambientes;
- trilha de auditoria;
- possibilidade de rollback por versão.

Esse é um ponto essencial: DataOps não é só orquestrar dados. É controlar mudança.

---

## Step Functions com Choice + Catch: orquestração que sabe parar

A orquestração precisa fazer mais do que executar tarefas em sequência.

No desenho da Energéticos/SA, cada gate retorna uma decisão:

```json
{
  "passed": true,
  "reason": "dataset aprovado"
}
```

ou

```json
{
  "passed": false,
  "reason": "volume abaixo da média esperada"
}
```

O Step Functions usa estados `Choice` para decidir se promove o dado ou se envia para quarentena. Cada task também tem `Catch`, para capturar falhas técnicas e acionar notificação.

Isso evita o maior erro de pipelines de dados: continuar rodando mesmo quando já deveria ter parado.

---

## Observabilidade em três planos

A observabilidade foi estruturada em três planos.

**Plano 1 — Pipeline.**  
Perguntas: o pipeline rodou? Quanto tempo demorou? Qual etapa falhou? Quantos registros foram processados?  
Público: engenharia de dados.

**Plano 2 — Dados.**  
Perguntas: as colunas estão completas? A cardinalidade mudou? A distribuição está estranha? A regra DQDL passou?  
Público: analistas e engenharia.

**Plano 3 — Negócio.**  
Perguntas: a receita de hoje faz sentido? O volume por canal está coerente? A margem caiu por motivo real ou por erro de dado?  
Público: liderança e áreas de negócio.

Essa separação evita que todo mundo olhe para o mesmo dashboard tentando responder perguntas diferentes.

---

## Quarentena não é cemitério de dados

Um ponto importante: quarentena não pode ser uma pasta esquecida no S3.

Toda entrada em quarentena precisa ter:

- dataset de origem;
- data de ingestão;
- motivo da reprovação;
- etapa em que falhou;
- link para logs;
- status de tratamento;
- responsável;
- decisão final: corrigido, reprocessado, descartado ou aceito com exceção.

Sem isso, a quarentena vira só um bucket caro com problema acumulado.

---

## Lessons learned

**Não dependa só de schema validation.**  
Schema pega estrutura. Não pega semântica.

**Não escreva alerta que ninguém lê.**  
Alerta precisa ser acionável, categorizado e com contexto.

**Não use crawler como contrato de produção.**  
Crawler é útil para discovery. Em produção, schema precisa ser explícito e versionado.

**Não trate CI/CD como detalhe técnico.**  
Em DataOps, CI/CD é mecanismo de governança.

**Não deixe regra de qualidade fora do repositório.**  
Regra fora do Git vira conhecimento tribal.

---

## Conclusão

DataOps na AWS bem feito não é “Glue + Step Functions com nome bonito”.

É um conjunto de decisões arquiteturais que transforma pipeline de dados em sistema operável, auditável e confiável:

- Raw imutável;
- gates explícitos de qualidade;
- DQDL versionado;
- orquestração com decisão;
- quarentena com processo;
- observabilidade em três planos;
- CI/CD completo;
- infraestrutura como código.

O ROI real de DataOps não é apenas velocidade. É confiança operacional.

Sem confiança, todo dashboard é uma aposta com cara de relatório.
