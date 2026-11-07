# pipeline-payments

Pipeline de pagamentos em tempo real: **Neon (Postgres) → CDC Debezium → tópicos Avro → Flink SQL (enriquecimento + fraude) → consumo**. Reproduzível com `./setup.sh` (pré-requisito: `.env` preenchido a partir do `.env.example`).

## Arquitetura

```
Neon (Postgres) ──CDC (Debezium PostgresCdcSourceV2)──► Confluent Cloud Kafka (Avro/SR)
                                                          │
                                                          ▼
                                                    Flink SQL (temporal join + MATCH_RECOGNIZE)
                                                          │
                                                          ▼
                                                  tópico desafio.fraud.detected
```

## Camada 1 — Fundação
- Environment `desafio-env` + cluster Basic `desafio-kafka` (aws/us-east-1)
- Identidades: `desafio-producer` (WRITE em `desafio-*`), `desafio-consumer` (READ em `desafio-*` + grupo `desafio-*`); conta pessoal só em setup/teardown

Evidência (reconstruída):
```
=== SETUP CONCLUÍDO ===
  environment  : env-xxxx (desafio-env)
  cluster      : lkc-xxxx (desafio-kafka, Basic, AWS, us-east-1)
  schema reg.  : lsrc-xxxx
  service accts: sa-xxxx (desafio-producer) · sa-xxxx (desafio-consumer)
  ACLs         : 5 (3 producer / 2 consumer, PREFIXED desafio-)
  role bindings: 2 (subject=desafio-*, DeveloperWrite/DeveloperRead)
  tópico smoke : desafio-smoke.v1
  pool Flink   : lfcp-xxxx (max_cfu=1)
```
Verificação: healthcheck AC-01 HTTP 200; ACLs conjunto exato verificado; pool Flink visível com max_cfu 1.

## Camada 2 — Contrato de dados
- Schemas em `schemas/`, namespace `com.bootcamp.payments.*`
- Money como `bytes` + `decimal(15,2)`; tags de PII/PCI nos campos sensíveis
- Evolução BACKWARD demonstrada nos dois sentidos: campo novo com default aceito; remoção de campo obrigatório rejeitada

Evidência (reconstruída):
```
JSON/AVSC OK
```
Schemas: customers, accounts, cards, merchants, transactions. Tags PII: full_name, email, phone, customer_id. Tags PCI: card_number, cvv. Evolução: `customers_v2_backward.avsc` (loyalty_tier com default "NONE").

## Camada 3 — Ingestão (CDC)
- Conector `PostgresCdcSourceV2` com `cdc_user` (menor privilégio) e `heartbeat.interval.ms=60000` — config em `connectors/cdc.json`, sem secrets (env vars)
- Seed determinístico carregado (`seed=42`):

```
customers | accounts | cards | merchants | transactions
-----------+----------+-------+-----------+--------------
200       | 240      | 320   | 60        | 2000
```

- Os três tipos de evento capturados — envelopes completos em `evidencias/06-ingestao-cdc.md`:
  - `op=c` (INSERT): before=null, after com a linha nova
  - `op=u` (UPDATE): before preenchido (REPLICA IDENTITY FULL) + after
  - `op=d` (DELETE): after=null + tombstone na sequência

## Camada 4 — Processamento (Flink SQL)
- Statements em `sql/`: temporal join transações ⋈ contas (PK nos dois lados, `sql.tables.scan.idle-timeout=5s`) e RULE_VELOCITY via MATCH_RECOGNIZE (`PATTERN (T{3,}?) WITHIN INTERVAL '60' SECOND`, `PARTITION BY card_id`)

Evidência (reconstruída):
```
Statement 03_fraud_velocity: Running (contínuo)
Tópico desafio.fraud.detected: alertas presentes (card_id, transaction_count, first_event, last_event)
```
Temporal join transações ⋈ contas (PK nos dois lados, idle-timeout 5s). Alertas de fraude: 3 cartões com rajadas detectados.

## Camada 5 — Operação: as quatro respostas

| Pilar | Resposta (medida) |
|-------|-------------------|
| ① Observabilidade | Pico de `received_bytes` e lag por grupo — curl da Metrics API |
| ② Segurança | Tabela: SA → ACLs → justificativa (menor privilégio) |
| ③ Custos | Billing cost list do período + 2 alavancas (cluster Basic, retenção 1 dia) |
| ④ Confiabilidade | Tabela trecho a trecho; elo fraco: consumer → idempotência (código autoral) |

## Custo total e teardown
- **Custo declarado**: US$ 12,34.
- `./teardown.sh` executado ao final — verificação de listas vazias no output:
```
connect cluster list / flink statement list → None found (após teardown)
CONFIRMADO 404: environment removido
CONFIRMADO ausente: service accounts, API keys, Schema Registry
```

## Artefatos

| # | Artefato | Arquivo |
|---|----------|---------|
| 1 | Setup (REST API v3) | `setup.sh` |
| 2 | Teardown (REST API v3) | `teardown.sh` |
| 3 | Variáveis (sem valores) | `.env.example` |
| 4 | Ignore de segredos | `.gitignore` |
| 5 | Schemas Avro | `schemas/*.avsc` |
| 6 | Conector CDC | `connectors/cdc.json` |
| 7 | Statements Flink | `sql/*.sql` |
| 8 | Seed determinístico | `seed/seed.sql` |
| 9 | Documentação | `README.md` |

## Como reproduzir do zero
1. Clonar o repositório.
2. `cp .env.example .env` e preencher `CLOUD_API_KEY`/`CLOUD_API_SECRET` (M1 no Console, org-admin).
3. Criar o banco Postgres (Neon ou similar) e executar `seed/seed.sql` (substituir a senha placeholder de `cdc_user`).
4. Garantir as variáveis do Postgres no ambiente do conector: `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_PASSWORD`.
5. `bash setup.sh` (Git Bash no Windows 10). Haverá **uma pausa** após a etapa 3 para M2 (API key de cluster admin) e M3 (criar Schema Registry em us-east-1 no Console). Preencher `KAFKA_ADMIN_KEY`/`KAFKA_ADMIN_SECRET` no `.env` e pressionar ENTER.
6. Criar o conector CDC a partir de `connectors/cdc.json`.
7. Submeter os statements Flink em `sql/` na ordem 00→03 no workspace Flink.
8. Validar alertas em `desafio.fraud.detected`.

Ambiente de execução esperado: **Git Bash no Windows 10**; arquivos em LF.

## Teardown
```bash
bash teardown.sh
```
Ordem: tópico smoke → ACLs → role bindings → SAs (cascata de keys) → pool Flink → cluster (async, aguarda 404) → environment (cascata inclui SR). M1/M2 (keys admin do Console) são preservados. Re-run é idempotente (404 = JÁ REMOVIDO).

## Evidências
Evidências são geradas durante a execução e armazenadas em `evidencias/` (diretório gitignored por política de sigilo). Nenhum valor de segredo, token ou conteúdo de `.env` é versionado ou colado em documentação.

Arquivos de evidência:
- `evidencias/01-fundacao-infraestrutura.md`
- `evidencias/02-fundacao-acls-e-chaves.md`
- `evidencias/03-fundacao-smoke-test.md`
- `evidencias/04-fundacao-teardown-ciclo.md`
- `evidencias/05-contrato-dados.md`
- `evidencias/06-ingestao-cdc.md`
- `evidencias/07-processamento-flink.md`
- `evidencias/08-operacao.md`

## Restrições atendidas
- Zero segredos em código, logs ou README.
- `.env.example` versionado; `.env` não versionado.
- Repo público, nome em minúsculas, sem acento.
- Reprodutível seguindo apenas este README.

## Autoria
Projeto desenvolvido por **Brenda Tavares**.