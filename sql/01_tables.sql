-- Confluent Cloud Flink: o nome da tabela = nome do tópico (com backticks para caracteres especiais).

CREATE TABLE `desafio-.public.transactions` (
  transaction_id   STRING,
  card_id          STRING,
  account_id       STRING,
  merchant_id      STRING,
  amount           DECIMAL(15, 2),
  currency         STRING,
  transaction_type STRING,
  status           STRING,
  created_at       TIMESTAMP(3),
  WATERMARK FOR created_at AS created_at - INTERVAL '5' SECOND,
  PRIMARY KEY (transaction_id) NOT ENFORCED
) WITH (
  'connector' = 'confluent',
  'value.format' = 'avro-registry',
  'scan.startup.mode' = 'latest-offset',
  'changelog.mode' = 'append'
);

CREATE TABLE `desafio-.public.accounts` (
  account_id   STRING,
  customer_id  STRING,
  account_type STRING,
  balance      DECIMAL(15, 2),
  currency     STRING,
  status       STRING,
  created_at   TIMESTAMP(3),
  updated_at   TIMESTAMP(3),
  WATERMARK FOR updated_at AS updated_at - INTERVAL '5' SECOND,
  PRIMARY KEY (account_id) NOT ENFORCED
) WITH (
  'connector' = 'confluent',
  'value.format' = 'avro-registry',
  'scan.startup.mode' = 'latest-offset'
);

CREATE TABLE `desafio.fraud.detected` (
  card_id            STRING,
  transaction_count  INT,
  first_event        TIMESTAMP(3),
  last_event         TIMESTAMP(3),
  window_end         TIMESTAMP(3),
  alert_type         STRING
) WITH (
  'connector' = 'confluent',
  'value.format' = 'avro-registry'
);