-- seed/seed.sql — carga determinística (seed=42) para o pipeline de pagamentos.
-- Executar no Postgres (Neon ou similar) após criar o banco.
-- Determinismo: SELECT setseed(0.42) no início; todas as séries usam generate_series.

\set ON_ERROR_STOP on

-- ------------------------------------------------------------------
-- 1. Usuário de CDC com menor privilégio 
-- ------------------------------------------------------------------
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'cdc_user') THEN
    CREATE ROLE cdc_user WITH LOGIN REPLICATION PASSWORD 'REPLACE_WITH_SECURE_PASSWORD';
  END IF;
END
$$;

GRANT USAGE ON SCHEMA public TO cdc_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO cdc_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO cdc_user;

-- ------------------------------------------------------------------
-- 2. Tabelas
-- ------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS customers (
  customer_id VARCHAR(36) PRIMARY KEY,
  full_name   VARCHAR(100) NOT NULL,
  email       VARCHAR(100) NOT NULL,
  phone       VARCHAR(20),
  created_at  TIMESTAMP NOT NULL DEFAULT now(),
  updated_at  TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS accounts (
  account_id   VARCHAR(36) PRIMARY KEY,
  customer_id  VARCHAR(36) NOT NULL REFERENCES customers(customer_id),
  account_type VARCHAR(10) NOT NULL CHECK (account_type IN ('CHECKING','SAVINGS')),
  balance      NUMERIC(15,2) NOT NULL DEFAULT 0,
  currency     VARCHAR(3) NOT NULL DEFAULT 'USD',
  status       VARCHAR(10) NOT NULL CHECK (status IN ('ACTIVE','BLOCKED','CLOSED')),
  created_at   TIMESTAMP NOT NULL DEFAULT now(),
  updated_at   TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS cards (
  card_id      VARCHAR(36) PRIMARY KEY,
  account_id   VARCHAR(36) NOT NULL REFERENCES accounts(account_id),
  card_number  VARCHAR(19) NOT NULL,
  cvv          VARCHAR(4),
  expiry_month INT NOT NULL,
  expiry_year  INT NOT NULL,
  status       VARCHAR(10) NOT NULL CHECK (status IN ('ACTIVE','BLOCKED','EXPIRED')),
  created_at   TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS merchants (
  merchant_id VARCHAR(36) PRIMARY KEY,
  name        VARCHAR(100) NOT NULL,
  category    VARCHAR(50) NOT NULL,
  country     VARCHAR(3) NOT NULL,
  created_at  TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS transactions (
  transaction_id   VARCHAR(36) PRIMARY KEY,
  card_id          VARCHAR(36) NOT NULL REFERENCES cards(card_id),
  merchant_id      VARCHAR(36) NOT NULL REFERENCES merchants(merchant_id),
  amount           NUMERIC(15,2) NOT NULL,
  currency         VARCHAR(3) NOT NULL DEFAULT 'USD',
  transaction_type VARCHAR(12) NOT NULL CHECK (transaction_type IN ('PURCHASE','REFUND','WITHDRAWAL')),
  status           VARCHAR(10) NOT NULL CHECK (status IN ('APPROVED','DECLINED','PENDING')),
  created_at       TIMESTAMP NOT NULL DEFAULT now()
);

-- REPLICA IDENTITY FULL para popular "before" no UPDATE (CDC)
ALTER TABLE customers   REPLICA IDENTITY FULL;
ALTER TABLE accounts    REPLICA IDENTITY FULL;
ALTER TABLE cards       REPLICA IDENTITY FULL;
ALTER TABLE merchants   REPLICA IDENTITY FULL;
ALTER TABLE transactions REPLICA IDENTITY FULL;

-- Publicação para o conector Debezium
DROP PUBLICATION IF EXISTS desafio_cdc_pub;
CREATE PUBLICATION desafio_cdc_pub FOR TABLE
  customers, accounts, cards, merchants, transactions;

-- Tabela de heartbeat do Debezium
CREATE TABLE IF NOT EXISTS debezium_heartbeat (
  id VARCHAR(255) PRIMARY KEY,
  ts TIMESTAMP NOT NULL DEFAULT now()
);
GRANT SELECT, INSERT, UPDATE ON debezium_heartbeat TO cdc_user;

-- ------------------------------------------------------------------
-- 3. Carga determinística
-- ------------------------------------------------------------------
SELECT setseed(0.42);

-- merchants (60)
INSERT INTO merchants (merchant_id, name, category, country, created_at)
SELECT
  'm-' || lpad(i::text, 4, '0'),
  'Merchant ' || i,
  (ARRAY['GROCERY','FUEL','ONLINE','RESTAURANT','TRAVEL'])[1 + (random() * 4)::int],
  (ARRAY['US','BR','CA','MX','GB'])[1 + (random() * 4)::int],
  now() - (random() * 365)::int * interval '1 day'
FROM generate_series(1, 60) AS i;

-- customers (200)
INSERT INTO customers (customer_id, full_name, email, phone, created_at, updated_at)
SELECT
  'c-' || lpad(i::text, 4, '0'),
  'Customer ' || i,
  'customer' || i || '@example.com',
  '+1' || lpad((random() * 9999999999)::bigint::text, 10, '0'),
  now() - (random() * 730)::int * interval '1 day',
  now()
FROM generate_series(1, 200) AS i;

-- accounts (240)
INSERT INTO accounts (account_id, customer_id, account_type, balance, currency, status, created_at, updated_at)
SELECT
  'a-' || lpad(i::text, 4, '0'),
  'c-' || lpad((((i - 1) % 200) + 1)::text, 4, '0'),
  (ARRAY['CHECKING','SAVINGS'])[1 + (random() * 1)::int],
  round((random() * 10000)::numeric, 2),
  'USD',
  (ARRAY['ACTIVE','ACTIVE','ACTIVE','BLOCKED'])[1 + (random() * 3)::int],
  now() - (random() * 700)::int * interval '1 day',
  now()
FROM generate_series(1, 240) AS i;

-- cards (320)
INSERT INTO cards (card_id, account_id, card_number, cvv, expiry_month, expiry_year, status, created_at)
SELECT
  'card-' || lpad(i::text, 4, '0'),
  'a-' || lpad((((i - 1) % 240) + 1)::text, 4, '0'),
  lpad((random() * 9999999999999999)::bigint::text, 16, '0'),
  lpad((random() * 999)::int::text, 3, '0'),
  1 + (random() * 11)::int,
  2027 + (random() * 3)::int,
  (ARRAY['ACTIVE','ACTIVE','ACTIVE','BLOCKED','EXPIRED'])[1 + (random() * 4)::int],
  now() - (random() * 600)::int * interval '1 day'
FROM generate_series(1, 320) AS i;

-- transactions (2000): 10 rajadas para 3 cartões + restante aleatório
-- Rajadas (velocity) para disparar RULE_VELOCITY
INSERT INTO transactions (transaction_id, card_id, account_id, merchant_id, amount, currency, transaction_type, status, created_at)
SELECT
  't-burst-' || c.card || '-' || g.n,
  c.card,
  (SELECT account_id FROM cards WHERE card_id = c.card),
  'm-' || lpad((1 + (random() * 59)::int)::text, 4, '0'),
  round((5 + random() * 95)::numeric, 2),
  'USD',
  'PURCHASE',
  'APPROVED',
  timestamp '2026-10-26 10:00:00' + (g.n * interval '10 seconds')
FROM (VALUES ('card-0001'),('card-0002'),('card-0003')) AS c(card)
CROSS JOIN generate_series(1, 3) AS g(n);

-- Restante aleatório para completar ~2000
INSERT INTO transactions (transaction_id, card_id, account_id, merchant_id, amount, currency, transaction_type, status, created_at)
SELECT
  't-' || lpad(i::text, 5, '0'),
  c.card_id,
  c.account_id,
  m.merchant_id,
  round((1 + random() * 500)::numeric, 2),
  'USD',
  (ARRAY['PURCHASE','PURCHASE','PURCHASE','REFUND','WITHDRAWAL'])[1 + (random() * 4)::int],
  (ARRAY['APPROVED','APPROVED','APPROVED','DECLINED','PENDING'])[1 + (random() * 4)::int],
  now() - (random() * 30)::int * interval '1 day' - (random() * 86400)::int * interval '1 second'
FROM generate_series(1, 1991) AS i
CROSS JOIN LATERAL (SELECT card_id, account_id FROM cards ORDER BY random() LIMIT 1) AS c
CROSS JOIN LATERAL (SELECT merchant_id FROM merchants ORDER BY random() LIMIT 1) AS m;

-- ------------------------------------------------------------------
-- 4. Verificação rápida
-- ------------------------------------------------------------------
SELECT 'customers' AS tbl, count(*) FROM customers
UNION ALL SELECT 'accounts', count(*) FROM accounts
UNION ALL SELECT 'cards', count(*) FROM cards
UNION ALL SELECT 'merchants', count(*) FROM merchants
UNION ALL SELECT 'transactions', count(*) FROM transactions;