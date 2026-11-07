-- Temporal join: transações ⋈ contas (PK nos dois lados).

CREATE VIEW enriched_transactions AS
SELECT
  t.transaction_id,
  t.card_id,
  t.account_id,
  t.merchant_id,
  t.amount,
  t.currency,
  t.transaction_type,
  t.status,
  t.created_at,
  a.account_id   AS joined_account_id,
  a.balance      AS account_balance,
  a.status       AS account_status
FROM `desafio-.public.transactions` AS t
LEFT JOIN `desafio-.public.accounts` FOR SYSTEM_TIME AS OF t.created_at AS a
  ON t.account_id = a.account_id;