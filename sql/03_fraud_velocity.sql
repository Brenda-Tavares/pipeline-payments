-- RULE_VELOCITY: 3+ transações do mesmo cartão em 60 segundos.

INSERT INTO `desafio.fraud.detected` (card_id, transaction_count, first_event, last_event, window_end, alert_type)
SELECT
  card_id,
  CAST(cnt AS INT) AS transaction_count,
  first_ts AS first_event,
  last_ts AS last_event,
  last_ts AS window_end,
  'RULE_VELOCITY' AS alert_type
FROM `desafio-.public.transactions`
MATCH_RECOGNIZE (
  PARTITION BY card_id
  ORDER BY created_at
  MEASURES
    FIRST(T.created_at) AS first_ts,
    LAST(T.created_at)  AS last_ts,
    COUNT(T.transaction_id) AS cnt
  ONE ROW PER MATCH
  AFTER MATCH SKIP TO LAST T
  PATTERN (T{3,}?) WITHIN INTERVAL '60' SECOND
  DEFINE
    T AS TRUE
);