---
name: flink-sql-build
description: Padrões Flink SQL do pipeline: temporal join, MATCH_RECOGNIZE, sinks Avro.
---
1. Temporal join: PK declarada nos dois lados + sql.tables.scan.idle-timeout=5s no lookup.
2. Fraude: MATCH_RECOGNIZE com PARTITION BY card_id, PATTERN (T{3,}?) WITHIN INTERVAL '60' SECOND.
3. Sink sempre Avro/Schema Registry; nenhuma credencial em DDL (usar ${VAR}).
4. Entregue com comando de submit e saída esperada do statement.