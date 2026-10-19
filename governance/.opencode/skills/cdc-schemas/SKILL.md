---
name: cdc-schemas
description: CDC Debezium/Postgres e contratos Avro: privilégios, heartbeat, envelopes c/u/d, evolução BACKWARD.
---
1. Conector com cdc_user de menor privilégio; heartbeat.interval.ms=60000; secrets via env, nunca no JSON.
2. Postgres com REPLICA IDENTITY FULL para before populado no UPDATE.
3. Avro: money como bytes + logicalType decimal(15,2); namespace com.bootcamp.payments.*.
4. Evidência: envelopes op=c, op=u e op=d com tombstone na sequência.