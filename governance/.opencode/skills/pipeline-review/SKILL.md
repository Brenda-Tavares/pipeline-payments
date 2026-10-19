---
name: pipeline-review
description: Checklist e veredito binário sobre artefatos de CODE.
---
1. Veredito APROVA/REPROVA com itens numerados arquivo:linha; nunca reescrever código.
2. Checklist: BACKWARD nos dois sentidos; c/u/d + tombstone; REPLICA IDENTITY FULL; heartbeat;
   ACL menor privilégio; janela 60s + card_id; PK nos dois lados; idle-timeout; sem secrets;
   idempotência do consumer declarada.
3. Dúvida semântica profunda → abrir RT para ORCH em vez de reprovar às cegas.