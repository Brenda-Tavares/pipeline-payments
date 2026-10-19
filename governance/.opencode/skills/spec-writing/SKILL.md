---
name: spec-writing
description: Specs e critérios de aceitação por camada; redige prompts delegados para CODE e REVIEW.
---
1. Toda spec tem: Objetivo | Entradas/Saídas | Tópicos e formatos | Critérios mensuráveis | Evidência exigida | Prompt CODE | Prompt REVIEW.
2. Critério mensurável = comando + saída esperada. Proibido "funciona".
3. Spec não contém código executável nem secrets; placeholders ${VAR}.
4. Uma spec por camada; mudança de escopo vira RT antes de reescrever.