---
name: streaming-arch
description: Decisões de arquitetura Confluent (CDC, Schema Registry, ACL, Flink, custos, PII/PCI) e narrativa do README.
---
1. Justifique cada peça: CDC vs batch; janela de 60s; Avro + Schema Registry; dois service accounts.
2. Segurança: menor privilégio por SA; tags PII/PCI nos campos sensíveis; secrets só via env.
3. Custos: registre alavancas (tipo de cluster, retenção, idle timeout) e meça via billing cost list.
4. README: camada → decisão → evidência → como reproduzir → custo.