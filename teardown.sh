#!/usr/bin/env bash
set -euo pipefail

# ==============================================================================
# teardown.sh — SPEC-C1 v2.2 · Camada 1: Fundação · desmontagem REST v3 (RT-008)
# Pin de API (AC-01): REST API v3 consultada em
#   docs.confluent.io/cloud/current/api.html em 2026-10-23.
#
# Regras aplicadas neste script:
#  - Ordem INVERSA da §3.3: tópico → ACLs → role bindings → SAs (cascata de keys)
#    → pool Flink → cluster (async) → environment (cascata SR) → confirmação final.
#  - Atua SOMENTE pelos recursos registrados no .env (ou resolvidos por nome/
#    principal dos recursos registrados) — nunca loop wildcard sobre listagens.
#  - TODO curl usa -u (Basic), -f/--fail (HTTP 4xx/5xx → exit ≠ 0; PATCH 14) e
#    -w '%{http_code}'. Janela de sucesso = 2xx (200/202/204 — D3).
#  - Inexistente (404) = "JÁ REMOVIDO" (idempotente; 2ª execução não falha).
#  - M1/M2 NÃO são destruídos: keys admin do Console sobrevivem ao ciclo.
#  - Zero secret em log: nenhum valor do .env é exibido por este script.
#  - Zero Confluent CLI: somente REST via curl (constraint RT-008).
#  - Ambiente: Git Bash no Windows 10 (RT-004); arquivo em LF.
# ==============================================================================

readonly API_BASE="https://api.confluent.cloud"
readonly ENV_FILE=".env"
readonly SA_PRODUCER="desafio-producer"
readonly SA_CONSUMER="desafio-consumer"
readonly POOL_NAME="desafio-flink"
readonly TOPIC_SMOKE="desafio-smoke.v1"

readonly REQUIRED_VARS=(
  CLOUD_API_KEY CLOUD_API_SECRET KAFKA_ADMIN_KEY KAFKA_ADMIN_SECRET
  CONFLUENT_ENV_ID CONFLUENT_KAFKA_CLUSTER_ID CONFLUENT_KAFKA_BOOTSTRAP
  CONFLUENT_KAFKA_REST_URL CONFLUENT_SR_CLUSTER_ID CONFLUENT_SR_URL FLINK_POOL_ID
  KAFKA_KEY_PRODUCER KAFKA_SECRET_PRODUCER KAFKA_KEY_CONSUMER KAFKA_SECRET_CONSUMER
  SR_KEY_PRODUCER SR_SECRET_PRODUCER SR_KEY_CONSUMER SR_SECRET_CONSUMER
)

ok()    { printf '  %s\n' "$*"; }
fail()  { printf '\nFALHA: %s\n' "$*" >&2; exit 1; }
etapa() { printf '\n=== ETAPA %s — %s ===\n' "$1" "$2"; }

# ---------- .env (nomes; valores lidos só no ponto de uso) --------------------

load_env() {
  [[ -f "$ENV_FILE" ]] || fail "$ENV_FILE ausente — o teardown atua apenas pelos recursos registrados nele"
  chmod 600 "$ENV_FILE" 2>/dev/null || true
  local bad_line
  bad_line=$(grep -nvE '^[[:space:]]*(#|$)|^[A-Za-z_][A-Za-z0-9_]*=' "$ENV_FILE" | head -n1 | cut -d: -f1 || true)
  if [[ -n "$bad_line" ]]; then
    fail "$ENV_FILE linha $bad_line fora do formato NOME=valor (linha não exibida por segurança)"
  fi
  set -a
  # shellcheck disable=SC1090
  . <(sed 's/\r$//' "$ENV_FILE")
  set +a
}

check_env_names() {
  local name missing=0
  for name in "${REQUIRED_VARS[@]}"; do
    if ! grep -qE "^${name}=" "$ENV_FILE"; then
      printf '  FALHA: %s não declarado no .env\n' "$name" >&2
      missing=1
    fi
  done
  if (( missing != 0 )); then
    fail "pré-checagem por NOMES falhou — teardown exige as 19 variáveis declaradas"
  fi
  ok "pré-checagem por nomes: 19 variáveis declaradas no .env"
}

# ---------- extratores fail-closed (só o campo JSON "id") ---------------------

list_values() { printf '%s' "$1" | grep -oE "$2" || true; }

id_field_value() { # <corpo> <prefixo do id> → só o campo JSON "id"
  list_values "$1" "\"id\":\"$2[a-z0-9]+\"" | sed -E 's/^"id":"//; s/"$//'
}

LOOKUP_ID=""
lookup_id_by_name() { # <corpo> <prefixo> <display_name> → define LOOKUP_ID; rc 1 = ausente
  local body="$1" prefix="$2" wanted="$3" i
  local -a ids names
  LOOKUP_ID=""
  mapfile -t ids   < <(id_field_value "$body" "$prefix")
  mapfile -t names < <(list_values "$body" '"display_name":"[^"]*"' | sed -E 's/^"display_name":"//; s/"$//')
  if [[ ${#ids[@]} -ne ${#names[@]} ]]; then
    fail "listagem com ${#ids[@]} ids × ${#names[@]} display_names — extração fail-closed (§3.4)"
  fi
  for (( i = 0; i < ${#ids[@]}; i++ )); do
    if [[ "${names[$i]}" == "$wanted" ]]; then
      LOOKUP_ID="${ids[$i]}"
      return 0
    fi
  done
  return 1
}

# ---------- transporte HTTP (curl -f em TODO curl) ---------------------------

curl_capture() { # <METHOD> <URL> <USER> <PASS> [JSON]
  local method="$1" url="$2" user="$3" pass="$4" data="${5-}" raw rc
  if [[ -n "$data" ]]; then
    raw=$(curl -sS -f -X "$method" -u "$user:$pass" \
      -H 'Content-Type: application/json' --data "$data" \
      -w '\n%{http_code}' "$url") && rc=0 || rc=$?
  else
    raw=$(curl -sS -f -X "$method" -u "$user:$pass" -w '\n%{http_code}' "$url") && rc=0 || rc=$?
  fi
  CURL_RC=$rc
  if [[ "$raw" == *$'\n'* ]]; then
    C_CODE="${raw##*$'\n'}"
    C_BODY="${raw%$'\n'*}"
  else
    C_CODE="${raw:-000}"
    C_BODY=""
  fi
  [[ "$C_CODE" =~ ^[0-9]{3}$ ]] || C_CODE="000"
  return 0
}

http() { # estrito: 4xx/5xx = FALHA
  curl_capture "$@"
  if [[ "$CURL_RC" -ne 0 ]] || [[ ! "$C_CODE" =~ ^2[0-9][0-9]$ ]]; then
    fail "HTTP $1 falhou (curl rc=$CURL_RC, HTTP $C_CODE): ${2%%\?*}"
  fi
  HTTP_CODE="$C_CODE"
  HTTP_BODY="$C_BODY"
}

http_probe() { # 0 = 2xx · 1 = 404 · 2 = outro erro
  curl_capture "$@"
  HTTP_CODE="$C_CODE"
  HTTP_BODY="$C_BODY"
  if [[ "$CURL_RC" -eq 0 ]] && [[ "$C_CODE" =~ ^2[0-9][0-9]$ ]]; then return 0; fi
  if [[ "$C_CODE" == "404" ]]; then return 1; fi
  return 2
}

delete_call() { # <url> <user> <pass> <descrição> — janela 2xx; 404 = JÁ REMOVIDO
  local url="$1" user="$2" pass="$3" desc="$4"
  curl_capture DELETE "$url" "$user" "$pass"
  if [[ "$CURL_RC" -eq 0 ]] && [[ "$C_CODE" =~ ^2[0-9][0-9]$ ]]; then
    printf '  REMOVIDO: %s (HTTP %s)\n' "$desc" "$C_CODE"
    return 0
  fi
  if [[ "$C_CODE" == "404" ]]; then
    printf '  JÁ REMOVIDO: %s (HTTP 404)\n' "$desc"
    return 0
  fi
  fail "DELETE de '$desc' falhou (HTTP $C_CODE, curl rc=$CURL_RC) — ${url%%\?*}"
}

# ==============================================================================
# ETAPA 1 — Tópico smoke
# ==============================================================================
etapa 1 "Tópico $TOPIC_SMOKE"
load_env
check_env_names

if [[ -n "${CONFLUENT_KAFKA_REST_URL:-}" && -n "${CONFLUENT_KAFKA_CLUSTER_ID:-}" ]]; then
  delete_call \
    "${CONFLUENT_KAFKA_REST_URL}/kafka/v3/clusters/${CONFLUENT_KAFKA_CLUSTER_ID}/topics/${TOPIC_SMOKE}" \
    "${KAFKA_ADMIN_KEY:?Defina KAFKA_ADMIN_KEY no .env}" \
    "${KAFKA_ADMIN_SECRET:?Defina KAFKA_ADMIN_SECRET no .env}" \
    "tópico $TOPIC_SMOKE"
else
  ok "SEM REGISTRO no .env (REST URL/cluster) — etapa do tópico pulada"
fi

# ==============================================================================
# ETAPA 2 — ACLs Kafka (5; principal resolvido pelo display_name do SA)
# ==============================================================================
etapa 2 "ACLs Kafka (5) — principal User:sa-…"
SA_ID_PRODUCER=""
SA_ID_CONSUMER=""
if [[ -n "${CLOUD_API_KEY:-}" && -n "${CLOUD_API_SECRET:-}" ]]; then
  http GET "$API_BASE/iam/v2/service-accounts?page_size=100" \
    "${CLOUD_API_KEY}" "${CLOUD_API_SECRET}"
  lookup_id_by_name "$HTTP_BODY" 'sa-' "$SA_PRODUCER" || true
  SA_ID_PRODUCER="$LOOKUP_ID"
  lookup_id_by_name "$HTTP_BODY" 'sa-' "$SA_CONSUMER" || true
  SA_ID_CONSUMER="$LOOKUP_ID"
fi

if [[ -n "$SA_ID_PRODUCER" || -n "$SA_ID_CONSUMER" ]] && [[ -n "${CONFLUENT_KAFKA_REST_URL:-}" ]]; then
  acl_base="${CONFLUENT_KAFKA_REST_URL}/kafka/v3/clusters/${CONFLUENT_KAFKA_CLUSTER_ID}/acls"
  delete_acl() { # <resource_type> <operation> <sa id>
    local rtype="$1" op="$2" sa_id="$3"
    delete_call "${acl_base}?resource_type=${rtype}&resource_name=desafio-&pattern_type=PREFIXED&principal=User%3A${sa_id}&host=%2A&operation=${op}&permission=ALLOW" \
      "${KAFKA_ADMIN_KEY:?Defina KAFKA_ADMIN_KEY no .env}" \
      "${KAFKA_ADMIN_SECRET:?Defina KAFKA_ADMIN_SECRET no .env}" \
      "ACL $op $rtype PREFIXED desafio- (User:$sa_id)"
  }
  if [[ -n "$SA_ID_PRODUCER" ]]; then
    delete_acl TOPIC CREATE "$SA_ID_PRODUCER"
    delete_acl TOPIC WRITE "$SA_ID_PRODUCER"
    delete_acl TOPIC DESCRIBE "$SA_ID_PRODUCER"
  fi
  if [[ -n "$SA_ID_CONSUMER" ]]; then
    delete_acl TOPIC READ "$SA_ID_CONSUMER"
    delete_acl GROUP READ "$SA_ID_CONSUMER"
  fi
else
  ok "SAs ausentes (já removidos) ou sem REST URL — ACLs órfãs não são resolvíveis; nada a deletar por esta via"
fi

# ==============================================================================
# ETAPA 3 — Role bindings do Schema Registry
# ==============================================================================
etapa 3 "Role bindings SR (subject=desafio-*)"
if [[ -n "$SA_ID_PRODUCER" || -n "$SA_ID_CONSUMER" ]]; then
  http GET "$API_BASE/iam/v2/role-bindings?page_size=100" \
    "${CLOUD_API_KEY:?Defina CLOUD_API_KEY no .env}" \
    "${CLOUD_API_SECRET:?Defina CLOUD_API_SECRET no .env}"
  mapfile -t rb_all_ids < <(id_field_value "$HTTP_BODY" 'rb-')
  mapfile -t rb_prins    < <(list_values "$HTTP_BODY" '"principal":"[^"]+"')
  if [[ ${#rb_all_ids[@]} -ne ${#rb_prins[@]} ]]; then
    fail "listagem de role bindings com contagens divergentes — extração fail-closed (§3.4)"
  fi
  rb_ids=""
  for (( i = 0; i < ${#rb_all_ids[@]}; i++ )); do
    for sa_id in "$SA_ID_PRODUCER" "$SA_ID_CONSUMER"; do
      [[ -z "$sa_id" ]] && continue
      if [[ "${rb_prins[$i]}" == "\"principal\":\"User:$sa_id\"" ]]; then
        rb_ids="$rb_ids ${rb_all_ids[$i]}"
      fi
    done
  done
  if [[ -z "$rb_ids" ]]; then
    ok "JÁ REMOVIDO: nenhum binding dos SAs na listagem"
  else
    for rb_id in $rb_ids; do
      delete_call "$API_BASE/iam/v2/role-bindings/$rb_id" \
        "${CLOUD_API_KEY:?Defina CLOUD_API_KEY no .env}" \
        "${CLOUD_API_SECRET:?Defina CLOUD_API_SECRET no .env}" \
        "role binding $rb_id"
    done
  fi
else
  ok "SAs ausentes (já removidos) — bindings não são resolvíveis pelo principal; nada a deletar por esta via"
fi

# ==============================================================================
# ETAPA 4 — Service accounts (cascata: keys do SA são excluídas com o principal)
# ==============================================================================
etapa 4 "Service accounts (cascata de keys)"
if [[ -n "$SA_ID_PRODUCER" ]]; then
  delete_call "$API_BASE/iam/v2/service-accounts/$SA_ID_PRODUCER" \
    "${CLOUD_API_KEY:?Defina CLOUD_API_KEY no .env}" \
    "${CLOUD_API_SECRET:?Defina CLOUD_API_SECRET no .env}" \
    "service account $SA_PRODUCER ($SA_ID_PRODUCER)"
else
  ok "JÁ REMOVIDO: $SA_PRODUCER não está na organização"
fi
if [[ -n "$SA_ID_CONSUMER" ]]; then
  delete_call "$API_BASE/iam/v2/service-accounts/$SA_ID_CONSUMER" \
    "${CLOUD_API_KEY:?Defina CLOUD_API_KEY no .env}" \
    "${CLOUD_API_SECRET:?Defina CLOUD_API_SECRET no .env}" \
    "service account $SA_CONSUMER ($SA_ID_CONSUMER)"
else
  ok "JÁ REMOVIDO: $SA_CONSUMER não está na organização"
fi

# ==============================================================================
# ETAPA 5 — Compute pool Flink
# ==============================================================================
etapa 5 "Compute pool $POOL_NAME"
if [[ -n "${FLINK_POOL_ID:-}" ]]; then
  delete_call \
    "$API_BASE/fcpm/v2/compute-pools/${FLINK_POOL_ID}?environment=${CONFLUENT_ENV_ID:?Defina CONFLUENT_ENV_ID no .env}" \
    "${CLOUD_API_KEY:?Defina CLOUD_API_KEY no .env}" \
    "${CLOUD_API_SECRET:?Defina CLOUD_API_SECRET no .env}" \
    "compute pool $POOL_NAME ($FLINK_POOL_ID)"
else
  ok "JÁ REMOVIDO: FLINK_POOL_ID vazio no .env"
fi

# ==============================================================================
# ETAPA 6 — Cluster Kafka (DELETE 202 + re-read até 404)
# ==============================================================================
etapa 6 "Cluster Kafka (assíncrono)"
if [[ -n "${CONFLUENT_KAFKA_CLUSTER_ID:-}" ]]; then
  delete_call "$API_BASE/cmk/v2/clusters/$CONFLUENT_KAFKA_CLUSTER_ID" \
    "${CLOUD_API_KEY:?Defina CLOUD_API_KEY no .env}" \
    "${CLOUD_API_SECRET:?Defina CLOUD_API_SECRET no .env}" \
    "cluster $CONFLUENT_KAFKA_CLUSTER_ID"
  attempt=0
  while :; do
    if http_probe GET "$API_BASE/cmk/v2/clusters/$CONFLUENT_KAFKA_CLUSTER_ID" \
        "${CLOUD_API_KEY:?Defina CLOUD_API_KEY no .env}" \
        "${CLOUD_API_SECRET:?Defina CLOUD_API_SECRET no .env}"; then
      : # ainda existe — segue aguardando
    elif [[ "$HTTP_CODE" == "404" ]]; then
      ok "CONFIRMADO 404: cluster $CONFLUENT_KAFKA_CLUSTER_ID removido"
      break
    else
      fail "re-read do cluster retornou HTTP $HTTP_CODE"
    fi
    attempt=$((attempt + 1))
    if (( attempt >= 40 )); then
      fail "timeout 40×15 s: cluster $CONFLUENT_KAFKA_CLUSTER_ID ainda visível após DELETE"
    fi
    printf '  aguardando remoção do cluster... (%d/40)\n' "$attempt"
    sleep 15
  done
else
  ok "JÁ REMOVIDO: CONFLUENT_KAFKA_CLUSTER_ID vazio no .env"
fi

# ==============================================================================
# ETAPA 7 — Environment (cascata remove SR e demais serviços do env)
# ==============================================================================
etapa 7 "Environment (cascata incluindo o Schema Registry)"
if [[ -n "${CONFLUENT_ENV_ID:-}" ]]; then
  delete_call "$API_BASE/org/v2/environments/$CONFLUENT_ENV_ID" \
    "${CLOUD_API_KEY:?Defina CLOUD_API_KEY no .env}" \
    "${CLOUD_API_SECRET:?Defina CLOUD_API_SECRET no .env}" \
    "environment $CONFLUENT_ENV_ID"
else
  ok "JÁ REMOVIDO: CONFLUENT_ENV_ID vazio no .env"
fi

# ==============================================================================
# ETAPA 8 — Confirmação final (re-read 404 / ausência por id)
# ==============================================================================
etapa 8 "Confirmação final"
cloud_u="${CLOUD_API_KEY:?Defina CLOUD_API_KEY no .env}"
cloud_p="${CLOUD_API_SECRET:?Defina CLOUD_API_SECRET no .env}"

if [[ -n "${CONFLUENT_ENV_ID:-}" ]]; then
  attempt=0
  while :; do
    curl_capture GET "$API_BASE/org/v2/environments/$CONFLUENT_ENV_ID" "$cloud_u" "$cloud_p"
    if [[ "$C_CODE" == "404" ]]; then break; fi
    if [[ "$CURL_RC" -eq 0 ]] && [[ "$C_CODE" =~ ^2[0-9][0-9]$ ]]; then
      attempt=$((attempt + 1))
      if (( attempt >= 20 )); then
        fail "environment $CONFLUENT_ENV_ID ainda existe (HTTP $C_CODE) — teardown incompleto"
      fi
      printf '  aguardando 404 do environment... (%d/20)\n' "$attempt"
      sleep 10
    else
      fail "re-read do environment retornou HTTP $C_CODE"
    fi
  done
  ok "CONFIRMADO 404: environment $CONFLUENT_ENV_ID"
fi

http GET "$API_BASE/iam/v2/service-accounts?page_size=100" "$cloud_u" "$cloud_p"
for sa_name in "$SA_PRODUCER" "$SA_CONSUMER"; do
  if printf '%s' "$HTTP_BODY" | grep -qF "\"display_name\":\"$sa_name\""; then
    fail "service account $sa_name ainda existe na organização"
  fi
  ok "CONFIRMADO ausente: service account $sa_name"
done

http GET "$API_BASE/iam/v2/api-keys?page_size=100" "$cloud_u" "$cloud_p"
for key_var in KAFKA_KEY_PRODUCER KAFKA_KEY_CONSUMER SR_KEY_PRODUCER SR_KEY_CONSUMER; do
  key_val="${!key_var:-}"
  [[ -z "$key_val" ]] && continue
  if printf '%s' "$HTTP_BODY" | grep -qF "\"id\":\"$key_val\""; then
    fail "API key $key_var ($key_val) ainda existe — cascata de keys não ocorreu"
  fi
  ok "CONFIRMADO ausente: $key_var (id registrado não aparece na listagem)"
done

http GET "$API_BASE/srcm/v3/clusters?page_size=100" "$cloud_u" "$cloud_p"
if [[ -n "${CONFLUENT_SR_CLUSTER_ID:-}" ]]; then
  if printf '%s' "$HTTP_BODY" | grep -qF "\"id\":\"$CONFLUENT_SR_CLUSTER_ID\""; then
    fail "Schema Registry $CONFLUENT_SR_CLUSTER_ID ainda existe (cascata do env não ocorreu)"
  fi
  ok "CONFIRMADO: $CONFLUENT_SR_CLUSTER_ID ausente de GET /srcm/v3/clusters"
else
  ok "SEM REGISTRO: CONFLUENT_SR_CLUSTER_ID vazio — confirmação de SR pulada"
fi

printf '\n=== TEARDOWN CONCLUÍDO ===\n'
ok "ordem executada: tópico → ACLs → bindings → SAs → pool → cluster → environment"
ok "M1/M2 preservados (keys admin do Console sobrevivem ao ciclo)"
ok "os pares de keys de SA no .env ficaram obsoletos — o próximo setup.sh os substitui (JÁ EXISTE/CRIADO)"
printf 'Nota: re-run de teardown é idempotente (404 = JÁ REMOVIDO).\n'

