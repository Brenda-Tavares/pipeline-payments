#!/usr/bin/env bash
set -euo pipefail

# ==============================================================================
# setup.sh — SPEC-C1 v2.2 · Camada 1: Fundação · REST API v3 via curl (RT-008)
# Pin de API (AC-01): REST API v3 consultada em docs.confluent.io/cloud/current/api.html em 2026-10-23.
#
# Regras aplicadas neste script:
#  - 10 etapas idempotentes (§3.2): recurso existe → "JÁ EXISTE"; senão → "CRIADO: <id>".
#  - TODO curl usa -u (Basic), -f/--fail (HTTP 4xx/5xx = FALHA fechada — PATCH 14)
#    e -w '%{http_code}'.
#  - Zero secret em log/terminal: o secret de POST /iam/v2/api-keys é retornado
#    UMA única vez e é gravado direto no .env local (LLM02).
#  - Extratores fail-closed por gramática de ID (sem jq; jq só inspeção humana).
#  - Zero Confluent CLI: somente REST via curl (constraint RT-008).
#  - UMA pausa manual (M2+M3 no Console) imediatamente após a etapa 3 (PATCH 15).
#  - Ambiente: Git Bash no Windows 10 (RT-004); arquivo em LF.
# ==============================================================================

readonly API_BASE="https://api.confluent.cloud"
readonly ENV_FILE=".env"
readonly ENV_NAME="desafio-env"
readonly CLUSTER_NAME="desafio-kafka"
readonly POOL_NAME="desafio-flink"
readonly SA_PRODUCER="desafio-producer"
readonly SA_CONSUMER="desafio-consumer"
readonly TOPIC_SMOKE="desafio-smoke.v1"
readonly REGION="us-east-1"
readonly PROVIDER="AWS"

readonly REQUIRED_VARS=(
  CLOUD_API_KEY CLOUD_API_SECRET KAFKA_ADMIN_KEY KAFKA_ADMIN_SECRET
  CONFLUENT_ENV_ID CONFLUENT_KAFKA_CLUSTER_ID CONFLUENT_KAFKA_BOOTSTRAP
  CONFLUENT_KAFKA_REST_URL CONFLUENT_SR_CLUSTER_ID CONFLUENT_SR_URL FLINK_POOL_ID
  KAFKA_KEY_PRODUCER KAFKA_SECRET_PRODUCER KAFKA_KEY_CONSUMER KAFKA_SECRET_CONSUMER
  SR_KEY_PRODUCER SR_SECRET_PRODUCER SR_KEY_CONSUMER SR_SECRET_CONSUMER
)

log()   { printf '%s\n' "$*"; }
ok()    { printf '  %s\n' "$*"; }
fail()  { printf '\nFALHA: %s\n' "$*" >&2; exit 1; }
etapa() { printf '\n=== ETAPA %s — %s ===\n' "$1" "$2"; }

# ---------- .env (nomes, sem ler valores na pré-checagem) ---------------------

load_env() {
  [[ -f "$ENV_FILE" ]] || fail "$ENV_FILE ausente — rode: cp .env.example .env"
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
    fail "pré-checagem por NOMES falhou — as 19 variáveis de .env.example devem existir no .env"
  fi
  ok "pré-checagem por nomes: 19 variáveis declaradas no .env (valores não são lidos aqui)"
}

set_env_var() { # grava/replace NOME=valor sem ecoar o valor
  local name="$1" value="$2" line found=0
  local -a out=()
  while IFS= read -r line || [[ -n "$line" ]]; do
    if [[ "$line" == "$name="* ]]; then
      out+=("$name=$value")
      found=1
    else
      out+=("$line")
    fi
  done < "$ENV_FILE"
  (( found )) || out+=("$name=$value")
  printf '%s\n' "${out[@]}" > "$ENV_FILE.tmp.$$"
  mv -f "$ENV_FILE.tmp.$$" "$ENV_FILE"
  chmod 600 "$ENV_FILE" 2>/dev/null || true
}

persist() { # <NOME_VAR> <valor não secreto> → .env + variável da sessão
  set_env_var "$1" "$2"
  printf -v "$1" '%s' "$2"
  ok "GRAVADO no .env: $1"
}

# ---------- extratores fail-closed (gramática de ID; §4.1) --------------------

list_values() { printf '%s' "$1" | grep -oE "$2" || true; }

id_field_value() { # <corpo> <prefixo do id> → só o campo JSON "id" (metadata.self repetiria o id)
  list_values "$1" "\"id\":\"$2[a-z0-9]+\"" | sed -E 's/^"id":"//; s/"$//'
}

grammar() { # <valor> <regex ancorada> <descrição>
  if [[ ! "$1" =~ $2 ]]; then
    fail "captura fail-closed: $3 (valor não casa com a gramática esperada)"
  fi
  printf '%s' "$1"
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

http() { # estrito: qualquer 4xx/5xx (curl -f) = FALHA
  curl_capture "$@"
  if [[ "$CURL_RC" -ne 0 ]] || [[ ! "$C_CODE" =~ ^2[0-9][0-9]$ ]]; then
    fail "HTTP $1 falhou (curl rc=$CURL_RC, HTTP $C_CODE): ${2%%\?*}"
  fi
  HTTP_CODE="$C_CODE"
  HTTP_BODY="$C_BODY"
}

http_probe() { # tolerante: 0 = 2xx · 1 = 404 · 2 = outros erros (quem chama decide)
  curl_capture "$@"
  HTTP_CODE="$C_CODE"
  HTTP_BODY="$C_BODY"
  if [[ "$CURL_RC" -eq 0 ]] && [[ "$C_CODE" =~ ^2[0-9][0-9]$ ]]; then return 0; fi
  if [[ "$C_CODE" == "404" ]]; then return 1; fi
  return 2
}

# ==============================================================================
# ETAPA 1 — Pré-checagens (nomes do .env) + healthcheck AC-01
# ==============================================================================
etapa 1 "Pré-checagens e healthcheck autenticado (AC-01)"
load_env
check_env_names
http GET "$API_BASE/iam/v2/service-accounts?page_size=100" \
  "${CLOUD_API_KEY:?Defina CLOUD_API_KEY no .env}" \
  "${CLOUD_API_SECRET:?Defina CLOUD_API_SECRET no .env}"
[[ "$HTTP_CODE" == "200" ]] || fail "healthcheck: HTTP $HTTP_CODE (esperado 200)"
ok "healthcheck: HTTP 200 — API alcançável e credencial org-admin válida"

# ==============================================================================
# ETAPA 2 — Environment desafio-env (+ org id derivado)
# ==============================================================================
etapa 2 "Environment $ENV_NAME"
cloud_u="${CLOUD_API_KEY:?Defina CLOUD_API_KEY no .env}"
cloud_p="${CLOUD_API_SECRET:?Defina CLOUD_API_SECRET no .env}"

env_id="${CONFLUENT_ENV_ID:-}"
if [[ -n "$env_id" ]] && http_probe GET "$API_BASE/org/v2/environments/$env_id" "$cloud_u" "$cloud_p"; then
  ok "JÁ EXISTE: $env_id (registrado no .env)"
elif [[ -n "$env_id" && "$HTTP_CODE" != "404" ]]; then
  fail "GET environment $env_id retornou HTTP $HTTP_CODE"
else
  http GET "$API_BASE/org/v2/environments?page_size=100" "$cloud_u" "$cloud_p"
  lookup_id_by_name "$HTTP_BODY" 'env-' "$ENV_NAME" || true
  env_id="$LOOKUP_ID"
  if [[ -n "$env_id" ]]; then
    ok "JÁ EXISTE: $env_id (descoberto por display_name)"
  else
    http POST "$API_BASE/org/v2/environments" "$cloud_u" "$cloud_p" \
      "{\"display_name\":\"$ENV_NAME\"}"
    env_id=$(grammar "$(id_field_value "$HTTP_BODY" 'env-' | head -n1)" '^env-[a-z0-9]+$' "environment id")
    ok "CRIADO: $env_id (HTTP $HTTP_CODE)"
  fi
fi
persist CONFLUENT_ENV_ID "$env_id"

http GET "$API_BASE/org/v2/environments/$env_id" "$cloud_u" "$cloud_p"
org_id=$(grammar "$(list_values "$HTTP_BODY" '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}' | head -n1)" \
  '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' "org uuid (derivado do resource_name)")
ok "org id derivado do resource_name (nunca digitado)"

# ==============================================================================
# ETAPA 3 — Cluster Kafka Basic AWS/us-east-1 (POST 202 + polling)
# ==============================================================================
etapa 3 "Cluster $CLUSTER_NAME (Basic · AWS · $REGION · SINGLE_ZONE)"
cluster_id="${CONFLUENT_KAFKA_CLUSTER_ID:-}"
if [[ -n "$cluster_id" ]] && http_probe GET "$API_BASE/cmk/v2/clusters/$cluster_id" "$cloud_u" "$cloud_p"; then
  ok "JÁ EXISTE: $cluster_id (registrado no .env)"
elif [[ -n "$cluster_id" && "$HTTP_CODE" != "404" ]]; then
  fail "GET cluster $cluster_id retornou HTTP $HTTP_CODE"
else
  http GET "$API_BASE/cmk/v2/clusters?environment=$env_id&page_size=100" "$cloud_u" "$cloud_p"
  lookup_id_by_name "$HTTP_BODY" 'lkc-' "$CLUSTER_NAME" || true
  cluster_id="$LOOKUP_ID"
  if [[ -n "$cluster_id" ]]; then
    ok "JÁ EXISTE: $cluster_id (descoberto por display_name)"
  else
    cluster_payload="{\"spec\":{\"display_name\":\"$CLUSTER_NAME\",\"availability\":\"SINGLE_ZONE\",\"cloud\":\"$PROVIDER\",\"region\":\"$REGION\",\"config\":{\"kind\":\"Basic\"},\"environment\":{\"id\":\"$env_id\"}}}"
    http POST "$API_BASE/cmk/v2/clusters" "$cloud_u" "$cloud_p" "$cluster_payload"
    cluster_id=$(grammar "$(id_field_value "$HTTP_BODY" 'lkc-' | head -n1)" '^lkc-[a-z0-9]+$' "cluster id")
    ok "CRIADO: $cluster_id (HTTP $HTTP_CODE — criação assíncrona)"
  fi
fi
persist CONFLUENT_KAFKA_CLUSTER_ID "$cluster_id"

attempt=0
while :; do
  if http_probe GET "$API_BASE/cmk/v2/clusters/$cluster_id" "$cloud_u" "$cloud_p"; then
    # host real: pkc-<id>.<região>.<provider>.confluent.cloud (§4.1 ajustado — drift na evidência p/ REVIEW)
    bootstrap=$(list_values "$HTTP_BODY" 'pkc-[a-z0-9]+(\.[a-z0-9-]+)+\.confluent\.cloud:9092' | head -n1)
    rest_ep=$(list_values "$HTTP_BODY" 'https://pkc-[a-z0-9]+(\.[a-z0-9-]+)+\.confluent\.cloud:443' | head -n1)
    if [[ -z "$rest_ep" ]]; then
      rest_host=$(list_values "$HTTP_BODY" 'pkc-[a-z0-9]+(\.[a-z0-9-]+)+\.confluent\.cloud:443' | head -n1)
      if [[ -n "$rest_host" ]]; then rest_ep="https://$rest_host"; fi
    fi
    if [[ -n "$bootstrap" && -n "$rest_ep" ]]; then break; fi
  elif [[ "$HTTP_CODE" != "404" ]]; then
    fail "GET cluster $cluster_id retornou HTTP $HTTP_CODE durante o polling"
  fi
  attempt=$((attempt + 1))
  if (( attempt >= 40 )); then
    fail "timeout 40×15 s: cluster $cluster_id sem bootstrap_endpoint/rest_endpoint preenchidos"
  fi
  printf '  aguardando cluster ativo... (%d/40)\n' "$attempt"
  sleep 15
done

cluster_region=$(list_values "$HTTP_BODY" '"region":"[a-z0-9-]+"' | head -n1 | sed -E 's/^"region":"//; s/"$//')
bootstrap=$(grammar "$bootstrap" '^pkc-[a-z0-9]+(\.[a-z0-9-]+)+\.confluent\.cloud:9092$' "bootstrap endpoint")
rest_ep=$(grammar "$rest_ep" '^https://pkc-[a-z0-9]+(\.[a-z0-9-]+)+\.confluent\.cloud:443$' "Kafka REST endpoint")
persist CONFLUENT_KAFKA_BOOTSTRAP "$bootstrap"
persist CONFLUENT_KAFKA_REST_URL "$rest_ep"
ok "cluster provisionado: bootstrap 9092 e REST 443 capturados"

# ==============================================================================
# PAUSA ÚNICA (PATCH 15/RT-009) — M2 + M3 no Console, após a etapa 3
# ==============================================================================
printf '\n=== PAUSA MANUAL — M2 + M3 no Console (navegador, não é CLI) ===\n'
cat <<'AVISO'
  M2 — criar a API key de cluster (Cluster admin) do desafio-kafka:
       Cluster > API keys > Add key > escopo no cluster (Kafka API key).
       Copie key/secret para o .env: KAFKA_ADMIN_KEY / KAFKA_ADMIN_SECRET.
  M3 — criar o Schema Registry no environment desafio-env, região us-east-1
       (a REST v3 do SR é de leitura; a criação é manual no Console).
  Feito isso, o script segue: a etapa 4 só DESCOBRE o SR via GET /srcm/v3/clusters.
AVISO
if [[ ! -t 0 ]]; then
  fail "esta pausa exige terminal interativo — rode 'bash setup.sh' no Git Bash sem redirecionar a entrada"
fi
printf 'Pressione ENTER após concluir M2 (par no .env) e M3 (SR em us-east-1): '
read -r _ || fail "pausa interrompida antes de M2/M3"
load_env
cloud_u="${CLOUD_API_KEY:?Defina CLOUD_API_KEY no .env}"
cloud_p="${CLOUD_API_SECRET:?Defina CLOUD_API_SECRET no .env}"
ok "pausa concluída — .env recarregado (M2 incluído); seguindo para a descoberta do SR"

# ==============================================================================
# ETAPA 4 — Schema Registry (criação = M3 no Console; aqui só descoberta)
# ==============================================================================
etapa 4 "Descoberta do Schema Registry (GET /srcm/v3/clusters)"
try=0
sr_id=""
while :; do
  http GET "$API_BASE/srcm/v3/clusters?page_size=100" "$cloud_u" "$cloud_p"
  sr_list_body="$HTTP_BODY"
  mapfile -t sr_all < <(id_field_value "$sr_list_body" 'lsrc-')
  if [[ -n "${CONFLUENT_SR_CLUSTER_ID:-}" && ${#sr_all[@]} -gt 0 ]]; then
    if printf '%s\n' "${sr_all[@]}" | grep -qx "$CONFLUENT_SR_CLUSTER_ID"; then
      sr_id="$CONFLUENT_SR_CLUSTER_ID"
      break
    fi
  fi
  if [[ ${#sr_all[@]} -eq 1 ]]; then
    sr_id="${sr_all[0]}"
    break
  fi
  if [[ ${#sr_all[@]} -gt 1 ]]; then
    fail "encontrados ${#sr_all[@]} Schema Registry clusters na organização — ambiguidade fail-closed; registre o correto em CONFLUENT_SR_CLUSTER_ID e re-rode"
  fi
  try=$((try + 1))
  if (( try > 3 )); then
    fail "nenhum Schema Registry encontrado — confirme M3 no Console (environment $env_id, região $REGION)"
  fi
  printf '  SR ainda não visível (propagação) — re-check %d/3 em 10 s\n' "$try"
  sleep 10
done

sr_id=$(grammar "$sr_id" '^lsrc-[a-z0-9]+$' "schema registry cluster id")
http GET "$API_BASE/srcm/v3/clusters/$sr_id" "$cloud_u" "$cloud_p"
sr_region=$(list_values "$HTTP_BODY" '"region":"[a-z0-9-]+"' | head -n1 | sed -E 's/^"region":"//; s/"$//')
sr_url=$(list_values "$HTTP_BODY" 'https://psrc-[a-z0-9]+\.[a-z0-9.-]+\.confluent\.cloud' | head -n1)
[[ -n "$sr_url" ]] || sr_url=$(list_values "$HTTP_BODY" 'psrc-[a-z0-9]+\.[a-z0-9.-]+\.confluent\.cloud' | head -n1 | sed 's|^|https://|')
if [[ -n "$cluster_region" && "$sr_region" != "$cluster_region" ]]; then
  fail "SR em região '$sr_region' diverge do cluster '$cluster_region' (AC-04 exige idêntica)"
fi
sr_url=$(grammar "$sr_url" '^https://psrc-[a-z0-9]+\.[a-z0-9.-]+\.confluent\.cloud$' "SR REST endpoint")
persist CONFLUENT_SR_CLUSTER_ID "$sr_id"
persist CONFLUENT_SR_URL "$sr_url"
ok "SR descoberto: $sr_id (região $sr_region) — endpoint psrc capturado"

# ==============================================================================
# ETAPA 5 — Service accounts (desafio-producer / desafio-consumer)
# ==============================================================================
etapa 5 "Service accounts ($SA_PRODUCER, $SA_CONSUMER)"
http GET "$API_BASE/iam/v2/service-accounts?page_size=100" "$cloud_u" "$cloud_p"
sa_list_body="$HTTP_BODY"

ensure_sa() { # <nome> <descrição> → ecoa o sa-...
  local name="$1" desc="$2" id payload
  lookup_id_by_name "$sa_list_body" 'sa-' "$name" || true
  id="$LOOKUP_ID"
  if [[ -n "$id" ]]; then
    printf '%s\n' "JÁ EXISTE: $id" >&2
    printf '%s' "$id"
    return 0
  fi
  payload="{\"display_name\":\"$name\",\"description\":\"$desc\"}"
  http POST "$API_BASE/iam/v2/service-accounts" "$cloud_u" "$cloud_p" "$payload"
  id=$(grammar "$(id_field_value "$HTTP_BODY" 'sa-' | head -n1)" '^sa-[a-z0-9]+$' "service account id ($name)")
  printf '%s\n' "CRIADO: $id" >&2
  printf '%s' "$id"
}

SA_ID_PRODUCER=$(ensure_sa "$SA_PRODUCER" "escritor CDC/dados")
SA_ID_CONSUMER=$(ensure_sa "$SA_CONSUMER" "leitor do pipeline")
SA_ID_PRODUCER=$(grammar "$SA_ID_PRODUCER" '^sa-[a-z0-9]+$' "service account id do producer")
SA_ID_CONSUMER=$(grammar "$SA_ID_CONSUMER" '^sa-[a-z0-9]+$' "service account id do consumer")
ok "principals REST: User:$SA_ID_PRODUCER · User:$SA_ID_CONSUMER"

# ==============================================================================
# ETAPA 6 — API keys dos SAs (4 pares; secret gravado só no .env local)
# ==============================================================================
etapa 6 "API keys dos SAs (cluster + SR por SA) — secret não é exibido"
http GET "$API_BASE/iam/v2/api-keys?page_size=100" "$cloud_u" "$cloud_p"
KEYS_LIST_BODY="$HTTP_BODY"

create_sa_key() { # <VAR_KEY> <VAR_SECRET> <display_name> <descrição> <owner_sa> <resource_id>
  local key_var="$1" sec_var="$2" disp="$3" desc="$4" owner="$5" resource="$6"
  local current="${!key_var:-}" payload body key_id secret
  if [[ -n "$current" ]] && printf '%s' "$KEYS_LIST_BODY" | grep -qF "\"id\":\"$current\""; then
    ok "JÁ EXISTE: $key_var ($current)"
    return 0
  fi
  if printf '%s' "$KEYS_LIST_BODY" | grep -qF "\"display_name\":\"$disp\""; then
    fail "API key '$disp' já existe na organização mas não está em $key_var — reconcilie o .env à mão (não criar duplicata)"
  fi
  payload="{\"spec\":{\"display_name\":\"$disp\",\"description\":\"$desc\",\"owner\":{\"id\":\"$owner\"},\"environment\":{\"id\":\"$CONFLUENT_ENV_ID\"},\"resource\":{\"id\":\"$resource\"}}}"
  http POST "$API_BASE/iam/v2/api-keys" "$cloud_u" "$cloud_p" "$payload"
  body="$HTTP_BODY"
  key_id=$(printf '%s' "$body" | sed -nE 's/^\{"id":"([A-Za-z0-9]+)".*/\1/p')
  if [[ -z "$key_id" ]]; then
    key_id=$(list_values "$body" '"id":"[A-Za-z0-9]{8,}"' | head -n1 | sed -E 's/^"id":"//; s/"$//')
  fi
  secret=$(list_values "$body" '"secret":"[^"]+"' | head -n1 | sed -E 's/^"secret":"//; s/"$//')
  if [[ -z "$key_id" || -z "$secret" ]]; then
    fail "POST /iam/v2/api-keys sem id/secret utilizáveis — nada foi gravado (corpo não exibido por segurança)"
  fi
  set_env_var "$key_var" "$key_id"
  set_env_var "$sec_var" "$secret"
  printf -v "$key_var" '%s' "$key_id"
  printf -v "$sec_var" '%s' "$secret"
  secret=""
  body=""
  ok "CRIADO: $key_id ($key_var) — par gravado no .env (secret não exibido)"
}

create_sa_key KAFKA_KEY_PRODUCER KAFKA_SECRET_PRODUCER "desafio-producer-kafka" "producer · produce REST em desafio-*" "$SA_ID_PRODUCER" "$cluster_id"
create_sa_key KAFKA_KEY_CONSUMER KAFKA_SECRET_CONSUMER "desafio-consumer-kafka" "consumer · leitura/ACL de grupo desafio-*" "$SA_ID_CONSUMER" "$cluster_id"
create_sa_key SR_KEY_PRODUCER SR_SECRET_PRODUCER "desafio-producer-sr" "producer · DeveloperWrite em Subject:desafio-*" "$SA_ID_PRODUCER" "$sr_id"
create_sa_key SR_KEY_CONSUMER SR_SECRET_CONSUMER "desafio-consumer-sr" "consumer · DeveloperRead em Subject:desafio-*" "$SA_ID_CONSUMER" "$sr_id"

# ==============================================================================
# ETAPA 7 — ACLs Kafka (exatamente 5, PREFIXED desafio-, 1 valor por POST)
# ==============================================================================
etapa 7 "ACLs Kafka (5 exatas) — data-plane /kafka/v3"
acl_url="${CONFLUENT_KAFKA_REST_URL:?Defina CONFLUENT_KAFKA_REST_URL (gravado pela etapa 3)}/kafka/v3/clusters/${CONFLUENT_KAFKA_CLUSTER_ID:?Defina CONFLUENT_KAFKA_CLUSTER_ID (gravado pela etapa 3)}/acls"
kafka_u="${KAFKA_ADMIN_KEY:?Defina KAFKA_ADMIN_KEY no .env (M2, Console)}"
kafka_p="${KAFKA_ADMIN_SECRET:?Defina KAFKA_ADMIN_SECRET no .env (M2, Console)}"

acl_present() { # <corpo> <resource_type> <operation> <principal>
  local body="$1" rtype="$2" op="$3" princ="$4" i n
  local -a rtypes ops princs perms pats names
  mapfile -t rtypes < <(list_values "$body" '"resource_type":"[A-Z_]+"')
  mapfile -t ops    < <(list_values "$body" '"operation":"[A-Z_]+"')
  mapfile -t princs < <(list_values "$body" '"principal":"[^"]+"')
  mapfile -t perms  < <(list_values "$body" '"permission":"[A-Z]+"')
  mapfile -t pats   < <(list_values "$body" '"pattern_type":"[A-Z]+"')
  mapfile -t names  < <(list_values "$body" '"resource_name":"[^"]+"')
  n=${#rtypes[@]}
  if [[ $n -ne ${#ops[@]} || $n -ne ${#princs[@]} || $n -ne ${#perms[@]} || $n -ne ${#pats[@]} || $n -ne ${#names[@]} ]]; then
    fail "listagem de ACLs com contagens divergentes — extração fail-closed (§3.4)"
  fi
  for (( i = 0; i < n; i++ )); do
    if [[ "${rtypes[$i]}" == "\"resource_type\":\"$rtype\"" && \
          "${ops[$i]}" == "\"operation\":\"$op\"" && \
          "${princs[$i]}" == "\"principal\":\"$princ\"" && \
          "${perms[$i]}" == '"permission":"ALLOW"' && \
          "${pats[$i]}" == '"pattern_type":"PREFIXED"' && \
          "${names[$i]}" == '"resource_name":"desafio-"' ]]; then
      return 0
    fi
  done
  return 1
}

ACL_SPECS=(
  "TOPIC|CREATE|$SA_ID_PRODUCER"
  "TOPIC|WRITE|$SA_ID_PRODUCER"
  "TOPIC|DESCRIBE|$SA_ID_PRODUCER"
  "TOPIC|READ|$SA_ID_CONSUMER"
  "GROUP|READ|$SA_ID_CONSUMER"
)

http GET "$acl_url" "$kafka_u" "$kafka_p"
acl_body="$HTTP_BODY"
for spec_line in "${ACL_SPECS[@]}"; do
  rtype="${spec_line%%|*}"
  rest="${spec_line#*|}"
  op="${rest%%|*}"
  sa_id="${rest#*|}"
  principal="User:$sa_id"
  if acl_present "$acl_body" "$rtype" "$op" "$principal"; then
    ok "JÁ EXISTE: ACL $op $rtype PREFIXED desafio- ($principal)"
    continue
  fi
  acl_payload="{\"resource_type\":\"$rtype\",\"resource_name\":\"desafio-\",\"pattern_type\":\"PREFIXED\",\"principal\":\"$principal\",\"host\":\"*\",\"operation\":\"$op\",\"permission\":\"ALLOW\"}"
  http POST "$acl_url" "$kafka_u" "$kafka_p" "$acl_payload"
  ok "CRIADO: ACL $op $rtype PREFIXED desafio- ($principal) — HTTP $HTTP_CODE"
done

# re-check de propagação (máx 6×10 s — §3.2)
attempt=0
while :; do
  http GET "$acl_url" "$kafka_u" "$kafka_p"
  acl_body="$HTTP_BODY"
  missing=""
  for spec_line in "${ACL_SPECS[@]}"; do
    rtype="${spec_line%%|*}"
    rest="${spec_line#*|}"
    op="${rest%%|*}"
    sa_id="${rest#*|}"
    if ! acl_present "$acl_body" "$rtype" "$op" "User:$sa_id"; then
      missing="$missing $op/$rtype"
    fi
  done
  if [[ -z "$missing" ]]; then break; fi
  attempt=$((attempt + 1))
  if (( attempt > 6 )); then
    fail "ACLs ausentes após re-check 6×10 s:$missing"
  fi
  printf '  re-check ACL (propagação) %d/6 — ausentes:%s\n' "$attempt" "$missing"
  sleep 10
done
ok "conjunto exato das 5 ACLs verificado na listagem (PREFIXED desafio-, sem extras neste escopo)"

# ==============================================================================
# ETAPA 8 — Acessos do SR (RBAC via /iam/v2/role-bindings, 2 bindings)
# ==============================================================================
etapa 8 "Role bindings do Schema Registry (2) — subject=desafio-*"
crn_subject="crn://confluent.cloud/organization=$org_id/environment=$env_id/schema-registry=$sr_id/subject=desafio-*"

rb_present() { # <corpo> <principal> <role>
  local body="$1" princ="$2" role="$3" i n
  local -a prins roles
  mapfile -t prins < <(list_values "$body" '"principal":"[^"]+"')
  mapfile -t roles < <(list_values "$body" '"role_name":"[A-Za-z]+"')
  n=${#prins[@]}
  if [[ $n -ne ${#roles[@]} ]]; then
    fail "listagem de role bindings com contagens divergentes — extração fail-closed (§3.4)"
  fi
  for (( i = 0; i < n; i++ )); do
    if [[ "${prins[$i]}" == "\"principal\":\"$princ\"" && "${roles[$i]}" == "\"role_name\":\"$role\"" ]]; then
      return 0
    fi
  done
  return 1
}

RB_SPECS=(
  "$SA_ID_PRODUCER|DeveloperWrite"
  "$SA_ID_CONSUMER|DeveloperRead"
)

http GET "$API_BASE/iam/v2/role-bindings?page_size=100" "$cloud_u" "$cloud_p"
rb_body="$HTTP_BODY"
for spec_line in "${RB_SPECS[@]}"; do
  sa_id="${spec_line%%|*}"
  role="${spec_line#*|}"
  principal="User:$sa_id"
  if rb_present "$rb_body" "$principal" "$role"; then
    ok "JÁ EXISTE: binding $role ($principal) em subject=desafio-*"
    continue
  fi
  # API iam/v2 real: principal + role_name + crn_pattern (drift registrado p/ REVIEW)
  rb_payload="{\"principal\":\"$principal\",\"role_name\":\"$role\",\"crn_pattern\":\"$crn_subject\"}"
  http POST "$API_BASE/iam/v2/role-bindings" "$cloud_u" "$cloud_p" "$rb_payload"
  ok "CRIADO: binding $role ($principal) — HTTP $HTTP_CODE"
done

attempt=0
while :; do
  http GET "$API_BASE/iam/v2/role-bindings?page_size=100" "$cloud_u" "$cloud_p"
  rb_body="$HTTP_BODY"
  missing=""
  for spec_line in "${RB_SPECS[@]}"; do
    sa_id="${spec_line%%|*}"
    role="${spec_line#*|}"
    if ! rb_present "$rb_body" "User:$sa_id" "$role"; then
      missing="$missing $role"
    fi
  done
  if [[ -z "$missing" ]]; then break; fi
  attempt=$((attempt + 1))
  if (( attempt > 6 )); then
    fail "role bindings ausentes após re-check 6×10 s:$missing"
  fi
  printf '  re-check bindings (propagação) %d/6 — ausentes:%s\n' "$attempt" "$missing"
  sleep 10
done
ok "2 role bindings vigentes no escopo subject=desafio-* (DeveloperWrite producer / DeveloperRead consumer)"

# ==============================================================================
# ETAPA 9 — Tópico smoke desafio-smoke.v1
# ==============================================================================
etapa 9 "Tópico smoke $TOPIC_SMOKE (1 partição, cleanup.policy=delete, retention.ms=86400000)"
topic_url="${CONFLUENT_KAFKA_REST_URL:?Defina CONFLUENT_KAFKA_REST_URL (gravado pela etapa 3)}/kafka/v3/clusters/${CONFLUENT_KAFKA_CLUSTER_ID:?Defina CONFLUENT_KAFKA_CLUSTER_ID (gravado pela etapa 3)}/topics/$TOPIC_SMOKE"
kafka_u="${KAFKA_ADMIN_KEY:?Defina KAFKA_ADMIN_KEY no .env (M2, Console)}"
kafka_p="${KAFKA_ADMIN_SECRET:?Defina KAFKA_ADMIN_SECRET no .env (M2, Console)}"

if http_probe GET "$topic_url" "$kafka_u" "$kafka_p"; then
  ok "JÁ EXISTE: tópico $TOPIC_SMOKE"
elif [[ "$HTTP_CODE" == "404" ]]; then
  topic_payload="{\"topic_name\":\"$TOPIC_SMOKE\",\"partitions_count\":1,\"configs\":[{\"name\":\"cleanup.policy\",\"value\":\"delete\"},{\"name\":\"retention.ms\",\"value\":\"86400000\"}]}"
  http POST "${CONFLUENT_KAFKA_REST_URL}/kafka/v3/clusters/${CONFLUENT_KAFKA_CLUSTER_ID}/topics" \
    "$kafka_u" "$kafka_p" "$topic_payload"
  ok "CRIADO: tópico $TOPIC_SMOKE (HTTP $HTTP_CODE)"
else
  fail "GET tópico $TOPIC_SMOKE retornou HTTP $HTTP_CODE"
fi

http GET "$topic_url" "$kafka_u" "$kafka_p"
if ! printf '%s' "$HTTP_BODY" | grep -qF '"partitions_count":1'; then
  fail "tópico $TOPIC_SMOKE sem partitions_count:1 na resposta REST"
fi
ok "tópico verificado: partitions_count 1"

# ==============================================================================
# ETAPA 10 — Compute pool Flink desafio-flink (max_cfu 1)
# ==============================================================================
etapa 10 "Compute pool $POOL_NAME (Flink · $PROVIDER · $REGION · max_cfu 1)"
pool_list_url="$API_BASE/fcpm/v2/compute-pools?environment=${CONFLUENT_ENV_ID:?Defina CONFLUENT_ENV_ID (gravado pela etapa 2)}"
http GET "$pool_list_url" "$cloud_u" "$cloud_p"
lookup_id_by_name "$HTTP_BODY" 'lfcp-' "$POOL_NAME" || true
pool_id="$LOOKUP_ID"
if [[ -n "$pool_id" ]]; then
  ok "JÁ EXISTE: $pool_id (descoberto por display_name)"
else
  pool_payload="{\"spec\":{\"display_name\":\"$POOL_NAME\",\"cloud\":\"$PROVIDER\",\"region\":\"$REGION\",\"environment\":{\"id\":\"$CONFLUENT_ENV_ID\"},\"max_cfu\":1}}"
  http POST "$API_BASE/fcpm/v2/compute-pools" "$cloud_u" "$cloud_p" "$pool_payload"
  pool_id=$(grammar "$(id_field_value "$HTTP_BODY" 'lfcp-' | head -n1)" '^lfcp-[a-z0-9]+$' "compute pool id")
  ok "CRIADO: $pool_id (HTTP $HTTP_CODE — criação assíncrona)"
fi
persist FLINK_POOL_ID "$pool_id"

attempt=0
while :; do
  http GET "$pool_list_url" "$cloud_u" "$cloud_p"
  if lookup_id_by_name "$HTTP_BODY" 'lfcp-' "$POOL_NAME"; then break; fi
  attempt=$((attempt + 1))
  if (( attempt >= 40 )); then
    fail "timeout 40×15 s: pool $POOL_NAME não aparece em GET /fcpm/v2/compute-pools?environment=$CONFLUENT_ENV_ID"
  fi
  printf '  aguardando pool visível... (%d/40)\n' "$attempt"
  sleep 15
done
if ! printf '%s' "$HTTP_BODY" | grep -qF '"max_cfu":1'; then
  fail "pool $POOL_NAME sem max_cfu:1 na listagem (AC-11)"
fi
ok "pool visível na listagem com max_cfu 1 (estado a registrar na evidência 01)"

# ==============================================================================
# RESUMO
# ==============================================================================
printf '\n=== SETUP CONCLUÍDO ===\n'
ok "environment  : $CONFLUENT_ENV_ID"
ok "cluster      : $CONFLUENT_KAFKA_CLUSTER_ID (REST 443 capturado no .env)"
ok "schema reg.  : $CONFLUENT_SR_CLUSTER_ID"
ok "service accts: $SA_ID_PRODUCER · $SA_ID_CONSUMER"
ok "ACLs         : 5 (3 producer / 2 consumer, PREFIXED desafio-)"
ok "role bindings: 2 (subject=desafio-*)"
ok "tópico smoke : $TOPIC_SMOKE"
ok "pool Flink   : $FLINK_POOL_ID"
printf '\nPróximos passos do operador (Git Bash):\n'
printf '  1) checagens AC-01…AC-14 com os comandos da §4.1 (somente leitura; ids, nunca secrets)\n'
printf '  2) smoke de escrita AC-09 no formato v3 {\"value\": {\"type\": \"STRING\", \"data\": \"msg-01\"}}\n'
printf '  3) ao encerrar: bash teardown.sh\n'

