#!/usr/bin/env bash
# Tests the Angry Duck peer mirror end to end on stg with a fresh, never-
# pulled tag:
#
#   1. Confirms the image is on zero nodes (refuses to run otherwise —
#      a false "hit" from leftover content is worse than no result).
#   2. Fires the preheat webhook and waits for exactly the seeded nodes to
#      finish pulling from origin.
#   3. Pulls the same image on a control-plane node — guaranteed excluded
#      from preheat (RANK_EXCLUDE_NODE_SUBSTRINGS), so guaranteed cold.
#   4. Reads containerd's own "bytes read" for that pull. A few KB means
#      every layer was already local (the mirror worked); hundreds of MB
#      means it went to origin.
#   5. Cross-references the mirror's own logs on both the cold node and
#      whichever node it says served the transfer.
#
# Run this from the bastion host with kubectl and root SSH to every node
# (matches the access described in this conversation), AS YOUR OWN USER —
# do NOT run the whole script with sudo. It already runs kubectl as
# "sudo kubectl" internally, where the sudo prompt is expected; running
# the whole script under sudo instead makes ssh use root's own identity
# and known_hosts, not yours, which is what's actually authorized on the
# nodes. Needs: kubectl, ssh, curl, jq, base64.
#
# Usage: ./test-p2p.sh [image-tag]
#   image-tag defaults to $DEFAULT_TAG below. Pass a tag you are sure has
#   never been pulled on this cluster — re-running with the same tag will
#   find it already local everywhere and every result below is bogus.
#   Match this registry's actual tagging convention (v9052, v9028, ... in
#   this conversation) — the controller does not check the registry, so a
#   typo'd tag is accepted and then just fails to pull from origin.

set -uo pipefail

# --- configuration -----------------------------------------------------
NAMESPACE="angryduck"
REPO="registry.internal-registry.example.com/snappfood/express/snappfood-express-server/stage"
DEFAULT_TAG="v9018"
WEBHOOK_URL="https://api-stg.sahand-k8s.internal-dev.example.com/angryduck/webhook/preheat"
REGISTRY_HOST="registry.internal-registry.example.com"
CREDS_SECRET="gitlab-registry-pull"
COLD_NODE_PATTERN="master1"   # which control-plane node to use for the cold pull
PREHEAT_TIMEOUT_S=180          # how long to wait for the seed pulls to land
KCTL="sudo kubectl"
# accept-new: trust a node's host key on first contact without prompting
# (BatchMode still blocks password/passphrase prompts; it does not affect
# host-key handling — this combination is what makes both safe together).
SSH="ssh -o BatchMode=yes -o ConnectTimeout=5 -o StrictHostKeyChecking=accept-new"

TAG="${1:-$DEFAULT_TAG}"
IMG="$REPO:$TAG"

# --- output helpers ------------------------------------------------------
c_red=$'\e[31m'; c_grn=$'\e[32m'; c_yel=$'\e[33m'; c_dim=$'\e[2m'; c_rst=$'\e[0m'
log()  { printf '%s[%s]%s %s\n' "$c_dim" "$(date +%H:%M:%S)" "$c_rst" "$*"; }
ok()   { printf '%s✓%s %s\n' "$c_grn" "$c_rst" "$*"; }
warn() { printf '%s⚠%s %s\n' "$c_yel" "$c_rst" "$*"; }
fail() { printf '%s✗%s %s\n' "$c_red" "$c_rst" "$*"; exit 1; }

for bin in kubectl ssh curl jq base64; do
  command -v "$bin" >/dev/null || fail "missing required tool: $bin"
done

log "target image: $IMG"
if ! [[ "$TAG" =~ ^v[0-9]+$ ]]; then
  warn "tag '$TAG' doesn't match this registry's usual v<number> convention (v9052, v9028, ...) — if that's a typo, the seed pulls below will fail against origin with a nonexistent tag, not a mirror problem"
fi

# --- node inventory ------------------------------------------------------
# name<TAB>internal-ip, one per line
NODES=$($KCTL get nodes -o json \
  | jq -r '.items[] | [.metadata.name, (.status.addresses[] | select(.type=="InternalIP") | .address)] | @tsv')
[ -n "$NODES" ] || fail "could not list nodes (check kubectl access)"

node_ip() { echo "$NODES" | awk -F'\t' -v n="$1" '$1==n{print $2}'; }
ip_node() { echo "$NODES" | awk -F'\t' -v i="$1" '$2==i{print $1}'; }

COLD_NODE=$(echo "$NODES" | awk -F'\t' -v p="$COLD_NODE_PATTERN" '$1 ~ p {print $1; exit}')
[ -n "$COLD_NODE" ] || fail "no node name matches pattern '$COLD_NODE_PATTERN' — set COLD_NODE_PATTERN at the top of this script"
COLD_IP=$(node_ip "$COLD_NODE")
log "cold-pull target: $COLD_NODE ($COLD_IP)"

# --- step -1: confirm SSH actually works before trusting anything below -
# A failed ssh (bad host key, wrong identity, network) must never be read
# as "the image isn't here" or "the pull succeeded" — both later checks
# distinguish ssh failure from a real negative result, but catching it
# here up front gives one clear error instead of a wall of them.
log "verifying SSH access to every node..."
UNREACHABLE=()
while IFS=$'\t' read -r name ip; do
  [ -z "$ip" ] && continue
  $SSH "root@$ip" true 2>/dev/null || UNREACHABLE+=("$name ($ip)")
done <<< "$NODES"
if [ "${#UNREACHABLE[@]}" -gt 0 ]; then
  printf '%s\n' "${UNREACHABLE[@]}" | sed 's/^/    /'
  fail "cannot SSH into ${#UNREACHABLE[@]} node(s) above as root — fix access before running this script (and make sure you're NOT running this script itself with sudo; see the header comment)"
fi
ok "SSH access confirmed to all nodes"

# --- step 0: refuse to run against a tag that's already somewhere -------
log "step 0/4: confirming '$TAG' is on zero nodes..."
DIRTY=0
while IFS=$'\t' read -r name ip; do
  [ -z "$ip" ] && continue
  if $SSH "root@$ip" "crictl images 2>/dev/null | grep -qE 'snappfood-express-server/stage[[:space:]]+$TAG([[:space:]]|\$)'"; then
    warn "$name ($ip) already has $TAG"
    DIRTY=1
  fi
done <<< "$NODES"
[ "$DIRTY" -eq 0 ] || fail "tag '$TAG' is already present on at least one node — pick an unused tag or re-run with a different one"
ok "confirmed: '$TAG' is not on any node"

# --- also sanity-check the cold node's _default isn't shadowed ----------
DEFAULT_TOML=$($SSH "root@$COLD_IP" "cat /etc/containerd/certs.d/_default/hosts.toml 2>/dev/null")
if [ -z "$DEFAULT_TOML" ]; then
  warn "$COLD_NODE has no certs.d/_default/hosts.toml — containerd's config_path is probably not set; the mirror can't work here"
elif ! grep -q "managed-by: angryduck-worker" <<< "$DEFAULT_TOML"; then
  warn "$COLD_NODE's _default/hosts.toml was NOT written by the current worker (see contents below) — the mirror is inactive on this node until it's cleared"
  echo "$DEFAULT_TOML" | sed 's/^/    /'
else
  ok "$COLD_NODE's _default/hosts.toml is ours and active"
fi

# --- step 1: fire the webhook -------------------------------------------
log "step 1/4: firing preheat webhook..."
RESP=$(curl -sS -X POST "$WEBHOOK_URL" -H 'Content-Type: application/json' \
  -d "{\"image\":\"$IMG\"}")
echo "$RESP" | jq . 2>/dev/null || echo "$RESP"
ACCEPTED=$(echo "$RESP" | jq -r '.accepted // false' 2>/dev/null)
[ "$ACCEPTED" = "true" ] || fail "webhook did not accept the request (see response above)"

mapfile -t SEEDS < <(echo "$RESP" | jq -r '.ordered_nodes[]')
[ "${#SEEDS[@]}" -gt 0 ] || fail "webhook accepted but ordered zero nodes — check controller logs"
ok "controller ordered ${#SEEDS[@]} seed node(s): ${SEEDS[*]}"

for s in "${SEEDS[@]}"; do
  if [ "$s" = "$COLD_NODE" ]; then
    fail "$COLD_NODE was itself chosen as a seed — the cold-pull step below would be meaningless; pick a different COLD_NODE_PATTERN or a fresh tag"
  fi
done

# --- step 2: wait for the seeds to finish pulling from origin -----------
log "step 2/4: waiting up to ${PREHEAT_TIMEOUT_S}s for ${#SEEDS[@]} seed pull(s) to land..."
START_EPOCH=$(date -u +%Y-%m-%dT%H:%M:%SZ)
deadline=$((SECONDS + PREHEAT_TIMEOUT_S))
# A plain counter, not ${#assoc_array[@]} — that expansion on an empty
# associative array trips "set -u" on some bash builds and aborts the
# while condition itself, silently skipping the loop body entirely (seen
# in practice: it printed the error and moved straight on, reporting "0
# preheat pulls" without ever having checked).
declare -A landed
landed_count=0
while [ "$SECONDS" -lt "$deadline" ] && [ "$landed_count" -lt "${#SEEDS[@]}" ]; do
  LOGS=$($KCTL -n "$NAMESPACE" logs -l app=angryduck-worker --prefix --tail=-1 --max-log-requests=30 --since-time="$START_EPOCH" 2>/dev/null \
    | grep -F "$TAG" | grep "pull succeeded")
  for s in "${SEEDS[@]}"; do
    if [ -n "${landed[$s]:-}" ]; then continue; fi
    if grep -q "/$s/" <<< "$LOGS" || $KCTL -n "$NAMESPACE" logs "$($KCTL -n "$NAMESPACE" get pod -l app=angryduck-worker --field-selector spec.nodeName="$s" -o name)" --tail=-1 --since-time="$START_EPOCH" 2>/dev/null | grep -q "pull succeeded.*$TAG"; then
      landed[$s]=1
      landed_count=$((landed_count + 1))
      ok "seed landed: $s"
    fi
  done
  [ "$landed_count" -lt "${#SEEDS[@]}" ] && sleep 5
done
if [ "$landed_count" -lt "${#SEEDS[@]}" ]; then
  warn "only $landed_count/${#SEEDS[@]} seeds confirmed landed within ${PREHEAT_TIMEOUT_S}s — continuing anyway, but a low count as a source may explain a miss below"
  FAILS=$($KCTL -n "$NAMESPACE" logs -l app=angryduck-worker --prefix --tail=-1 --max-log-requests=30 --since-time="$START_EPOCH" 2>/dev/null \
    | grep -F "$TAG" | grep "pull failed")
  if [ -n "$FAILS" ]; then
    warn "seed pulls that actively failed (check whether '$TAG' really exists in the registry):"
    echo "$FAILS" | sed 's/^/    /'
  fi
else
  ok "all ${#SEEDS[@]} seed(s) landed"
fi

# a fan-out check: make sure ONLY the seeds pulled, nobody else
FANOUT=$($KCTL -n "$NAMESPACE" logs -l app=angryduck-worker --prefix --tail=-1 --max-log-requests=30 --since-time="$START_EPOCH" 2>/dev/null \
  | grep -F "$TAG" | grep -c "pull succeeded")
if [ "$FANOUT" -gt "${#SEEDS[@]}" ]; then
  warn "$FANOUT nodes ran a preheat pull for $TAG, expected exactly ${#SEEDS[@]} — the ranker seeded more than RANK_TOP_N; check RANK_TOP_N and the ranker's dedup logic"
else
  ok "no fan-out: exactly $FANOUT preheat pull(s), matching the ${#SEEDS[@]} seed(s)"
fi

# --- step 3: the actual test — cold pull on an excluded node ------------
log "step 3/4: fetching registry credentials..."
CREDS=$($KCTL -n "$NAMESPACE" get secret "$CREDS_SECRET" -o jsonpath='{.data.\.dockerconfigjson}' \
  | base64 -d | jq -r --arg h "$REGISTRY_HOST" '.auths[$h].auth' | base64 -d)
[ -n "$CREDS" ] || fail "could not extract credentials for $REGISTRY_HOST from secret $CREDS_SECRET"

log "step 3/4: pulling $IMG on $COLD_NODE (cold, never seeded)..."
PULL_START=$(date -u +%Y-%m-%dT%H:%M:%SZ)
T0=$(date +%s.%N)
PULL_OUT=$($SSH "root@$COLD_IP" "crictl pull --creds '$CREDS' $IMG" 2>&1)
PULL_RC=$?
T1=$(date +%s.%N)
PULL_SECS=$(awk -v a="$T0" -v b="$T1" 'BEGIN{printf "%.1f", b-a}')
echo "$PULL_OUT" | sed 's/^/    /'
[ "$PULL_RC" -eq 0 ] || fail "crictl pull failed (rc=$PULL_RC) — see output above"
ok "pull finished in ${PULL_SECS}s"

# --- step 4: evidence -----------------------------------------------------
log "step 4/4: gathering evidence..."

JLINE=$($SSH "root@$COLD_IP" "journalctl -u containerd --since '-3min' 2>/dev/null | grep -F '$TAG' | grep 'stop pulling image'" | tail -1)
echo "    $JLINE"
BYTES=$(echo "$JLINE" | grep -oE 'bytes read=[0-9]+' | grep -oE '[0-9]+')
DIGEST=$($SSH "root@$COLD_IP" "journalctl -u containerd --since '-3min' 2>/dev/null | grep -F '$TAG' | grep 'Pulled image'" \
  | tail -1 | grep -oE 'repo digest \\"[^\\"]+@sha256:[a-f0-9]+' | grep -oE 'sha256:[a-f0-9]+')

W=$($KCTL -n "$NAMESPACE" get pod -l app=angryduck-worker --field-selector spec.nodeName="$COLD_NODE" -o name)
MIRROR_LOG=$($KCTL -n "$NAMESPACE" logs "$W" --tail=-1 --since-time="$PULL_START" 2>/dev/null | grep -i mirror)
echo "$MIRROR_LOG" | sed 's/^/    /'

RESULT=$(echo "$MIRROR_LOG" | grep -oE 'result=[a-z_]+' | tail -1)
SOURCE_IP=$(echo "$MIRROR_LOG" | grep -oE 'imported .* from [0-9.]+:[0-9]+' | grep -oE '[0-9.]+:[0-9]+' | tail -1 | cut -d: -f1)

if [ -n "$SOURCE_IP" ]; then
  SOURCE_NODE=$(ip_node "$SOURCE_IP")
  ok "peer transfer sourced from $SOURCE_NODE ($SOURCE_IP)"
  SW=$($KCTL -n "$NAMESPACE" get pod -l app=angryduck-worker --field-selector spec.nodeName="$SOURCE_NODE" -o name 2>/dev/null)
  if [ -n "$SW" ]; then
    log "$SOURCE_NODE's export side:"
    $KCTL -n "$NAMESPACE" logs "$SW" --tail=-1 --since-time="$PULL_START" 2>/dev/null | grep -i "export" | sed 's/^/    /'
  fi
fi

# --- verdict ---------------------------------------------------------------
echo
echo "================================ VERDICT ================================"
echo "image:            $IMG"
[ -n "$DIGEST" ] && echo "digest:           $DIGEST"
echo "cold node:        $COLD_NODE ($COLD_IP)"
echo "pull wall time:   ${PULL_SECS}s"
echo "containerd bytes read from origin: ${BYTES:-unknown}"
echo "mirror result:    ${RESULT:-(no mirror log line found)}"
[ -n "$SOURCE_IP" ] && echo "served by:        ${SOURCE_NODE:-$SOURCE_IP}"
echo "---------------------------------------------------------------------"
if [ -n "${BYTES:-}" ] && [ "$BYTES" -lt 1000000 ] && [ "$RESULT" = "result=hit" ]; then
  echo "${c_grn}PASS${c_rst}: containerd read only ${BYTES} bytes from origin (manifest-sized)"
  echo "      and the mirror logged a hit. The peer transfer supplied every layer."
elif [ -n "${BYTES:-}" ] && [ "$BYTES" -lt 1000000 ]; then
  echo "${c_yel}INCONCLUSIVE${c_rst}: bytes read is small (peer path likely worked) but no"
  echo "      matching 'result=hit' mirror log line was found — check the mirror log"
  echo "      above by hand."
else
  echo "${c_red}FAIL${c_rst}: containerd read ${BYTES:-a large number of} bytes from origin —"
  echo "      this was an ordinary origin pull, not a peer transfer. Check:"
  echo "        - mirror result above (miss/failed/timeout?)"
  echo "        - whether $COLD_NODE's _default/hosts.toml is ours (checked above)"
  echo "        - whether any seed actually landed before this pull ran"
fi
echo "==========================================================================="