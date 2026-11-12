#!/usr/bin/env bash
# duckbench: pull-time benchmarks and chaos tests for Angry Duck on a test
# cluster. See bench/README.md. Never point it at production: it switches
# Angry Duck's features on and off, restarts its pods, and fills a node's disk.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
[ -f "$HERE/bench.env" ] && . "$HERE/bench.env"

: "${KUBECTL:=sudo kubectl}"
: "${BENCH_HOST:=}"                 # registry host the nodes pull from, behind your ingress
: "${BENCH_NS:=angryduck-bench}"
: "${AD_NS:=angryduck}"
: "${INGRESS_CLASS:=nginx}"
: "${TLS_SECRET:=}"                 # TLS secret in BENCH_NS; empty: no tls section (TLS ends in front of the ingress, or its default certificate serves)
: "${BENCH_INGRESS_PATH:=/}"        # /v2/bench/ shares BENCH_HOST with other ingresses: only bench repos are routed here
: "${REGISTRY_IMAGE:=registry:2}"
: "${TOOLBOX_IMAGE:=gcr.io/go-containerregistry/crane:debug}"
: "${BASE_IMAGE:=docker.io/library/busybox:1.36}"
: "${REGISTRY_SIZE_LIMIT:=40Gi}"
: "${PREHEAT_LEAD:=60}"            # seconds between the webhook and the rollout (rest of the pipeline)
: "${READY_TIMEOUT:=900}"
: "${SPREAD_TIMEOUT:=900}"
: "${RESCUE_TIMEOUT:=1200}"         # kubelet's pull backoff alone can reach 5 minutes
: "${KILL_AFTER:=5}"                # seconds into a spread before chaos kills something
: "${FILL_TARGET:=0.75}"            # disk-pressure: fill the node's root filesystem up to this
: "${FILL_HOLD:=300}"               # disk-pressure: how long to keep it full
: "${SHAPE:=ds}"                     # ds: every node at once (worst case for peers); rolling: a Deployment updated in waves
: "${ROLL_UNAVAILABLE:=25%}"        # rolling: maxUnavailable, i.e. the wave size (Kubernetes' default)
: "${NODE:=}"                       # node for single-node scenarios; empty: picked automatically
: "${PULL_SECRET_FROM:=}"           # <namespace>/<secret> with credentials for the bench's own images (Nexus); copied into BENCH_NS
: "${SSH_USER:=root}"
: "${SSH_OPTS:=-o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=accept-new}"
: "${FILL_PATH:=/var/lib/angryduck-bench-fill}"

STATE="$HERE/.state"
RESULTS="$HERE/results"
REG_SVC="angryduck-bench-registry"
PUSH_HOST="$REG_SVC.$BENCH_NS.svc:5000"
mkdir -p "$STATE" "$RESULTS"

K() { $KUBECTL "$@"; }
log() { printf '%s  %s\n' "$(date +%H:%M:%S)" "$*" >&2; }
die() { log "ERROR: $*"; exit 1; }
need() { command -v "$1" >/dev/null || die "$1 is required"; }
now() { date -u +%s; }
rfc3339() { date -u +%Y-%m-%dT%H:%M:%SZ; }
runid() { date -u +r%m%d-%H%M%S; }

# ---------------------------------------------------------------- images

# size -> "<layers> <MB per layer>"; every layer is fresh random data, so
# every run is a cold pull no node has seen.
size_spec() {
  case "$1" in
    tiny)   echo "1 1" ;;
    small)  echo "1 50" ;;
    medium) echo "5 100" ;;
    large)  echo "8 256" ;;
    layers) echo "30 10" ;;
    *) die "unknown size '$1' (tiny, small, medium, large, layers)" ;;
  esac
}

# push_image <size> <run> -> prints the image reference the nodes pull
push_image() {
  local size=$1 run=$2 n mb
  read -r n mb < <(size_spec "$size")
  local push="$PUSH_HOST/bench/$size:$run"
  log "building $size image: $n layer(s) x ${mb}MB random, tag $run"
  K -n "$BENCH_NS" exec toolbox -- sh -ec "
    rm -rf /work/* && mkdir -p /work/d
    base='$BASE_IMAGE'; i=1
    while [ \$i -le $n ]; do
      dd if=/dev/urandom of=/work/d/blob-\$i bs=1048576 count=$mb 2>/dev/null
      tar -C /work/d -cf /work/layer.tar blob-\$i
      crane append --insecure --platform linux/amd64 -b \"\$base\" -f /work/layer.tar -t '$push' >/dev/null 2>>/work/crane.log || { cat /work/crane.log >&2; exit 1; }
      rm -f /work/d/blob-\$i /work/layer.tar
      base='$push'; i=\$((i+1))
    done" || die "pushing the $size image failed"
  echo "$BENCH_HOST/bench/$size:$run"
}

# The image a rolling Deployment starts from: BASE_IMAGE, copied once into the
# bench registry, so every node has it before the measured update.
base_ref() {
  K -n "$BENCH_NS" exec toolbox -- crane copy --insecure --platform linux/amd64 "$BASE_IMAGE" "$PUSH_HOST/bench/base:v1" >/dev/null 2>&1 \
    || die "couldn't copy $BASE_IMAGE into the bench registry"
  echo "$BENCH_HOST/bench/base:v1"
}

image_bytes() { # <size> <run>
  K -n "$BENCH_NS" exec toolbox -- crane manifest --insecure "$PUSH_HOST/bench/$1:$2" \
    | jq '([.layers[].size] | add) + .config.size'
}

# ---------------------------------------------------------------- nodes

node_ip() { K get node "$1" -o jsonpath='{.status.addresses[?(@.type=="InternalIP")].address}'; }
# shellcheck disable=SC2086
nssh() { local ip; ip=$(node_ip "$1"); shift; ssh $SSH_OPTS "$SSH_USER@$ip" "$@"; }
untainted_nodes() {
  K get nodes -o json | jq -r '.items[] | select((.spec.taints // []) | length == 0) | select(.spec.unschedulable != true) | .metadata.name'
}

# ---------------------------------------------------------------- Angry Duck

ad_get() { K get --raw "/api/v1/namespaces/$AD_NS/services/angryduck-controller:http/proxy$1"; }
ad_status() { ad_get /status; }

webhook_token() {
  K -n "$AD_NS" get secret angryduck-webhook-token -o jsonpath='{.data.token}' 2>/dev/null | base64 -d 2>/dev/null || true
}

# preheat <image> -> prints the controller's answer
preheat() {
  local img=$1 tok auth=()
  tok=$(webhook_token)
  [ -n "$tok" ] && auth=(--header "Authorization: Bearer $tok")
  K -n "$BENCH_NS" exec toolbox -- wget -qO- "${auth[@]}" --header 'Content-Type: application/json' \
    --post-data "{\"image\":\"$img\"}" "http://angryduck-controller.$AD_NS.svc:8080/webhook/preheat"
}

# wait_settled <image> <timeout>: no spread or propagation of the image
# still working. A restarted controller forgets its jobs, so "nothing
# running" counts as settled too; the rollout afterwards tells the truth.
wait_settled() {
  local img=$1 deadline=$(( $(now) + $2 )) start st
  start=$(now)
  while [ "$(now)" -lt "$deadline" ]; do
    st=$(ad_status | jq -r --arg i "$img" '
      ([.spreads[]? | select(.image==$i and (.complete | not))] | length) +
      ([.propagations[]? | select(.image==$i and ((.missing|length) + (.in_flight|length) + ((.seeding // [])|length) > 0))] | length)')
    [ "${st:-1}" = 0 ] && [ $(( $(now) - start )) -ge 20 ] && return 0
    sleep 3
  done
  return 1
}

worker_pod() { # <node>
  K -n "$AD_NS" get pod -l app.kubernetes.io/component=worker --field-selector "spec.nodeName=$1" -o jsonpath='{.items[0].metadata.name}'
}

# sum_worker_metric <regex>: summed across every worker. Slow-ish (one call per node), used around chaos runs only.
sum_worker_metric() {
  local re=$1 p total=0 v
  for p in $(K -n "$AD_NS" get pod -l app.kubernetes.io/component=worker -o jsonpath='{.items[*].metadata.name}'); do
    v=$(K get --raw "/api/v1/namespaces/$AD_NS/pods/$p:18081/proxy/metrics" 2>/dev/null \
        | awk -v re="$re" '$0 !~ /^#/ && $1 ~ re {s+=$2} END{printf "%.0f", s+0}')
    total=$(( total + ${v:-0} ))
  done
  echo "$total"
}

node_metric() { # <node> <regex>
  local p; p=$(worker_pod "$1")
  K get --raw "/api/v1/namespaces/$AD_NS/pods/$p:18081/proxy/metrics" 2>/dev/null \
    | awk -v re="$2" '$0 !~ /^#/ && $1 ~ re {s+=$2} END{printf "%.0f", s+0}'
}

controller_metric() { # <regex>
  ad_get /metrics | awk -v re="$1" '$0 !~ /^#/ && $1 ~ re {s+=$2} END{printf "%.0f", s+0}'
}

# ---------------------------------------------------------------- modes

# Modes switch Angry Duck's features through its ConfigMap; setup saved the
# original and teardown puts it back. Every mode adds BENCH_HOST to
# MIRROR_REGISTRIES so the mirror covers the bench registry on cold nodes.
mode_data() {
  local mode=$1 base mr
  base=$(jq -c '.data' "$STATE/cm-backup.json")
  mr=$(jq -r '.MIRROR_REGISTRIES // ""' <<<"$base")
  case ",$mr," in *",$BENCH_HOST,"*) ;; *) mr="${mr:+$mr,}$BENCH_HOST" ;; esac
  base=$(jq -c --arg mr "$mr" '. + {MIRROR_REGISTRIES: $mr}' <<<"$base")
  case "$mode" in
    on)          echo "$base" ;;
    kubelet)     jq -c '. + {MIRROR_ENABLED:"false", SPREAD_ENABLED:"false", PROPAGATE_ENABLED:"false", RESCUE_ENABLED:"false"}' <<<"$base" ;;
    mirror-only) jq -c '. + {SPREAD_ENABLED:"false", PROPAGATE_ENABLED:"false"}' <<<"$base" ;;
    rescue-only) jq -c '. + {MIRROR_ENABLED:"false", SPREAD_ENABLED:"false", PROPAGATE_ENABLED:"false"}' <<<"$base" ;;
    *) die "unknown mode '$mode' (on, kubelet, mirror-only, rescue-only)" ;;
  esac
}

apply_cm_data() { # <json data>
  K -n "$AD_NS" get cm angryduck-config -o json \
    | jq --argjson d "$1" '.data = $d | del(.metadata.resourceVersion, .metadata.managedFields)' \
    | K replace -f - >/dev/null
}

restart_ad() {
  log "restarting Angry Duck"
  K -n "$AD_NS" rollout restart deploy/angryduck-controller ds/angryduck-worker >/dev/null
  K -n "$AD_NS" rollout status deploy/angryduck-controller --timeout=10m >/dev/null
  K -n "$AD_NS" rollout status ds/angryduck-worker --timeout=20m >/dev/null
  local want deadline=$(( $(now) + 180 )) fresh
  want=$(K -n "$AD_NS" get ds angryduck-worker -o jsonpath='{.status.desiredNumberScheduled}')
  while [ "$(now)" -lt "$deadline" ]; do
    fresh=$(ad_status | jq '.fresh_count' 2>/dev/null || echo 0)
    [ "${fresh:-0}" -ge "$want" ] && { sleep 5; return 0; }
    sleep 3
  done
  die "only ${fresh:-0}/$want workers reported after the restart"
}

set_mode() {
  local mode=$1
  [ -f "$STATE/cm-backup.json" ] || die "run setup first"
  if [ "$(cat "$STATE/mode" 2>/dev/null)" = "$mode" ]; then return 0; fi
  log "mode -> $mode"
  apply_cm_data "$(mode_data "$mode")"
  restart_ad
  echo "$mode" >"$STATE/mode"
}

# ---------------------------------------------------------------- bench registry

registry_down() {
  log "registry OUTAGE: the ingress has no endpoints now"
  K -n "$BENCH_NS" patch svc "$REG_SVC-public" --type json \
    -p '[{"op":"replace","path":"/spec/selector","value":{"app":"angryduck-bench-registry","outage":"true"}}]' >/dev/null
}
registry_up() {
  K -n "$BENCH_NS" patch svc "$REG_SVC-public" --type json \
    -p '[{"op":"replace","path":"/spec/selector","value":{"app":"angryduck-bench-registry"}}]' >/dev/null 2>&1 || true
}

# registry_blob_bytes <since rfc3339>: blob bytes the registry served, from
# its access log (seed pulls, spread pulls and kubelet pulls alike).
registry_blob_bytes() {
  K -n "$BENCH_NS" logs "deploy/$REG_SVC" --since-time="$1" 2>/dev/null | awk -F'"' '
    $2 ~ /^GET \/v2\/.*\/blobs\// { split($3, a, " "); if (a[1] == 200 || a[1] == 206) b += a[2] }
    END { printf "%.0f\n", b }'
}

# ---------------------------------------------------------------- workloads

deploy_ds() { # <name> <image> <pullPolicy>
  K apply -f - >/dev/null <<EOF
apiVersion: apps/v1
kind: DaemonSet
metadata: {name: $1, namespace: $BENCH_NS, labels: {bench: "$1"}}
spec:
  selector: {matchLabels: {bench: "$1"}}
  template:
    metadata: {labels: {bench: "$1"}}
    spec:
      terminationGracePeriodSeconds: 0
      containers:
        - name: c
          image: $2
          imagePullPolicy: $3
          command: ["sleep", "86400"]
          resources: {requests: {cpu: 1m, memory: 4Mi}, limits: {memory: 16Mi}}
EOF
}

# deploy_rolling <name> <image> <replicas>: one pod per node (required
# anti-affinity), updated maxUnavailable at a time, no surge.
deploy_rolling() {
  K apply -f - >/dev/null <<EOF
apiVersion: apps/v1
kind: Deployment
metadata: {name: $1, namespace: $BENCH_NS, labels: {bench: "$1"}}
spec:
  replicas: $3
  strategy: {type: RollingUpdate, rollingUpdate: {maxSurge: 0, maxUnavailable: "$ROLL_UNAVAILABLE"}}
  selector: {matchLabels: {bench: "$1"}}
  template:
    metadata: {labels: {bench: "$1"}}
    spec:
      terminationGracePeriodSeconds: 0
      affinity:
        podAntiAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            - {topologyKey: kubernetes.io/hostname, labelSelector: {matchLabels: {bench: "$1"}}}
      containers:
        - name: c
          image: $2
          imagePullPolicy: IfNotPresent
          command: ["sleep", "86400"]
          resources: {requests: {cpu: 1m, memory: 4Mi}, limits: {memory: 16Mi}}
EOF
}

deploy_pinned() { # <name> <image> <node>
  K apply -f - >/dev/null <<EOF
apiVersion: v1
kind: Pod
metadata: {name: $1, namespace: $BENCH_NS, labels: {bench: "$1"}}
spec:
  nodeName: $3
  terminationGracePeriodSeconds: 0
  containers:
    - name: c
      image: $2
      imagePullPolicy: IfNotPresent
      command: ["sleep", "86400"]
      resources: {requests: {cpu: 1m, memory: 4Mi}, limits: {memory: 16Mi}}
EOF
}

wait_ds_ready() { # <name> <timeout>
  local deadline=$(( $(now) + $2 )) want ready
  while [ "$(now)" -lt "$deadline" ]; do
    read -r want ready < <(K -n "$BENCH_NS" get ds "$1" -o jsonpath='{.status.desiredNumberScheduled} {.status.numberReady}' 2>/dev/null || echo "0 0")
    [ "${want:-0}" -gt 0 ] && [ "$want" = "$ready" ] && return 0
    sleep 2
  done
  return 1
}

wait_pod_ready() { # <name> <timeout>
  K -n "$BENCH_NS" wait --for=condition=Ready "pod/$1" --timeout="${2}s" >/dev/null
}

cleanup_workloads() {
  K -n "$BENCH_NS" delete deploy,ds,pod -l bench --wait=false --ignore-not-found >/dev/null 2>&1 || true
}

# measure <prefix> <t0> [image]: per-pod pull time (kubelet's Pulled event) and
# start-to-Ready. Prints: nodes wall_s p50 p95 max already_present
measure() {
  local prefix=$1 t0=$2 img=${3:-} pulls ready
  pulls=$(K -n "$BENCH_NS" get events --field-selector reason=Pulled -o json \
    | jq -r --arg p "$prefix" --arg i "$img" '.items[] | select(.involvedObject.name | startswith($p))
        | select($i == "" or (.message | contains("\"" + $i + "\""))) | .message' \
    | awk '
      function dur(s,   t, tok, n, u) { t = 0
        while (match(s, /^[0-9.]+(h|ms|m|s|us|ns)/)) {
          tok = substr(s, 1, RLENGTH); s = substr(s, RLENGTH + 1); n = tok + 0; u = tok; sub(/^[0-9.]+/, "", u)
          if (u == "h") t += n * 3600; else if (u == "m") t += n * 60; else if (u == "s") t += n
          else if (u == "ms") t += n / 1000; else t += n / 1e6 }
        return t }
      /already present/ { print "P 0"; next }
      match($0, / in [0-9][^ ]* /) { print "S", dur(substr($0, RSTART + 4, RLENGTH - 5)) }')
  ready=$(K -n "$BENCH_NS" get pods -o json \
    | jq -r --arg p "$prefix" --arg i "$img" '.items[] | select(.metadata.name | startswith($p))
        | select($i == "" or .spec.containers[0].image == $i)
        | (.status.conditions[]? | select(.type=="Ready" and .status=="True") | .lastTransitionTime)' \
    | while read -r t; do echo $(( $(date -u -d "$t" +%s) - t0 )); done | sort -n | tail -1)
  printf '%s\n' "$pulls" | awk -v wall="${ready:-NA}" '
    NF == 2 { v[++n] = $2; if ($1 == "P") pres++ }
    END { if (n == 0) { print 0, wall, "NA NA NA 0"; exit }
      for (i = 1; i <= n; i++) for (j = i + 1; j <= n; j++) if (v[j] < v[i]) { t = v[i]; v[i] = v[j]; v[j] = t }
      p50 = v[int((n - 1) * 0.50) + 1]; p95 = v[int((n - 1) * 0.95) + 1]
      printf "%d %s %.1f %.1f %.1f %d\n", n, wall, p50, p95, v[n], pres }'
}

# ---------------------------------------------------------------- commands

# Every untainted node: SSH works, containerd reads hosts.toml, BENCH_HOST resolves.
preflight_nodes() {
  local n bad=0 out
  log "checking nodes over SSH ($SSH_USER)"
  for n in $(untainted_nodes); do
    if ! out=$(nssh "$n" "grep -q '^[^#]*config_path *= *\"/etc/containerd/certs.d\"' /etc/containerd/config.toml && echo cfg-ok; getent hosts '$BENCH_HOST' >/dev/null && echo dns-ok" 2>&1); then
      log "  $n: ssh failed: $out"; bad=1; continue
    fi
    case "$out" in *cfg-ok*) ;; *) log "  $n: containerd config_path isn't /etc/containerd/certs.d"; bad=1 ;; esac
    case "$out" in *dns-ok*) ;; *) log "  $n: $BENCH_HOST doesn't resolve"; bad=1 ;; esac
  done
  [ "$bad" = 0 ] || die "fix the nodes above first"
  log "nodes OK"
}

cmd_setup() {
  need jq
  [ -n "$BENCH_HOST" ] || die "set BENCH_HOST in bench/bench.env (copy bench.env.example)"
  if [ ! -f "$STATE/cm-backup.json" ]; then
    K -n "$AD_NS" get cm angryduck-config -o json >"$STATE/cm-backup.json"
    log "saved Angry Duck's ConfigMap to $STATE/cm-backup.json"
  fi
  preflight_nodes
  local tls="" ips="" dvol="" dmount="" denv=""
  if [ -n "$TLS_SECRET" ]; then tls="tls: [{hosts: [$BENCH_HOST], secretName: $TLS_SECRET}]"; fi
  K create ns "$BENCH_NS" --dry-run=client -o yaml | K apply -f - >/dev/null
  if [ -n "$PULL_SECRET_FROM" ]; then
    K -n "${PULL_SECRET_FROM%%/*}" get secret "${PULL_SECRET_FROM#*/}" -o json \
      | jq --arg ns "$BENCH_NS" '{apiVersion, kind, type, data, metadata: {name: "bench-pull", namespace: $ns}}' \
      | K apply -f - >/dev/null
    log "copied $PULL_SECRET_FROM into $BENCH_NS/bench-pull"
    ips="imagePullSecrets: [{name: bench-pull}]"
    dvol="- {name: dockercfg, secret: {secretName: bench-pull, items: [{key: .dockerconfigjson, path: config.json}]}}"
    dmount="- {name: dockercfg, mountPath: /docker, readOnly: true}"
    denv="env: [{name: DOCKER_CONFIG, value: /docker}]"
  fi
  K apply -f - >/dev/null <<EOF
apiVersion: apps/v1
kind: Deployment
metadata: {name: $REG_SVC, namespace: $BENCH_NS}
spec:
  replicas: 1
  strategy: {type: Recreate}
  selector: {matchLabels: {app: angryduck-bench-registry}}
  template:
    metadata: {labels: {app: angryduck-bench-registry}}
    spec:
      $ips
      containers:
        - name: registry
          image: $REGISTRY_IMAGE
          env:
            - {name: REGISTRY_STORAGE_DELETE_ENABLED, value: "true"}
          ports: [{containerPort: 5000}]
          readinessProbe: {httpGet: {path: /v2/, port: 5000}}
          volumeMounts: [{name: data, mountPath: /var/lib/registry}]
          resources: {requests: {cpu: 200m, memory: 128Mi}, limits: {memory: 1Gi}}
      volumes:
        - name: data
          emptyDir: {sizeLimit: $REGISTRY_SIZE_LIMIT}
---
# Pushes go here: never broken by the outage tests.
apiVersion: v1
kind: Service
metadata: {name: $REG_SVC, namespace: $BENCH_NS}
spec:
  selector: {app: angryduck-bench-registry}
  ports: [{port: 5000, targetPort: 5000}]
---
# Nodes pull through the ingress to this one; the outage tests empty its selector.
apiVersion: v1
kind: Service
metadata: {name: $REG_SVC-public, namespace: $BENCH_NS}
spec:
  selector: {app: angryduck-bench-registry}
  ports: [{port: 5000, targetPort: 5000}]
---
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: $REG_SVC
  namespace: $BENCH_NS
  annotations:
    nginx.ingress.kubernetes.io/proxy-body-size: "0"
    nginx.ingress.kubernetes.io/proxy-buffering: "off"
    nginx.ingress.kubernetes.io/proxy-request-buffering: "off"
    nginx.ingress.kubernetes.io/proxy-read-timeout: "900"
spec:
  ingressClassName: $INGRESS_CLASS
  $tls
  rules:
    - host: $BENCH_HOST
      http:
        paths:
          - {path: $BENCH_INGRESS_PATH, pathType: Prefix, backend: {service: {name: $REG_SVC-public, port: {number: 5000}}}}
---
apiVersion: v1
kind: Pod
metadata: {name: toolbox, namespace: $BENCH_NS}
spec:
  terminationGracePeriodSeconds: 0
  $ips
  containers:
    - name: toolbox
      image: $TOOLBOX_IMAGE
      command: ["sh", "-c", "sleep 2147483647"]
      $denv
      volumeMounts:
        - {name: work, mountPath: /work}
        $dmount
      resources: {requests: {cpu: 500m, memory: 256Mi}, limits: {memory: 1Gi}}
  volumes:
    - name: work
      emptyDir: {sizeLimit: 2Gi}
    $dvol
EOF
  K -n "$BENCH_NS" rollout status "deploy/$REG_SVC" --timeout=5m >/dev/null
  K -n "$BENCH_NS" wait --for=condition=Ready pod/toolbox --timeout=5m >/dev/null
  log "bench registry and toolbox are up"
  K -n "$BENCH_NS" exec toolbox -- crane digest --platform linux/amd64 "$BASE_IMAGE" >/dev/null 2>&1 \
    || die "the toolbox can't fetch BASE_IMAGE ($BASE_IMAGE) from inside the cluster; point it at a registry pods can reach (e.g. your Nexus) in bench.env"
  log "toolbox can fetch $BASE_IMAGE"

  set_mode on
  log "smoke test: a node pulls a tiny image through $BENCH_HOST"
  local run img node
  run=$(runid); img=$(push_image tiny "$run")
  node=$(pick_node)
  deploy_pinned "smoke-$run" "$img" "$node"
  if wait_pod_ready "smoke-$run" 180; then
    log "OK: $node pulled $img"
  else
    K -n "$BENCH_NS" describe pod "smoke-$run" | tail -15 >&2
    die "the node couldn't pull from $BENCH_HOST: check DNS on the nodes and the ingress certificate"
  fi
  cleanup_workloads
  log "setup done"
}

cmd_mode() { set_mode "${1:?mode: on, kubelet, mirror-only or rescue-only}"; }

# bench <scenario> <size> [repeat]
#   kubelet     Angry Duck's mirror, spread, propagation and rescue off: what kubelet alone does
#   no-webhook  everything on, CI doesn't call the webhook
#   preheat     everything on, webhook right after the push, rollout PREHEAT_LEAD seconds later
cmd_bench() {
  local scenario=${1:?scenario: kubelet, no-webhook or preheat} size=${2:?size} rep=${3:-1} shape=${4:-$SHAPE} label base="" replicas=0
  case "$scenario" in kubelet) set_mode kubelet ;; no-webhook|preheat) set_mode on ;; *) die "unknown scenario $scenario" ;; esac
  label=$scenario
  [ "$scenario" = preheat ] && label="preheat@${PREHEAT_LEAD}s" # the lead time changes everything: keep runs apart
  case "$shape" in ds) ;; rolling) label="$label+rolling" ;; *) die "unknown shape $shape (ds, rolling)" ;; esac
  if [ "$shape" = rolling ]; then
    base=$(base_ref); replicas=$(untainted_nodes | wc -l)
  fi
  local csv="$RESULTS/bench.csv"
  [ -f "$csv" ] || echo "run,scenario,size,nodes,image_mb,wall_s,pull_p50_s,pull_p95_s,pull_max_s,already_present,registry_mb,registry_copies" >"$csv"
  for _ in $(seq 1 "$rep"); do
    local run img bytes since t0 m rb name
    run=$(runid); name="b-$run"
    cleanup_workloads
    if [ "$shape" = rolling ]; then
      log "starting $name on $base ($replicas replicas, one per node)"
      deploy_rolling "$name" "$base" "$replicas"
      K -n "$BENCH_NS" rollout status "deploy/$name" --timeout="${READY_TIMEOUT}s" >/dev/null || die "the base Deployment didn't become ready"
    fi
    img=$(push_image "$size" "$run"); bytes=$(image_bytes "$size" "$run")
    since=$(rfc3339)
    if [ "$scenario" = preheat ]; then
      log "preheat: $(preheat "$img" | tr -d '\n')"
      log "rest of the pipeline: ${PREHEAT_LEAD}s"; sleep "$PREHEAT_LEAD"
    fi
    t0=$(now)
    log "rollout $name ($label, $size)"
    if [ "$shape" = rolling ]; then
      K -n "$BENCH_NS" set image "deploy/$name" "c=$img" >/dev/null
      K -n "$BENCH_NS" rollout status "deploy/$name" --timeout="${READY_TIMEOUT}s" >/dev/null || log "WARN: the rollout didn't finish within ${READY_TIMEOUT}s"
    else
      deploy_ds "$name" "$img" IfNotPresent
      wait_ds_ready "$name" "$READY_TIMEOUT" || log "WARN: not every pod was Ready within ${READY_TIMEOUT}s"
    fi
    sleep 3
    m=$(measure "$name-" "$t0" "$img")
    rb=$(registry_blob_bytes "$since")
    local line
    line=$(awk -v r="$run" -v s="$label" -v z="$size" -v m="$m" -v b="$bytes" -v rb="$rb" 'BEGIN {
      split(m, x, " ")
      printf "%s,%s,%s,%s,%.0f,%s,%s,%s,%s,%s,%.0f,%.2f\n", r, s, z, x[1], b/1048576, x[2], x[3], x[4], x[5], x[6], rb/1048576, rb/b }')
    echo "$line" >>"$csv"
    log "result: $line"
    cleanup_workloads
  done
}

cmd_bench_all() {
  local rep=${1:-3} sizes=${2:-"small medium large layers"} shapes=${3:-"ds rolling"} s z h
  for s in kubelet no-webhook preheat; do
    for h in $shapes; do
      for z in $sizes; do cmd_bench "$s" "$z" "$rep" "$h"; done
    done
  done
  cmd_report
}

pick_node() {
  if [ -n "$NODE" ]; then echo "$NODE"; return; fi
  # a schedulable node without taints, lowest disk utilization first
  local ok
  ok=$(untainted_nodes)
  ad_status | jq -r '.workers | sort_by(.utilization)[] | .node_id' | grep -Fx -f <(echo "$ok") | head -1
}

chaos_csv() { # <scenario> <size> <PASS|FAIL> <seconds> <notes>
  local csv="$RESULTS/chaos.csv"
  [ -f "$csv" ] || echo "run,scenario,size,result,seconds,notes" >"$csv"
  echo "$(runid),$1,$2,$3,$4,\"$5\"" >>"$csv"
  log "RESULT: $1 $2 -> $3 in ${4}s ($5)"
}

FILL_NODE=""
unfill() {
  [ -n "$FILL_NODE" ] || return 0
  nssh "$FILL_NODE" "rm -f '$FILL_PATH'" && log "removed $FILL_PATH on $FILL_NODE" || log "WARN: remove $FILL_PATH on $FILL_NODE by hand"
  FILL_NODE=""
}

chaos_cleanup() {
  registry_up
  unfill
  cleanup_workloads
}

# chaos <scenario> [size]
cmd_chaos() {
  local scenario=${1:?scenario} size=${2:-medium}
  trap chaos_cleanup EXIT
  cleanup_workloads
  case "$scenario" in
    outage-always)      chaos_outage_always "$size" ;;
    outage-one-holder)  chaos_outage_one_holder mirror-only "$size" ;;
    outage-rescue)      chaos_outage_one_holder rescue-only "$size" ;;
    worker-kill)        chaos_kill worker "${2:-large}" ;;
    controller-kill)    chaos_kill controller "${2:-large}" ;;
    disk-pressure)      chaos_disk_pressure "${2:-small}" ;;
    *) die "unknown chaos scenario $scenario" ;;
  esac
}

# The image is on every node, the registry dies, and a rollout with
# imagePullPolicy: Always still has to start. kubelet alone fails this.
chaos_outage_always() {
  local size=$1 run img t0 r0 r1 name
  set_mode on
  run=$(runid); img=$(push_image "$size" "$run")
  log "preheat: $(preheat "$img" | tr -d '\n')"
  wait_settled "$img" "$SPREAD_TIMEOUT" || log "WARN: spread/propagation still running at timeout"
  deploy_ds "warm-$run" "$img" IfNotPresent
  wait_ds_ready "warm-$run" "$READY_TIMEOUT" || die "couldn't warm every node"
  K -n "$BENCH_NS" delete ds "warm-$run" --wait=true >/dev/null
  r0=$(sum_worker_metric '^angryduck_worker_mirror_resolves_total[{].*origin="(unreachable|missing|denied)"')
  registry_down; sleep 10
  name="always-$run"; t0=$(now)
  deploy_ds "$name" "$img" Always
  if wait_ds_ready "$name" "$READY_TIMEOUT"; then
    r1=$(sum_worker_metric '^angryduck_worker_mirror_resolves_total[{].*origin="(unreachable|missing|denied)"')
    chaos_csv outage-always "$size" PASS $(( $(now) - t0 )) "every pod started with the registry down; tags resolved from peers while unreachable: $((r1 - r0))"
  else
    chaos_csv outage-always "$size" FAIL $(( $(now) - t0 )) "$(K -n "$BENCH_NS" get pods -l bench="$name" --no-headers | awk '$3!="Running"' | wc -l) pods not running"
  fi
}

# One node has the image (spread and propagation off), the registry dies, the
# rollout goes to every node. mirror-only: peers serve the pulls.
# rescue-only: pulls fail and rescue must fetch the image from that node.
chaos_outage_one_holder() {
  local mode=$1 size=$2 run img node t0 name timeout m0 m1 metric scen
  set_mode "$mode"
  if [ "$mode" = mirror-only ]; then
    scen=outage-one-holder; timeout=$READY_TIMEOUT
    metric='^angryduck_worker_mirror_bytes_total[{].*source="peer"'
  else
    scen=outage-rescue; timeout=$RESCUE_TIMEOUT
    metric='^angryduck_worker_rescues_total[{].*reason="rescue".*result="success"'
  fi
  run=$(runid); img=$(push_image "$size" "$run"); node=$(pick_node)
  deploy_pinned "holder-$run" "$img" "$node"
  wait_pod_ready "holder-$run" 600 || die "the holder node couldn't pull"
  log "holder: $node; letting inventories settle"; sleep 30
  m0=$(sum_worker_metric "$metric")
  registry_down; sleep 10
  name="spread-$run"; t0=$(now)
  deploy_ds "$name" "$img" IfNotPresent
  if wait_ds_ready "$name" "$timeout"; then
    m1=$(sum_worker_metric "$metric")
    if [ "$mode" = mirror-only ]; then
      chaos_csv "$scen" "$size" PASS $(( $(now) - t0 )) "registry down, one holder; MB served by peers' mirrors: $(( (m1 - m0) / 1048576 ))"
    else
      chaos_csv "$scen" "$size" PASS $(( $(now) - t0 )) "registry down, mirror off, one holder; successful rescues: $((m1 - m0)) (includes kubelet backoff)"
    fi
  else
    chaos_csv "$scen" "$size" FAIL $(( $(now) - t0 )) "$(K -n "$BENCH_NS" get pods -l bench="$name" --no-headers | awk '$3!="Running"' | wc -l) pods not running"
  fi
}

# Kill a worker on two seeds, or the controller, a few seconds into a spread.
# The spread must still finish, and a rollout after it must find the image on
# every node.
chaos_kill() {
  local what=$1 size=$2 run img ans seeds t0 name m ok s
  set_mode on
  run=$(runid); img=$(push_image "$size" "$run")
  ans=$(preheat "$img"); log "preheat: $(tr -d '\n' <<<"$ans")"
  t0=$(now); sleep "$KILL_AFTER"
  if [ "$what" = worker ]; then
    seeds=$(jq -r '.ordered_nodes[:2][]' <<<"$ans")
    for s in $seeds; do
      log "killing the worker on seed $s mid-spread"
      K -n "$AD_NS" delete pod "$(worker_pod "$s")" --wait=false >/dev/null
    done
  else
    log "killing the controller mid-spread"
    K -n "$AD_NS" delete pod -l app.kubernetes.io/component=controller --wait=false >/dev/null
    sleep 5
    K -n "$AD_NS" rollout status deploy/angryduck-controller --timeout=5m >/dev/null
    sleep 30 # workers report to the new controller
  fi
  ok=1; wait_settled "$img" "$SPREAD_TIMEOUT" || ok=0
  local spread_s=$(( $(now) - t0 ))
  name="after-$run"; t0=$(now)
  deploy_ds "$name" "$img" IfNotPresent
  wait_ds_ready "$name" "$READY_TIMEOUT" || ok=0
  sleep 3; m=$(measure "$name-" "$t0")
  local nodes pres broken
  read -r nodes _ _ _ _ pres <<<"$m"
  broken=$(sum_worker_metric '^angryduck_worker_broken_image')
  [ "$broken" = 0 ] || ok=0
  local res=FAIL
  if [ "$ok" = 1 ]; then res=PASS; [ "$pres" = "$nodes" ] || res=DEGRADED; fi
  chaos_csv "$what-kill" "$size" "$res" "$spread_s" "image already on $pres/$nodes nodes at rollout; broken images: $broken; settled=$([ "$ok" = 1 ] && echo yes || echo no)"
}

# Fill one node's root filesystem past GC_HIGH_UTILIZATION (a fallocate'd
# file over SSH) while a pod runs a bench image there. Cleanup may remove
# unused images, never the running one.
chaos_disk_pressure() {
  local size=$1 run img node t0 d0 d1 stalled max_util=0 u
  set_mode on
  awk -v t="$FILL_TARGET" 'BEGIN { exit !(t > 0 && t <= 0.82) }' || die "FILL_TARGET must be in (0, 0.82]"
  run=$(runid); img=$(push_image "$size" "$run"); node=$(pick_node)
  deploy_pinned "inuse-$run" "$img" "$node"
  wait_pod_ready "inuse-$run" 600 || die "couldn't start the in-use pod"
  d0=$(node_metric "$node" '^angryduck_worker_gc_images_deleted_total')
  log "filling $node's root filesystem to $FILL_TARGET over SSH ($FILL_PATH)"
  FILL_NODE=$node
  nssh "$node" "set -e
    root=\$(df --output=source / | tail -1); here=\$(df --output=source \$(dirname '$FILL_PATH') | tail -1)
    [ \"\$root\" = \"\$here\" ] || { echo '$FILL_PATH is not on the root filesystem'; exit 3; }
    set -- \$(df -Pk / | tail -1); total=\$2; used=\$3
    mb=\$(awk -v t=\$total -v u=\$used -v f=$FILL_TARGET 'BEGIN { x = int((t * f - u) / 1024); print (x > 0 ? x : 0) }')
    echo \"filling \${mb}MB\"
    [ \"\$mb\" -gt 0 ] && fallocate -l \${mb}M '$FILL_PATH'
    df -h /" >&2 || die "filling $node failed"
  t0=$(now)
  log "holding for ${FILL_HOLD}s"
  while [ $(( $(now) - t0 )) -lt "$FILL_HOLD" ]; do
    u=$(ad_status | jq -r --arg n "$node" '.workers[] | select(.node_id==$n) | .utilization')
    max_util=$(awk -v a="$max_util" -v b="${u:-0}" 'BEGIN { print (b > a ? b : a) }')
    log "  $node utilization $u, cleaning=$(node_metric "$node" '^angryduck_worker_gc_cleaning') stalled=$(node_metric "$node" '^angryduck_worker_gc_stalled')"
    sleep 30
  done
  d1=$(node_metric "$node" '^angryduck_worker_gc_images_deleted_total')
  stalled=$(node_metric "$node" '^angryduck_worker_gc_stalled')
  unfill
  # The in-use image must have survived: restarting the pod finds it locally.
  K -n "$BENCH_NS" delete pod "inuse-$run" --wait=true >/dev/null
  t0=$(now); deploy_pinned "inuse2-$run" "$img" "$node"
  wait_pod_ready "inuse2-$run" 300 || true
  local m pres
  m=$(measure "inuse2-$run" "$t0"); pres=$(awk '{print $6}' <<<"$m")
  local res=FAIL; [ "$pres" = 1 ] && res=PASS
  chaos_csv disk-pressure "$size" "$res" "$FILL_HOLD" "node $node peaked at $max_util; images removed: $((d1 - d0)); stalled at end: $stalled; in-use image kept: $([ "$pres" = 1 ] && echo yes || echo no)"
}

cmd_report() {
  local f="$RESULTS/bench.csv"
  if [ -f "$f" ]; then
    echo "== bench (means per scenario and size)"
    printf "%-8s %-19s %4s %8s %10s %10s %9s %9s\n" size scenario runs wall_s pull_p95 pull_max reg_copy present
    awk -F, 'NR > 1 { k = $3 "," $2; n[k]++; w[k] += $6; p[k] += $8; m[k] += $9; rc[k] += $12; ap[k] += $10/$4 }
      END { for (k in n) { split(k, a, ","); printf "%-8s %-19s %4d %8.1f %10.1f %10.1f %9.2f %8.0f%%\n", a[1], a[2], n[k], w[k]/n[k], p[k]/n[k], m[k]/n[k], rc[k]/n[k], 100*ap[k]/n[k] } }' "$f" | sort
  fi
  f="$RESULTS/chaos.csv"
  if [ -f "$f" ]; then echo; echo "== chaos"; column -s, -t <"$f"; fi
}

cmd_reset_registry() {
  log "wiping the bench registry's storage"
  K -n "$BENCH_NS" rollout restart "deploy/$REG_SVC" >/dev/null
  K -n "$BENCH_NS" rollout status "deploy/$REG_SVC" --timeout=5m >/dev/null
}

cmd_teardown() {
  registry_up
  if [ -f "$STATE/cm-backup.json" ]; then
    log "restoring Angry Duck's original ConfigMap"
    apply_cm_data "$(jq -c '.data' "$STATE/cm-backup.json")"
    restart_ad
    rm -f "$STATE/cm-backup.json" "$STATE/mode"
  fi
  K delete ns "$BENCH_NS" --wait=false --ignore-not-found >/dev/null
  log "teardown done; results stay in $RESULTS"
}

usage() {
  cat <<EOF
usage: $0 <command>
  setup                              registry, toolbox, ConfigMap backup, smoke test
  bench <kubelet|no-webhook|preheat> <size> [repeat] [ds|rolling]
  bench-all [repeat] ["sizes"] ["shapes"]  every scenario x shape x size, then report
  chaos <scenario> [size]            outage-always | outage-one-holder | outage-rescue |
                                     worker-kill | controller-kill | disk-pressure
  mode <on|kubelet|mirror-only|rescue-only>
  report                             summarize results/
  reset-registry                     wipe the bench registry's storage
  teardown                           restore Angry Duck's ConfigMap, delete the bench namespace
sizes: tiny small medium large layers
EOF
}

need jq
need ssh
case "${1:-}" in
  setup) cmd_setup ;;
  bench) shift; cmd_bench "$@" ;;
  bench-all) shift; cmd_bench_all "$@" ;;
  chaos) shift; cmd_chaos "$@" ;;
  mode) shift; cmd_mode "$@" ;;
  report) cmd_report ;;
  reset-registry) cmd_reset_registry ;;
  teardown) cmd_teardown ;;
  *) usage; exit 1 ;;
esac
