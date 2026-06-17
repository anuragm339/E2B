#!/usr/bin/env bash
#
# admin-verify.sh — interactive verifier for the broker admin/status API.
#
# Usage:
#   ./scripts/admin-verify.sh                 # interactive menu
#   ./scripts/admin-verify.sh 1               # run option 1 directly, then exit
#   BROKER=http://host:8081 ./scripts/admin-verify.sh
#   TOPIC=prices-v1 ./scripts/admin-verify.sh 12
#
# Requires: curl, jq (falls back to python3/raw if jq is missing).

set -uo pipefail

BROKER="${BROKER:-http://localhost:8081}"
TOPIC="${TOPIC:-}"
TAB=$(printf '\t')

# Reusable jq definition: humanize a byte count to B/KB/MB/GB (no decimals chasing).
JQ_HB='def hb: if type=="number" then
                 if .>=1073741824 then "\((.*10/1073741824|floor)/10)GB"
                 elif .>=1048576 then "\((.*10/1048576|floor)/10)MB"
                 elif .>=1024 then "\((.*10/1024|floor)/10)KB"
                 else "\(.)B" end
               else . end;'

# ── formatting ──────────────────────────────────────────────────────────────
if [ -t 1 ]; then
  BOLD=$'\033[1m'; DIM=$'\033[2m'; RST=$'\033[0m'
  GRN=$'\033[32m'; RED=$'\033[31m'; YEL=$'\033[33m'; CYN=$'\033[36m'
else
  BOLD=; DIM=; RST=; GRN=; RED=; YEL=; CYN=
fi
HAS_JQ=0; command -v jq >/dev/null 2>&1 && HAS_JQ=1

pretty() {  # pretty-print JSON from stdin; pass through if not JSON
  if [ "$HAS_JQ" = 1 ]; then
    if [ -t 1 ]; then jq -C . 2>/dev/null || cat; else jq . 2>/dev/null || cat; fi
  elif command -v python3 >/dev/null 2>&1; then
    python3 -m json.tool 2>/dev/null || cat
  else
    cat
  fi
}

# Rewrite JSON for humans before display: any *bytes*/size field -> KB/MB/GB,
# and epoch-millis timestamps (ts / *Seen / *EpochMs / numeric *Time) -> a readable date.
# Passes non-JSON (e.g. a thread dump) straight through.
humanize_json() {
  [ "$HAS_JQ" = 1 ] || { cat; return; }
  jq "$JQ_HB"'
    def hd: if type=="number" and . > 1000000000000
            then (./1000 | strftime("%Y-%m-%d %H:%M:%S UTC")) else . end;
    walk(
      if type == "object" then
        with_entries(
          if (.key | test("[Bb]ytes") or test("(maxSegmentSize|dataSize)$")) then .value |= hb
          elif (.key | test("(^ts$|Seen$|EpochMs$|Time$)")) then .value |= hd
          else . end
        )
      else . end
    )
  ' 2>/dev/null || cat
}

hr()    { printf "${DIM}%s${RST}\n" "------------------------------------------------------------"; }
title() { echo; printf "${BOLD}${CYN}== %s ==${RST}\n" "$1"; hr; }

fetch() { curl -s --max-time 15 "$BROKER$1"; }

# call METHOD PATH [curl-args...] — prints method/path, colored status, pretty body
call() {
  local method=$1 path=$2; shift 2
  local out code body
  out=$(curl -s --max-time 20 -w $'\n%{http_code}' -X "$method" "$BROKER$path" "$@" 2>/dev/null)
  code=${out##*$'\n'}; body=${out%$'\n'*}
  if [ "$code" = 200 ] || [ "$code" = 202 ]; then
    printf "${DIM}%s${RST} %s  ${GRN}HTTP %s${RST}\n" "$method" "$path" "$code"
  else
    printf "${DIM}%s${RST} %s  ${RED}HTTP %s${RST}\n" "$method" "$path" "${code:-ERR}"
  fi
  [ -n "$body" ] && printf '%s\n' "$body" | humanize_json | pretty
  echo
}

# jqf PATH PROGRAM — fetch a path and run a jq program; silent if jq missing
jqf() {
  [ "$HAS_JQ" = 1 ] || return 0
  fetch "$1" | jq -r "$2" 2>/dev/null
}

pick_topic() {  # resolve a topic: $TOPIC env, else first topic from the broker
  if [ -n "$TOPIC" ]; then printf '%s\n' "$TOPIC"; return; fi
  local t; t=$(jqf /admin/status/topics '.topics[0].topic // empty')
  [ -z "$t" ] && t="prices-v1"
  printf '%s\n' "$t"
}

require_broker() {
  if ! curl -s --max-time 5 -o /dev/null "$BROKER/health"; then
    printf "${RED}Cannot reach broker at %s${RST}\n" "$BROKER"
    printf "Set a different one with:  ${BOLD}BROKER=http://host:port %s${RST}\n" "$0"
    exit 1
  fi
}

# ── verifications ───────────────────────────────────────────────────────────
v_health() {
  title "Overall health & diagnosis"
  printf "Health   : %s\n" "$(jqf /health '.status')"
  printf "Diagnosis: ${BOLD}%s${RST}\n" "$(jqf /admin/status/diagnosis '.status')"
  echo "Checks:"
  jqf /admin/status/diagnosis '.checks[] | "  - \(.name): \(if .ok then "OK" else "FAIL" end)  (\(.detail))"'
}

v_pipe() {
  title "Pipe / ingestion"
  jqf /admin/status/pipe '"role=\(.role)  health=\(.health)  pausedForRefresh=\(.pausedForRefresh)\nupstreamCursor=\(.upstreamCursor)  lastPollAgeMs=\(.lastSuccessfulPollAgeMs)  parent=\(.parentUrl)"'
  echo; echo "Raw:"; call GET /admin/status/pipe
}

v_consumers() {
  title "Consumers & lag"
  jqf /admin/status/consumers '.summary | "consumers=\(.consumerCount)  totalLag=\(.totalLag)\nbyState=\(.byState)\nworstByLag=\(.worstByLag.group // "n/a") lag=\(.worstByLag.lag // 0)"'
  echo; echo "Raw:"; call GET /admin/status/consumers
}

v_topics() {
  title "Topics & offsets"
  { printf 'TOPIC\tHEAD\tDURABLE\tEARLIEST\n'
    fetch /admin/status/topics | jq -r '.topics[] | [.topic, .headOffset, .durableMaxOffset, .earliestOffset] | @tsv' 2>/dev/null
  } | column -t -s "$TAB"
}

v_refresh() {
  title "Refresh state"
  echo "Current:"; call GET /admin/refresh-current
  local t; t=$(pick_topic)
  printf "Per-topic (${BOLD}%s${RST}):\n" "$t"
  call GET "/admin/refresh-status?topic=$t"
}

v_errors() {
  local lvl="${LEVEL:-ERROR}"
  title "Errors  (level=$lvl — set LEVEL=WARN to include warnings)"
  echo "Top unique errors (what / how / why):"
  fetch "/admin/status/errors/top?level=$lvl&limit=10" | jq -r '
    .top[]? |
      "  \(.count)x  what: \(.what)\n        how : \(.how)\n        why : \(.why)\n        last=\((.lastSeen/1000)|strftime("%Y-%m-%d %H:%M:%S"))  trace=\(.sampleTraceId // "-")\n"' 2>/dev/null
  echo
  local tid
  tid=$(fetch "/admin/status/errors?level=$lvl&limit=1" | jq -r '.errors[0].traceId // empty' 2>/dev/null)
  if [ -n "$tid" ]; then
    printf "Full trace of most recent error (${BOLD}%s${RST}), start -> end:\n" "$tid"
    fetch "/admin/status/errors/trace/$tid?level=$lvl" | jq -r '
      .chain[]? | "  \((.ts/1000)|strftime("%H:%M:%S")) \(.level) \(.logger // "?"): \((.message // "")[0:140])"' 2>/dev/null
  else
    echo "(no recent error carrying a traceId at level=$lvl)"
  fi
}

v_consistency() {
  title "Pipe consistency"
  jqf /admin/status/consistency '.summary | "total=\(.total)  byState=\(.byState)"'
  echo; echo "Per-topic report:"
  call GET /admin/pipe-consistency/report
}

v_storage() {
  title "Storage - segments, bytes & offsets"
  local d="${DISK:-false}"
  fetch "/admin/status/storage?disk=$d" | jq -r "$JQ_HB"'"totals: topics=\(.topicCount)  segments=\(.totalSegments)  size=\(.totalBytes|hb)\(if .totalDiskBytes != null then "  onDisk=\(.totalDiskBytes|hb)" else "" end)"' 2>/dev/null
  echo
  { printf 'TOPIC\tHEAD\tDURABLE\tdLAG\tSEGS\tSEALED\tTOTAL_SIZE\n'
    fetch "/admin/status/storage?disk=$d" | jq -r "$JQ_HB"'.topics[] | [.topic,.headOffset,.durableMaxOffset,.durabilityLag,.totalSegments,.sealedSegmentCount,(.totalBytes|hb)] | @tsv' 2>/dev/null
  } | column -t -s "$TAB"
  echo
  local t; t=$(pick_topic)
  printf "Segment inventory (${BOLD}%s${RST}):\n" "$t"
  { printf 'BASE\tNEXT\tRECORDS\tSIZE\tACTIVE\tFULL\tLOGFILE\n'
    fetch "/admin/status/storage/$t?disk=$d" | jq -r "$JQ_HB"'.segments[] | [.baseOffset,.nextOffset,.recordCount,(.sizeBytes|hb),.active,.full,.logFile] | @tsv' 2>/dev/null
  } | column -t -s "$TAB"
}

v_failed() {
  title "Failed messages (last 20 records — which records failed, and their disposition)"
  fetch /admin/status/failed-messages | jq -r '
    if (.count // 0) == 0 then "  none — no failed records currently tracked"
    else (.failedMessages[] |
      "  [\(.disposition)]  \(.topic)#\(.offset)  key=\(.key // "-")  attempts=\(.attempts)\n        reason: \(.reason)\n        last=\((.lastSeen/1000)|strftime("%Y-%m-%d %H:%M:%S"))\(if .group then "  group=\(.group)" else "" end)")
    end' 2>/dev/null
}

v_ack()       { title "ACK integrity";       call GET /admin/status/ack-integrity; }
v_inflight()  { title "In-flight deliveries"; call GET /admin/status/in-flight; }
v_perf()      { title "Performance metrics";  call GET /admin/status/performance; }
v_compaction(){ title "Compaction status";    call GET /admin/compaction/status; }

v_threads() {
  title "Thread diagnostics"
  echo "Summary:";    call GET /diagnostics/threads/summary
  echo "Deadlocks:";  call GET /diagnostics/threads/deadlocks
  echo "Problematic:";call GET /diagnostics/threads/problematic
}

v_peek() {
  local t; t=$(pick_topic)
  title "Peek records — topic '$t'"
  call GET "/admin/status/topics/$t/peek?limit=5"
}

v_full() {
  v_health; v_pipe; v_consumers; v_topics; v_storage; v_refresh
  v_errors; v_failed; v_consistency; v_ack; v_inflight; v_perf; v_compaction
}

v_trigger_refresh() {
  local t; t=$(pick_topic)
  title "Trigger data refresh — topic '$t'  (WRITES / pauses pipe)"
  printf "${YEL}This pauses pipe ingestion until the refresh completes (consumers replay + READY-ack).${RST}\n"
  read -r -p "Type the topic name to confirm [$t]: " ans
  [ -n "$ans" ] && t="$ans"
  read -r -p "Proceed with refresh of '$t'? [y/N] " yn
  case "$yn" in [yY]*) ;; *) echo "aborted."; return;; esac
  call POST /admin/refresh-topic -H 'Content-Type: application/json' -d "{\"topic\":\"$t\"}"
  echo "Monitoring until COMPLETED / pipe resumes (max ~3 min)..."
  for i in $(seq 1 36); do
    local st state off rdy paused
    st=$(fetch "/admin/refresh-status?topic=$t")
    state=$(printf '%s' "$st" | jq -r '.state // "?"' 2>/dev/null)
    off=$(printf '%s' "$st"   | jq -r '[.consumerDetails[]?.currentOffset] | max // "?"' 2>/dev/null)
    rdy=$(printf '%s' "$st"   | jq -r '.receivedReadyAcks // 0' 2>/dev/null)
    paused=$(jqf /admin/status/pipe '.pausedForRefresh')
    printf "  t+%03ds  state=%-12s maxOffset=%-10s readyAcks=%s  pipePaused=%s\n" "$((i*5))" "$state" "$off" "$rdy" "$paused"
    case "$state" in COMPLETED|ABORTED|"?"|null) echo "  >>> finished (state=$state)"; break;; esac
    sleep 5
  done
}

# ── menu ────────────────────────────────────────────────────────────────────
menu() {
  echo
  printf "${BOLD}Broker admin verifier${RST}  ${DIM}(%s)${RST}\n" "$BROKER"
  cat <<'EOF'
  What do you want to verify?
   1) Overall health & diagnosis
   2) Pipe / ingestion status
   3) Consumers & lag
   4) Topics & offsets
   5) Refresh state
   6) Errors: top-unique + recent-error trace  (LEVEL=WARN to include warnings)
   7) Pipe consistency
   8) ACK integrity
   9) In-flight deliveries
  10) Performance metrics
  11) Compaction status
  12) Peek a topic's records
  13) Thread diagnostics
  14) FULL read-only sweep (all read-only checks)
  15) Storage: segments / bytes / offsets   (DISK=1 for on-disk sizes)
  16) Failed messages: which records failed (topic/offset/key, last 20)

  90) Trigger a data refresh   (WRITES - pauses pipe, then monitors)
   0) Exit
EOF
  printf "Select: "
}

run_option() {
  case "$1" in
    1) v_health;;       2) v_pipe;;        3) v_consumers;;   4) v_topics;;
    5) v_refresh;;      6) v_errors;;      7) v_consistency;; 8) v_ack;;
    9) v_inflight;;    10) v_perf;;       11) v_compaction;; 12) v_peek;;
   13) v_threads;;     14) v_full;;       15) v_storage;;
   16) v_failed;;      90) v_trigger_refresh;;
    0) return 1;;
    *) printf "${RED}Unknown option: %s${RST}\n" "$1";;
  esac
  return 0
}

require_broker

# Non-interactive: option passed as arg
if [ $# -ge 1 ]; then run_option "$1"; exit 0; fi

# Interactive loop
while true; do
  menu
  read -r choice || break
  [ -z "$choice" ] && continue
  run_option "$choice" || { echo "bye."; break; }
  echo; read -r -p "Enter to return to menu... " _ || break
done
