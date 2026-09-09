#!/bin/bash
# Phase 2 Manual Verification Script
# Checks all 3 exit criteria from implementation_plan.md §Phase 2
# Usage: bash verify-phase2.sh
# Requires cluster to already be running via scripts/run-cluster.sh

set -euo pipefail

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'
PASS=0; FAIL=0

ok()   { echo -e "${GREEN}  ✓ $*${NC}"; PASS=$((PASS+1)); }
fail() { echo -e "${RED}  ✗ $*${NC}"; FAIL=$((FAIL+1)); }
info() { echo -e "${CYAN}  → $*${NC}"; }
banner() { echo -e "\n${YELLOW}══════════════════════════════════════════${NC}"; echo -e "${YELLOW}  $*${NC}"; echo -e "${YELLOW}══════════════════════════════════════════${NC}"; }

ROUTER="http://127.0.0.1:9000"

banner "PRE-CHECK: Cluster reachability"

if ! curl -sf "$ROUTER/cluster/shards" > /dev/null 2>&1; then
    echo -e "${RED}Router not reachable at $ROUTER. Start the cluster first.${NC}"
    exit 1
fi
ok "Router responding at $ROUTER"

banner "EXIT CRITERION 1: 9 nodes, 3 independent Raft groups"

SHARDS_JSON=$(curl -sf "$ROUTER/cluster/shards")
SHARD_COUNT=$(echo "$SHARDS_JSON" | python3 -c "import sys,json; d=json.load(sys.stdin); print(len(d['shards']))")

if [ "$SHARD_COUNT" -eq 3 ]; then
    ok "Router knows about 3 shards"
else
    fail "Expected 3 shards, got $SHARD_COUNT"
fi

for ADDR in \
    "127.0.0.1:8100" "127.0.0.1:8110" "127.0.0.1:8120" \
    "127.0.0.1:8130" "127.0.0.1:8140" "127.0.0.1:8150" \
    "127.0.0.1:8160" "127.0.0.1:8170" "127.0.0.1:8180"; do
    if curl -sf "http://$ADDR/cluster/health" > /dev/null 2>&1; then
        ok "Node $ADDR: responding"
    else
        fail "Node $ADDR: not responding"
    fi
done

echo ""
info "Checking each shard has exactly 1 leader..."
for SHARD_BASE in "8100 8110 8120:A" "8130 8140 8150:B" "8160 8170 8180:C"; do
    PORTS=$(echo "$SHARD_BASE" | cut -d: -f1)
    SID=$(echo "$SHARD_BASE" | cut -d: -f2)
    LEADER_COUNT=0
    LEADERS=""
    for PORT in $PORTS; do
        H=$(curl -sf "http://127.0.0.1:$PORT/cluster/health" 2>/dev/null || echo '{}')
        IS_L=$(echo "$H" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('is_leader',''))" 2>/dev/null || echo "")
        SHARD_REPORTED=$(echo "$H" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('shard_id',''))" 2>/dev/null || echo "")
        STATE=$(echo "$H" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('raft_state','?'))" 2>/dev/null || echo "?")
        echo "    Port $PORT → shard=$SHARD_REPORTED state=$STATE is_leader=$IS_L"
        if [ "$IS_L" = "True" ]; then
            LEADER_COUNT=$((LEADER_COUNT+1))
            LEADERS="$LEADERS 127.0.0.1:$PORT"
        fi
        if [ "$SHARD_REPORTED" != "$SID" ]; then
            fail "Port $PORT claims shard_id='$SHARD_REPORTED', expected '$SID' — wrong shard!"
        fi
    done
    if [ "$LEADER_COUNT" -eq 1 ]; then
        ok "Shard $SID: exactly 1 leader elected ($LEADERS)"
    elif [ "$LEADER_COUNT" -eq 0 ]; then
        fail "Shard $SID: NO leader elected yet"
    else
        fail "Shard $SID: $LEADER_COUNT nodes claim leadership (split brain?)"
    fi
done

banner "EXIT CRITERION 2: Router routes keys across all 3 shards"

TEST_KEYS=("alpha" "beta" "gamma" "delta" "epsilon" "zeta" "eta" "theta" "iota" "kappa"
           "lambda" "mu" "nu" "xi" "omicron" "pi" "rho" "sigma" "tau" "upsilon"
           "phi" "chi" "psi" "omega")

info "PUTting ${#TEST_KEYS[@]} keys through router..."
PUT_FAIL=0
for KEY in "${TEST_KEYS[@]}"; do
    STATUS=$(curl -sf -o /dev/null -w "%{http_code}" -X PUT "$ROUTER/kv/$KEY" \
        -H "Content-Type: application/json" -d "{\"value\":\"val-$KEY\"}" 2>/dev/null)
    if [ "$STATUS" != "200" ]; then
        echo "    PUT $KEY → HTTP $STATUS (FAILED)"
        PUT_FAIL=$((PUT_FAIL+1))
    fi
done
if [ "$PUT_FAIL" -eq 0 ]; then
    ok "All ${#TEST_KEYS[@]} PUT operations succeeded"
else
    fail "$PUT_FAIL PUT operations failed"
fi

info "GETting all keys back through router..."
GET_FAIL=0; WRONG_VAL=0
for KEY in "${TEST_KEYS[@]}"; do
    RESP=$(curl -sf "$ROUTER/kv/$KEY" 2>/dev/null || echo '{}')
    STATUS=$(curl -sf -o /dev/null -w "%{http_code}" "$ROUTER/kv/$KEY" 2>/dev/null)
    VAL=$(echo "$RESP" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('value','MISSING'))" 2>/dev/null || echo "MISSING")
    if [ "$STATUS" != "200" ]; then
        GET_FAIL=$((GET_FAIL+1))
        echo "    GET $KEY → HTTP $STATUS (FAILED)"
    elif [ "$VAL" != "val-$KEY" ]; then
        WRONG_VAL=$((WRONG_VAL+1))
        echo "    GET $KEY → got value='$VAL', expected 'val-$KEY' (WRONG VALUE)"
    fi
done
if [ "$GET_FAIL" -eq 0 ] && [ "$WRONG_VAL" -eq 0 ]; then
    ok "All ${#TEST_KEYS[@]} GET operations returned correct values"
else
    [ "$GET_FAIL" -gt 0 ] && fail "$GET_FAIL GET operations failed"
    [ "$WRONG_VAL" -gt 0 ] && fail "$WRONG_VAL GET operations returned wrong values"
fi

info "Verifying key distribution across shards..."
SHARDS_DETAIL=$(curl -sf "$ROUTER/cluster/shards")
declare -A SHARD_KEY_COUNT
SHARD_KEY_COUNT["A"]=0; SHARD_KEY_COUNT["B"]=0; SHARD_KEY_COUNT["C"]=0

LEADER_A=$(echo "$SHARDS_DETAIL" | python3 -c "import sys,json; d=json.load(sys.stdin); l=[s['leader_addr'] for s in d['shards'] if s['id']=='A']; print(l[0] if l else '')" 2>/dev/null)
LEADER_B=$(echo "$SHARDS_DETAIL" | python3 -c "import sys,json; d=json.load(sys.stdin); l=[s['leader_addr'] for s in d['shards'] if s['id']=='B']; print(l[0] if l else '')" 2>/dev/null)
LEADER_C=$(echo "$SHARDS_DETAIL" | python3 -c "import sys,json; d=json.load(sys.stdin); l=[s['leader_addr'] for s in d['shards'] if s['id']=='C']; print(l[0] if l else '')" 2>/dev/null)

info "Leaders: A=$LEADER_A  B=$LEADER_B  C=$LEADER_C"

MISPLACED=0
for KEY in "${TEST_KEYS[@]}"; do
    FOUND_ON=""
    for SID in A B C; do
        eval "LADDR=\$LEADER_$SID"
        if [ -n "$LADDR" ]; then
            STATUS=$(curl -s -o /dev/null -w "%{http_code}" "http://$LADDR/kv/$KEY" 2>/dev/null || echo "FAIL")
            if [ "$STATUS" = "200" ]; then
                FOUND_ON="$SID"
                SHARD_KEY_COUNT[$SID]=$((${SHARD_KEY_COUNT[$SID]}+1))
            fi
        fi
    done
    if [ -z "$FOUND_ON" ]; then
        fail "Key '$KEY' not found on ANY shard leader"
        MISPLACED=$((MISPLACED+1))
    fi
done

echo "    Key distribution: A=${SHARD_KEY_COUNT[A]}  B=${SHARD_KEY_COUNT[B]}  C=${SHARD_KEY_COUNT[C]}"
if [ "${SHARD_KEY_COUNT[A]}" -gt 0 ] && [ "${SHARD_KEY_COUNT[B]}" -gt 0 ] && [ "${SHARD_KEY_COUNT[C]}" -gt 0 ]; then
    ok "Keys distributed across all 3 shards (A=${SHARD_KEY_COUNT[A]}, B=${SHARD_KEY_COUNT[B]}, C=${SHARD_KEY_COUNT[C]})"
else
    fail "Not all shards received keys — A=${SHARD_KEY_COUNT[A]}, B=${SHARD_KEY_COUNT[B]}, C=${SHARD_KEY_COUNT[C]}"
fi

banner "EXIT CRITERION 3: Shard isolation (kill leader of shard A)"

A_LEADER_PORT=$(echo "$LEADER_A" | cut -d: -f2)
A_LEADER_PID=""
for NODE in A1 A2 A3; do
    PF="/tmp/shardkv/${NODE}.pid"
    if [ -f "$PF" ]; then
        PID=$(cat "$PF")
        HPORT=""
        case $NODE in
          A1) HPORT=8100 ;; A2) HPORT=8110 ;; A3) HPORT=8120 ;;
        esac
        if [ "$HPORT" = "$A_LEADER_PORT" ] && kill -0 "$PID" 2>/dev/null; then
            A_LEADER_PID=$PID
            info "Shard A leader: $NODE (PID=$PID, HTTP=127.0.0.1:$A_LEADER_PORT)"
            break
        fi
    fi
done

if [ -z "$A_LEADER_PID" ]; then
    fail "Could not identify shard A leader PID — skipping isolation test"
else
    B_BEFORE=$(curl -sf "$ROUTER/kv/beta" 2>/dev/null | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('value','ERR'))" 2>/dev/null || echo "ERR")
    C_BEFORE=$(curl -sf "$ROUTER/kv/gamma" 2>/dev/null | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('value','ERR'))" 2>/dev/null || echo "ERR")
    info "Pre-kill check — shard B read: '$B_BEFORE'  shard C read: '$C_BEFORE'"

    info "Killing shard A leader (PID $A_LEADER_PID) with SIGKILL..."
    kill -9 "$A_LEADER_PID" 2>/dev/null
    sleep 6  

    if kill -0 "$A_LEADER_PID" 2>/dev/null; then
        fail "Process $A_LEADER_PID still alive after SIGKILL"
    else
        ok "Shard A old leader (PID $A_LEADER_PID) is dead"
    fi

    B_AFTER=$(curl -sf "$ROUTER/kv/beta" 2>/dev/null | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('value','ERR'))" 2>/dev/null || echo "ERR")
    C_AFTER=$(curl -sf "$ROUTER/kv/gamma" 2>/dev/null | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('value','ERR'))" 2>/dev/null || echo "ERR")
    info "Post-kill check — shard B read: '$B_AFTER'  shard C read: '$C_AFTER'"

    if [ "$B_AFTER" = "val-beta" ]; then
        ok "Shard B unaffected — GET returned correct value after shard A leader death"
    else
        fail "Shard B disrupted — expected 'val-beta', got '$B_AFTER'"
    fi
    if [ "$C_AFTER" = "val-gamma" ]; then
        ok "Shard C unaffected — GET returned correct value after shard A leader death"
    else
        fail "Shard C disrupted — expected 'val-gamma', got '$C_AFTER'"
    fi

    info "Waiting for shard A new leader election..."
    sleep 4
    NEW_LEADER_A=$(curl -sf "$ROUTER/cluster/shards" 2>/dev/null | \
        python3 -c "import sys,json; d=json.load(sys.stdin); l=[s['leader_addr'] for s in d['shards'] if s['id']=='A']; print(l[0] if l else '')" 2>/dev/null)
    info "Shard A new leader: ${NEW_LEADER_A:-NONE}"

    if [ -n "$NEW_LEADER_A" ] && [ "$NEW_LEADER_A" != "$LEADER_A" ]; then
        ok "Shard A elected new leader: $NEW_LEADER_A"
        STATUS=$(curl -sf -o /dev/null -w "%{http_code}" -X PUT "$ROUTER/kv/recovery-test" \
            -H "Content-Type: application/json" -d '{"value":"ok"}' 2>/dev/null)
        if [ "$STATUS" = "200" ]; then
            ok "Shard A accepting writes again after leader failover"
        else
            fail "Shard A not accepting writes after failover (HTTP $STATUS)"
        fi
    elif [ -n "$NEW_LEADER_A" ] && [ "$NEW_LEADER_A" = "$LEADER_A" ]; then
        fail "Shard A router still pointing at dead leader ($NEW_LEADER_A)"
    else
        fail "Shard A has no new leader after election wait"
    fi
fi

banner "SUMMARY"
echo -e "  ${GREEN}PASSED: $PASS${NC}"
echo -e "  ${RED}FAILED: $FAIL${NC}"
echo ""
if [ "$FAIL" -eq 0 ]; then
    echo -e "${GREEN}ALL Phase 2 exit criteria PASSED ✓${NC}"
else
    echo -e "${RED}$FAIL check(s) FAILED — see above ✗${NC}"
fi
