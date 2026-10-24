#!/usr/bin/env bash
# Run a local cluster, one process per node, behind a load balancer. The
# load balancer serves on PORT (default 8080) and spreads requests over every
# healthy node; node nI serves HTTP on PORT+I and gossips on GOSSIP_PORT+I
# (default 7940+I).
#
#   scripts/cluster.sh start [N] [OPTIONS]  build and start N nodes (default 5) and the balancer
#   scripts/cluster.sh add          start one more node, joining the cluster
#   scripts/cluster.sh stop         stop every node and the balancer
#   scripts/cluster.sh status       show which processes are running
#   scripts/cluster.sh members      show the cluster as gossip sees it
#   scripts/cluster.sh kill ID      SIGKILL one node, e.g. n3 (simulates a crash)
#   scripts/cluster.sh revive ID    restart one node with the same data
#
# Options for start, each also settable in the environment:
#   --nodes N         nodes to start                      (default 5)
#   --n N             replicas of each key                (REPLICAS, default 3)
#   --r R             replicas a read waits for           (READS, default 2)
#   --w W             replicas a write waits for          (WRITES, default 2)
#   --transfer-mb MB  background transfer cap per node    (TRANSFER_MB, default 50; 0 = unlimited)
#   --anti-entropy D  how often replicas compare          (ANTI_ENTROPY, default 30s; 0 = off)
#   --sloppy          sloppy quorums: when a key's replicas are down, the
#                     next nodes on the ring stand in     (SLOPPY=true; default off)
#   --port P          the load balancer's port; node nI
#                     serves on P+I                       (PORT, default 8080)
#   --gossip-port G   node nI gossips on G+I              (GOSSIP_PORT, default 7940)
# They're saved in data/config, and every later command reuses them, so every
# node in the cluster agrees on N, R, W and sloppiness, and on where it is.
# start refuses to run if another program holds any port it needs.
#
# Then talk to it with bin/objctl, e.g.  echo hi | bin/objctl put greeting
# (with a --port other than 8080, first: export OBJSTORE_ADDR=127.0.0.1:P)
# Data and logs live in data/<id>/ and data/<id>.log; the balancer's in data/lb.log.
set -euo pipefail
cd "$(dirname "$0")/.."

DATA=data
BIN=bin/node

die() { echo "cluster: $*" >&2; exit 1; }

http_port() { echo $((PORT + ${1#n})); }
gossip_port() { echo $((GOSSIP_PORT + ${1#n})); }

# ids prints every node ever started, one per line.
ids() {
	[[ -f "$DATA/nodes" ]] || die "no cluster found; run '$0 start' first"
	cat "$DATA/nodes"
}

# seeds_except prints the gossip addresses of every other node. Joining
# needs only one of them to answer, so dead ones in the list are harmless.
seeds_except() { # id
	local out="" other
	for other in $(ids); do
		[[ "$other" == "$1" ]] || out+="127.0.0.1:$(gossip_port "$other"),"
	done
	echo "${out%,}"
}

start_node() { # id [join]
	local id=$1 join=${2:-}
	# The settings come from data/config; see load_config.
	local args=(-id "$id" -data "$DATA/$id"
		-listen "127.0.0.1:$(http_port "$id")" -gossip "127.0.0.1:$(gossip_port "$id")"
		-n "${REPLICAS:-3}" -r "${READS:-2}" -w "${WRITES:-2}" -transfer-mb "${TRANSFER_MB:-50}"
		-anti-entropy "${ANTI_ENTROPY:-30s}" -sloppy="${SLOPPY:-false}")
	[[ -n "$join" ]] && args+=(-join "$join")
	"$BIN" "${args[@]}" >>"$DATA/$id.log" 2>&1 &
	echo $! >"$DATA/$id.pid"
}

# load_config reads the settings the cluster was started with. A node added
# or revived later must agree with the rest on N, R and W, and every command
# must find the cluster on its ports; taking them from the environment of
# whoever runs the command would let them drift apart.
load_config() {
	[[ -f "$DATA/config" ]] && source "$DATA/config"
	PORT=${PORT:-8080} GOSSIP_PORT=${GOSSIP_PORT:-7940}
	return 0
}

# start_lb runs the load balancer in front of every node in the cluster.
start_lb() {
	local backends="" id
	for id in $(ids); do backends+="127.0.0.1:$(http_port "$id"),"; done
	bin/objctl lb -listen "127.0.0.1:$PORT" -backends "${backends%,}" >>"$DATA/lb.log" 2>&1 &
	echo $! >"$DATA/lb.pid"
	for _ in $(seq 1 50); do
		running lb || break
		# Our nodes answer "ok <id>". Anything else on the port is some other
		# program's, and our balancer couldn't bind it.
		if [[ $(curl -sf "http://127.0.0.1:$PORT/healthz") == ok* ]] && running lb; then
			echo "  load balancer up: :$PORT, in front of $(ids | wc -l) nodes"
			return 0
		fi
		sleep 0.1
	done
	echo "  load balancer failed to start; last log lines:" >&2
	tail -5 "$DATA/lb.log" >&2
	return 1
}

stop_lb() {
	if running lb; then
		kill -INT "$(cat "$DATA/lb.pid")"
		for _ in $(seq 1 50); do running lb || break; sleep 0.1; done
	fi
	rm -f "$DATA/lb.pid"
}

wait_healthy() { # id
	local id=$1 port
	port=$(http_port "$id")
	for _ in $(seq 1 50); do
		# Check the process we started is alive first. Otherwise a node left
		# over from an earlier run can answer /healthz on this port, while
		# ours died at once with "address already in use".
		if ! running "$id"; then
			break
		fi
		if curl -sf "http://127.0.0.1:$port/healthz" >/dev/null; then
			echo "  $id up: http :$port, gossip :$(gossip_port "$id")"
			return 0
		fi
		sleep 0.1
	done
	echo "  $id failed to start; last log lines:" >&2
	tail -5 "$DATA/$id.log" >&2
	return 1
}

# wait_converged waits until every running node sees every node in the
# cluster. Membership spreads by gossip, so for a moment after a start nodes
# disagree about who exists, route by different rings, and a read through
# one can miss a write through another. "Started" should mean "agreed".
wait_converged() {
	local want id port n
	want=$(ids | wc -l)
	for _ in $(seq 1 100); do
		n=0
		for id in $(ids); do
			running "$id" || continue
			port=$(http_port "$id")
			[[ $(curl -s "http://127.0.0.1:$port/cluster/members" | grep -o '"id"' | wc -l) -eq $want ]] || continue 2
			n=$((n + 1))
		done
		echo "  converged: every running node sees all $want"
		return 0
	done
	echo "  not converged after 10s; check the logs" >&2
	return 1
}

# port_free reports whether nothing is listening on 127.0.0.1:PORT.
port_free() { # port
	! (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null
}

# check_ports refuses to go on if another program holds a port the cluster
# needs: the balancer's, and each node's HTTP and gossip ports. Otherwise
# that program would answer in our place, and its errors look like ours.
check_ports() { # id...
	local id p busy=()
	port_free "$PORT" || busy+=("$PORT (load balancer)")
	for id in "$@"; do
		for p in "$(http_port "$id")" "$(gossip_port "$id")"; do
			port_free "$p" || busy+=("$p ($id)")
		done
	done
	((${#busy[@]} == 0)) && return 0
	echo "cluster: these ports are in use by another program: ${busy[*]}" >&2
	echo "cluster: see what holds one with: sudo lsof -i :${busy[0]%% *}. Stop it, or start on other ports," >&2
	echo "cluster: e.g. $0 start --port 9080 --gossip-port 9940" >&2
	exit 1
}

# running reports whether node ID's process exists and is not a zombie.
# `kill -0` alone is not enough: a killed process stays a zombie until its
# parent collects its exit status, and kill -0 reports zombies as alive.
running() { # id
	local pid state
	[[ -f "$DATA/$1.pid" ]] || return 1
	pid=$(cat "$DATA/$1.pid")
	# Field 3 of /proc/PID/stat is the state; strip "PID (comm) " first,
	# since comm may contain spaces.
	state=$(sed 's/^.*) //' "/proc/$pid/stat" 2>/dev/null | cut -d' ' -f1) || return 1
	[[ -n "$state" && "$state" != Z ]]
}

# start takes its settings from its options; every other command from the
# cluster it acts on.
[[ ${1:-} == start ]] || load_config

case "${1:-}" in
start)
	shift
	n=5
	REPLICAS=${REPLICAS:-3} READS=${READS:-2} WRITES=${WRITES:-2}
	TRANSFER_MB=${TRANSFER_MB:-50} ANTI_ENTROPY=${ANTI_ENTROPY:-30s} SLOPPY=${SLOPPY:-false}
	PORT=${PORT:-8080} GOSSIP_PORT=${GOSSIP_PORT:-7940}
	while [[ $# -gt 0 ]]; do
		if [[ $1 =~ ^[0-9]+$ ]]; then n=$1; shift; continue; fi
		if [[ $1 == --sloppy ]]; then SLOPPY=true; shift; continue; fi
		[[ $# -ge 2 ]] || die "$1 needs a value"
		case $1 in
		--nodes) n=$2 ;;
		--n) REPLICAS=$2 ;;
		--r) READS=$2 ;;
		--w) WRITES=$2 ;;
		--transfer-mb) TRANSFER_MB=$2 ;;
		--anti-entropy) ANTI_ENTROPY=$2 ;;
		--port) PORT=$2 ;;
		--gossip-port) GOSSIP_PORT=$2 ;;
		*) die "unknown option $1; run '$0' alone for usage" ;;
		esac
		shift 2
	done
	for v in "$n" "$REPLICAS" "$READS" "$WRITES"; do
		[[ $v =~ ^[1-9][0-9]*$ ]] || die "nodes, N, R and W must be positive whole numbers; got $v"
	done
	((READS <= REPLICAS && WRITES <= REPLICAS)) || die "R ($READS) and W ($WRITES) can't exceed N ($REPLICAS)"
	((n >= REPLICAS)) || die "$n nodes can't hold $REPLICAS replicas of each key"
	[[ $SLOPPY == true || $SLOPPY == false ]] || die "SLOPPY must be true or false; got $SLOPPY"
	for v in "$PORT" "$GOSSIP_PORT"; do
		[[ $v =~ ^[1-9][0-9]*$ ]] && ((v + n + 10 <= 65535)) || die "ports must be whole numbers, low enough to leave room for every node; got $v"
	done
	# Leave room for nodes added later: ten of them.
	((GOSSIP_PORT > PORT + n + 10 || PORT > GOSSIP_PORT + n + 10)) ||
		die "--port $PORT and --gossip-port $GOSSIP_PORT are too close: nodes' ports would overlap"
	((READS + WRITES > REPLICAS)) || echo "note: R + W <= N, so a read may miss an acknowledged write" >&2
	if [[ -f "$DATA/nodes" ]]; then
		for id in $(ids); do
			running "$id" && die "$id is already running; run '$0 stop' first"
		done
	fi
	running lb && die "the load balancer is already running; run '$0 stop' first"
	# shellcheck disable=SC2046 # one word per node ID
	check_ports $(seq -f 'n%g' 1 "$n")
	mkdir -p bin "$DATA"
	go build -o "$BIN" ./cmd/node
	go build -o bin/objctl ./cmd/objctl
	printf 'REPLICAS=%s\nREADS=%s\nWRITES=%s\nTRANSFER_MB=%s\nANTI_ENTROPY=%s\nSLOPPY=%s\nPORT=%s\nGOSSIP_PORT=%s\n' \
		"$REPLICAS" "$READS" "$WRITES" "$TRANSFER_MB" "$ANTI_ENTROPY" "$SLOPPY" "$PORT" "$GOSSIP_PORT" >"$DATA/config"
	seq -f 'n%g' 1 "$n" >"$DATA/nodes"
	sloppiness=""
	[[ $SLOPPY == true ]] && sloppiness=", sloppy quorums"
	echo "starting $n nodes: N=$REPLICAS R=$READS W=$WRITES$sloppiness"
	# n1 founds the cluster; everyone else joins through it.
	start_node n1
	wait_healthy n1
	for id in $(ids | tail -n +2); do start_node "$id" "127.0.0.1:$(gossip_port n1)"; done
	for id in $(ids | tail -n +2); do wait_healthy "$id"; done
	wait_converged
	start_lb
	if ((PORT != 8080)); then
		echo "  objctl talks to :8080 unless told otherwise: export OBJSTORE_ADDR=127.0.0.1:$PORT"
	fi
	;;
add)
	load_config
	id="n$(($(ids | wc -l) + 1))"
	seeds=$(seeds_except "$id")
	echo "$id" >>"$DATA/nodes"
	start_node "$id" "$seeds"
	wait_healthy "$id"
	wait_converged
	stop_lb # restarted with the new node behind it
	start_lb
	;;
stop)
	stop_lb
	for id in $(ids); do
		if running "$id"; then
			kill -INT "$(cat "$DATA/$id.pid")"
		fi
	done
	for id in $(ids); do
		for _ in $(seq 1 100); do running "$id" || break; sleep 0.1; done
		rm -f "$DATA/$id.pid"
	done
	echo "stopped"
	;;
status)
	if running lb; then echo "  lb running  :$PORT"; else echo "  lb STOPPED  :$PORT"; fi
	for id in $(ids); do
		if running "$id"; then echo "  $id running  :$(http_port "$id")"; else echo "  $id STOPPED  :$(http_port "$id")"; fi
	done
	;;
members)
	for id in $(ids); do
		if running "$id"; then
			echo "as seen by $id:"
			curl -s "http://127.0.0.1:$(http_port "$id")/cluster/members" | grep -o '{[^}]*}' | sed 's/^/  /'
			exit 0
		fi
	done
	die "no node is running"
	;;
kill)
	id=${2:?usage: $0 kill ID}
	running "$id" || die "$id is not running"
	kill -KILL "$(cat "$DATA/$id.pid")"
	for _ in $(seq 1 50); do running "$id" || break; sleep 0.1; done
	running "$id" && die "$id did not die"
	echo "killed $id (SIGKILL: no graceful shutdown, no goodbye to the cluster)"
	;;
revive)
	load_config
	id=${2:?usage: $0 revive ID}
	running "$id" && die "$id is already running"
	start_node "$id" "$(seeds_except "$id")"
	wait_healthy "$id"
	wait_converged
	;;
*)
	sed -n '2,/^set -euo/p' "$0" | grep '^#' | sed 's/^# \{0,1\}//'
	exit 2
	;;
esac
