#!/usr/bin/env bash
# Run one benchmark case several times, each on a fresh cluster.
#
#   scripts/bench.sh LABEL "CLUSTER OPTIONS" "OBJBENCH OPTIONS" [REPEATS]
#
# e.g. scripts/bench.sh "R=2 W=2" "--n 3 --r 2 --w 2" "-clients 16 -reads 0.5" 3
#
# Each run starts a cluster with scripts/cluster.sh start CLUSTER OPTIONS,
# loads it with bin/objbench OBJBENCH OPTIONS (through the balancer unless
# the options say -addr), and stops it. Results are appended, one JSON line
# per run, to $RESULTS (default bench-results.jsonl); summarise them with
#   bin/objbench -report bench-results.jsonl
set -euo pipefail
cd "$(dirname "$0")/.."
[[ $# -ge 3 ]] || { sed -n '2,/^set -euo/p' "$0" | grep '^#' | sed 's/^# \{0,1\}//'; exit 2; }
label=$1 cluster=$2 load=$3 repeats=${4:-3}
results=${RESULTS:-bench-results.jsonl}
# Every run starts by deleting data/. A cluster running on it would lose its
# data, and its nodes would keep the ports the new cluster needs.
status=$( [[ -f data/nodes ]] && scripts/cluster.sh status 2>/dev/null || true)
if [[ $status == *" running "* ]]; then
	echo "bench: a cluster is running on data/, which each run deletes; stop it first: scripts/cluster.sh stop" >&2
	exit 1
fi
go build -o bin/objbench ./cmd/objbench
trap 'scripts/cluster.sh stop >/dev/null 2>&1 || true' EXIT
for i in $(seq 1 "$repeats"); do
	rm -rf data
	# shellcheck disable=SC2086 # the options are meant to split into words
	scripts/cluster.sh start $cluster >/dev/null
	# shellcheck disable=SC2086
	bin/objbench $load -label "$label" -json "$results" | sed -n 2p | sed "s/^/$label, run $i:/"
	scripts/cluster.sh stop >/dev/null
done
