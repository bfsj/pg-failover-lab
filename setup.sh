#!/usr/bin/env bash
set -euo pipefail
mode=${1:-async}
case "$mode" in sync|async) ;; *) echo 'Expected sync or async' >&2; exit 2 ;; esac
kind create cluster --name pg-lab --wait 120s
k=(kubectl --context kind-pg-lab)
"${k[@]}" apply --server-side -f https://raw.githubusercontent.com/cloudnative-pg/cloudnative-pg/release-1.30/releases/cnpg-1.30.1.yaml
"${k[@]}" -n cnpg-system rollout status deployment/cnpg-controller-manager --timeout=300s
"${k[@]}" apply -f lab.yaml
if [[ "$mode" == sync ]]; then
  "${k[@]}" -n pg-lab patch cluster lab --type merge -p '{"spec":{"postgresql":{"synchronous":{"method":"any","number":1,"dataDurability":"required"}}}}'
fi
"${k[@]}" -n pg-lab wait cluster/lab --for=condition=Ready --timeout=600s
"${k[@]}" -n pg-lab wait pod/client --for=condition=Ready --timeout=180s
# Wait for both replicas to stream and the requested commit policy to take effect.
for _ in {1..90}; do
  p=$("${k[@]}" -n pg-lab get cluster lab -o jsonpath='{.status.currentPrimary}')
  states=$("${k[@]}" -n pg-lab exec "$p" -c postgres -- psql -Atqc "SELECT sync_state FROM pg_stat_replication WHERE state='streaming'" || true)
  if [[ $(wc -w <<< "$states") == 2 ]]; then
    if [[ "$mode" == sync && "$states" =~ (quorum|sync) && "$states" != *async* ]]; then exit 0; fi
    if [[ "$mode" == async && "$states" == $'async\nasync' ]]; then exit 0; fi
  fi
  sleep 2
done
echo 'Replication readiness timed out' >&2
exit 1
