python3 run_all.py -hh config/hosts-local.yml -s '1:4:1' -c '1:2:1' -r '3' \
  -cc config/concurrent_1.yml \
  -cc config/rw.yml -cc config/client_closed.yml \
  -cc config/occ_paxos.yml -b rw -m occ:multi_paxos \
  --allow-client-overlap testing

