#python3 run_all.py -hh config/hosts-local.yml -s '1:4:1' -c '1:2:1' -r '3' \
#  -cc config/concurrent_1.yml \
#  -cc config/rw.yml -cc config/client_closed.yml \
#  -cc config/occ_paxos.yml -b rw -m occ:multi_paxos \
#  --allow-client-overlap testing

rm *.pdf
killall -9 deptran_server
./waf configure build -p -J -W
#./waf configure build -p -J
#./run.py -f config/9c3s3r18p.yml -f config/tpca_zipf_1.yml -f config/troad.yml -f config/concurrent_400.yml -d 30
./run.py -f config/9c3s3r18p.yml -f config/tpca_zipf_1.yml -f config/janus.yml -f config/concurrent_400.yml -d 30
#./run.py -f config/9c3s3r18p.yml -f config/tpca_uniform.yml -f config/troad.yml -f config/concurrent_400.yml -d 30
sleep 10
./scripts/pprof --pdf ./build/deptran_server process-h01.prof &> tmp01.pdf
./scripts/pprof --pdf ./build/deptran_server process-h09.prof &> tmp09.pdf
