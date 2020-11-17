cmd="./build/deptran_server -f config/64c8s1p.yml -f config/tpcc.yml -f config/occ.yml -f config/client_open.yml -f config/concurrent_10.yml"
gdb --args $cmd
