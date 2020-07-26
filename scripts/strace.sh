./waf configure build -J
strace -f -c -w ./build/deptran_server -f config/9c3s3r1p.yml -f config/concurrent_400.yml -f config/troad.yml -f config/tpca_fixed.yml -d 60 -P localhost &> tmp1.txt
./waf configure build -J -W 
strace -f -c -w ./build/deptran_server -f config/9c3s3r1p.yml -f config/concurrent_400.yml -f config/troad.yml -f config/tpca_fixed.yml -d 60 -P localhost &> tmp2.txt
#strace -f -c -w ./build/deptran_server -f config/3c1s3r1p.yml -f config/concurrent_100.yml -f config/tpl_ww_paxos.yml -f config/tpca_fixed.yml -d 60 -P localhost &> tmp2.txt
