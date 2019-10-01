(Comment out the server kills in run.py if profiling distributed runs)

Compile with google perf tools.
```
./waf configure -p build
```

Run
```
./build/deptran_server -f config/3c3s3r1p.yml -f config/brq.yml -f config/tpca.yml -P localhost -d 60
```

The profiling result will be stored in a file named process-{process_name}.prof

Generate pdf
```
./scripts/pprof --pdf ./build/deptran_server process-localhost.prof > tmp.pdf
```