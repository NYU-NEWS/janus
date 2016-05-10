tar -xzvf $1
cd archive && rm *csv
$janus/scripts/aggregate_run_output.py --rev $2 --prefix $3 *yml
$janus/scripts/make_graphs
