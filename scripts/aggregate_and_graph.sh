prefix=${1?"error -- give prefix for experiment files"}
$janus/scripts/aggregate_run_output.py  --prefix $1 *yml
$janus/scripts/make_graphs '*csv' . $janus
