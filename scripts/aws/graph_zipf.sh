tar czvf csv.tgz *csv
: ${janus:=~/git/janus}
echo "janus dir is: $janus"
pre="${1:-unknown_prefix_}"
python3 $janus/scripts/aggregate_run_output.py --prefix $pre *yml
$janus/scripts/make_graphs '*csv' . $janus
