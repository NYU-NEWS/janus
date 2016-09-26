tar czvf csv.tgz *csv
: ${janus:=~/work_area/nyu/research/janus}
echo "janus dir is: $janus"
$janus/scripts/aggregate_run_output.py --prefix $1 *yml
$janus/scripts/make_graphs '*csv' . $janus
