input_file=${1:-input.tgz}
output_dir=${2:-$PWD}
work_dir=${3:-~/.tmp}
current_dir=$PWD
mkdir -p $work_dir
mkdir -p $output_dir
cp $input_file $work_dir/target.tgz
cd $work_dir
tar --wildcards -xzvf target.tgz archive/*.csv
cd $current_dir
mv $work_dir/archive/*.csv $output_dir/

rm $work_dir/target.tgz
rm -rf $work_dir/archive

