regions=${1-us-west2} # us-west-2, us-east-1
region_servers=${2-19}
instance_type=${3-m4.large}
data_dir=${4-.ec2_data}

fab --set data_dir="$data_dir" deploy_all:regions=${regions},servers_per_region=${region_servers},instance_type=${instance_type}
sleep 30 
fab --set data_dir="$data_dir" ec2.set_instance_roles deploy_continue

