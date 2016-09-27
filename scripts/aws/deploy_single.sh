#!/bin/sh
data_dir=.ec2_single
$janus/scripts/aws/build_aws.sh us-west-2 37 m4.large $data_dir
