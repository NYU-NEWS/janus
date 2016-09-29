#!/bin/sh
data_dir=.ec2_multi

regions=us-west-2:eu-west-1:ap-northeast-2
servers=13:12:12

$janus/scripts/aws/build_aws.sh "$regions" "$servers" m4.large "$data_dir"

