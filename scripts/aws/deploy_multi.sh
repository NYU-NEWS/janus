#!/bin/sh
fab --set data_dir=.ec2_multi deploy_all:regions=us-west-2:eu-west-1:ap-northeast-2,servers_per_region=13:12:12,instance_type=t2.small
