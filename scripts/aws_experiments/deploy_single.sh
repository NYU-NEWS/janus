#!/bin/sh
fab --set data_dir=.ec2_single deploy_all:regions=us-west-2,servers_per_region=37,instance_type=t2.small
