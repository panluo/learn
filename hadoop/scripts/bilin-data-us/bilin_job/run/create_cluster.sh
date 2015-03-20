#!/bin/bash
#
#
#


REQ_CLUSTER_ID=""

function create_req_cluster(){
	REQ_CLUSTER_ID=$(aws emr create-cluster --name testCluster --ami-version 3.3.1 \
	--instance-groups Name=Masternode,InstanceGroupType=MASTER,InstanceType=m1.large,InstanceCount=1 \
	Name=Corenode,InstanceGroupType=CORE,InstanceType=c3.xlarge,InstanceCount=2 \
	Name=Tasknode,InstanceGroupType=TASK,InstanceType=c3.xlarge,InstanceCount=1 \
	--no-auto-terminate \
	--ec2-attributes KeyName=bilin2014_east \
	--use-default-roles \
	--log-uri s3://bilin-data/hadoop/emrlog \
	--applications Name=Hive Name=Ganglia \
	--enable-debugging \
	--bootstrap-actions Path=s3://elasticmapreduce/bootstrap-actions/configure-hadoop,Name=Hadoop_configure,Args=["-m","mapreduce.job.reduce.slowstart.completedmaps=0.80","-m","mapreduce.map.output.compress=true"] \
	Path=s3://bilin-data/hadoop/bilin_emr/bootstrap/bilin_emr_bs.sh,Name=Bilin_job_sync) || return 1
#	--bootstrap-action Path=s3://elasticmapreduce/bootstrap-actions/configure-hadoop,Args=["-y","yarn.scheduler.maximum-allocation-mb=2048","-y","yarn.scheduler.maximum-allocation-vcores=4"]) || return 1
	
}
create_req_cluster
echo $REQ_CLUSTER_ID
