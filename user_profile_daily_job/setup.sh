#!/bin/bash

#
# Yichao
# 2016/2/15

/root/spark-ec2/copy-dir /usr/local/lib64/python2.7/site-packages
/root/spark-ec2/copy-dir /usr/local/lib/python2.7/site-packages

export PYSPARK_PYTHON=`which python27`
#
spark_url=spark://`cat /root/spark-ec2/masters`:7077
mongo_hadoop_jar_path=/usr/local/lib/mongo-hadoop/spark/build/libs/`ls /usr/local/lib/mongo-hadoop/spark/build/libs/`
mongo_hadoop_package=org.mongodb:mongodb-driver:3.2.1

IPYTHON=1 ./spark/bin/pyspark --driver-memory 13G --executor-memory 13G --jars $mongo_hadoop_jar_path --packages $mongo_hadoop_package  --driver-class-path $mongo_hadoop_jar_path --py-files /root/RED-Ambergris/python/protobuf-3.0.0b2-py2.7.egg,/root/RED-Ambergris/python/homefeed_recom_ls_pb2.py


#######################
cd ~
/root/spark-ec2/copy-dir .aws
/root/spark-ec2/copy-dir homefeed_result
/root/spark-ec2/copy-dir x.sh  # with parameter 1~49


yum install -y aws-cli;
aws s3 cp /root/homefeed_result/test_date/${part_name} s3://xhs.homefeed.bucket/test_date/${part_name}
echo "Successfully upload to S3"






########################



## Get time
if [ $# -lt 1 ]; then
    echo "$INFO_PREFIX you input nothing, as default I'll will process yesterday's data."
    pdate=`date -d 'yesterday' +%Y/%m/%d`
    DTM=`date -d 'yesterday' +%Y%m%d`
    DAY=`date -d 'yesterday' +%d`
    MONTH=`date -d 'yesterday' +%m`
    YEAR=`date -d 'yesterday' +%Y`
else
    date=$1 ## like: yyyyMMdd yyyy-MM-dd
    pdate=`date -d $date +%Y/%m/%d`
    ts=`date -d $date +%s`
    DTM=`date -d @$ts +%Y%m%d`
    DAY=`date -d @$ts +%d`
    MONTH=`date -d @$ts +%m`
    YEAR=`date -d @$ts +%Y`
fi

nohup ./spark/bin/spark-submit --master $spark_url --driver-memory 13G --executor-memory 13G --jars $mongo_hadoop_jar_path --packages $mongo_hadoop_package  --driver-class-path $mongo_hadoop_jar_path spark_merge_user_profile.py $pdate &
nohup ./spark/bin/spark-submit --master $spark_url --driver-memory 13G --executor-memory 13G --jars $mongo_hadoop_jar_path --packages $mongo_hadoop_package  --driver-class-path $mongo_hadoop_jar_path spark_merge_user_profile.py $pdate &


##########################

cd ~
rm spark_tag_goods_by_title.py user_profile_reset_longterm.sh spark_user_profile_longterm_daily.py spark_merge_user_profile.py
rm /usr/local/lib64/python2.7/site-packages/*user_action*
rm /usr/local/lib64/python2.7/site-packages/*user_profile*
s3cmd get s3://xhs.redshift.tools/spark_pkg/user_profile_helper_functions.tar.gz /usr/local/lib64/python2.7/site-packages/
s3cmd get s3://xhs.redshift.tools/spark_pkg/spark_user_profile_longterm_daily.py ./
s3cmd get s3://xhs.redshift.tools/spark_pkg/spark_tag_goods_by_title.py ./
s3cmd get s3://xhs.redshift.tools/spark_pkg/spark_merge_user_profile.py ./
s3cmd get s3://xhs.redshift.tools/spark_pkg/user_profile_reset_longterm.sh ./
cd /usr/local/lib64/python2.7/site-packages/
tar -zxvf /usr/local/lib64/python2.7/site-packages/user_profile_helper_functions.tar.gz
~/spark-ec2/copy-dir /usr/local/lib64/python2.7/site-packages
cd ~
