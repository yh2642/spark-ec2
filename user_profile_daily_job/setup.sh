#!/bin/bash

#
# Yichao
# 2016/2/15

cd ~

yum install -y python-pip

pip install s3cmd

s3cmd get s3://xhs.redshift.tools/spark_pkg/user_profile_helper_functions.tar.gz /usr/local/lib64/python2.7/site-packages/
s3cmd get s3://xhs.redshift.tools/spark_pkg/spark_user_profile_longterm_daily.py ./
s3cmd get s3://xhs.redshift.tools/spark_pkg/spark_tag_goods_by_title.py ./
s3cmd get s3://xhs.redshift.tools/spark_pkg/user_profile_reset_longterm.sh ./
s3cmd get s3://xhs.redshift.tools/spark_pkg/spark_merge_user_profile.py ./


cd /usr/local/lib64/python2.7/site-packages/
tar -zxvf /usr/local/lib64/python2.7/site-packages/user_profile_helper_functions.tar.gz

wget https://bootstrap.pypa.io/get-pip.py

python27 get-pip.py

yum install -y python27-devel

pip2.7 install ujson
pip2.7 install pymongo
pip2.7 install pytz
pip2.7 install requests
pip2.7 install ipython[all]
pip2.7 install gevent
pip2.7 install boto
pip2.7 install numpy
pip2.7 install bson

cd ~
git clone https://github.com/mongodb/mongo-hadoop.git
cd mongo-hadoop/spark/src/main/python
python27 setup.py install
cd ~/mongo-hadoop/
./gradlew jar
cd ~
cp /root/mongo-hadoop/spark/src/main/python/pymongo_spark.py /usr/local/lib64/python2.7/site-packages/


#./spark/bin/pyspark  --py-files /root/mongo-hadoop/spark/src/main/python/pymongo_spark.py --jars /root/mongo-hadoop/spark/build/libs/mongo-hadoop-spark-1.5.0-SNAPSHOT.jar --driver-memory=13G --executor-memory=13G

#./spark/bin/pyspark --jars RedshiftJDBC41-1.1.10.1010.jar --packages com.databricks:spark-redshift_2.10:0.6.0

/root/spark-ec2/copy-dir /usr/local/lib64/python2.7/site-packages
/root/spark-ec2/copy-dir /usr/local/lib/python2.7/site-packages


export PYSPARK_PYTHON=`which python27`

spark_url=spark://`cat /root/spark-ec2/masters`:7077
mongo_hadoop_jar_path=/root/mongo-hadoop/spark/build/libs/`ls /root/mongo-hadoop/spark/build/libs/`
mongo_hadoop_package=org.mongodb:mongodb-driver:3.2.1
#./spark/bin/spark-submit spark_tag_goods_by_title.py $pdate --master $spark_url \
# --driver-memory 13G --executor-memory 13G


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


#echo "./spark/bin/spark-submit --driver-memory 13G --executor-memory 13G --jars $mongo_hadoop_jar_path spark_user_profile_longterm_daily.py $pdate"
./spark/bin/spark-submit --driver-memory 13G --executor-memory 13G --jars $mongo_hadoop_jar_path spark_user_profile_longterm_daily.py $pdate
#echo "./spark/bin/spark-submit --driver-memory 13G --executor-memory 13G --jars $mongo_hadoop_jar_path --packages $mongo_hadoop_package spark_merge_user_profile.py $pdate"
./spark/bin/spark-submit --driver-memory 13G --executor-memory 13G --jars $mongo_hadoop_jar_path --packages $mongo_hadoop_package spark_merge_user_profile.py $pdate
