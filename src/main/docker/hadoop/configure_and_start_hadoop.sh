#!/usr/bin/env bash

cat - > /opt/hadoop/etc/hadoop/core-site.xml <<EOM
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://$HOSTNAME:9000</value>
    </property>
</configuration>
EOM

cat - > /opt/hadoop/etc/hadoop/hdfs-site.xml <<EOM
<configuration>
    <property>
        <name>dfs.replication</name>
        <value>1</value>
    </property>
</configuration>
EOM

cat - > /opt/hadoop/etc/hadoop/mapred-site.xml <<EOM
<configuration>
    <property>
        <name>mapreduce.framework.name</name>
        <value>yarn</value>
    </property>
</configuration>
EOM

cat - > /opt/hadoop/etc/hadoop/yarn-site.xml <<EOM
<configuration>
    <property>
        <name>yarn.nodemanager.aux-services</name>
        <value>mapreduce_shuffle</value>
    </property>
</configuration>
EOM

test -d /tmp/hadoop-root || su hdfs -c '/opt/hadoop/bin/hdfs namenode -format'
echo 'export PATH=$PATH:/opt/hadoop/bin' >> /root/.bashrc

/usr/bin/supervisord -n
