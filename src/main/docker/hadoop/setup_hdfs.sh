#!/usr/bin/env bash

# Setup directories and permissions in hdfs.
/opt/hadoop/bin/hdfs dfs -mkdir /user /tmp && /opt/hadoop/bin/hdfs dfs -chmod 777 /user /tmp

# Hold indefinately if we're successful.
/opt/hadoop/bin/hdfs dfs -stat /tmp && head 
