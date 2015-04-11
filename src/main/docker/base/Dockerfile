# Base image for piggybank-squeal stuff
FROM centos:6
MAINTAINER James Lampton <jlampton@gmail.com>

# Install EPEL
RUN yum install -y http://download.fedoraproject.org/pub/epel/6/x86_64/epel-release-6-8.noarch.rpm
# Update and install Java
RUN yum update -y
RUN yum install -y java-1.8.0-openjdk which

# Install Zookeeper
ENV ZOOKEEPER_VERSION 3.4.6
ADD zookeeper-$ZOOKEEPER_VERSION.tar.gz /opt/
RUN mv /opt/zookeeper-$ZOOKEEPER_VERSION /opt/zookeeper

# Install Storm
ENV STORM_VERSION 0.9.3
ADD apache-storm-$STORM_VERSION.tar.gz /opt/
RUN mv /opt/apache-storm-$STORM_VERSION /opt/storm

# Install Hadoop
ENV HADOOP_VERSION 2.6.0
ADD hadoop-$HADOOP_VERSION.tar.gz /opt/
RUN mv /opt/hadoop-$HADOOP_VERSION /opt/hadoop

RUN ln -s $(dirname $(dirname $(readlink -f $(which java)))) /opt/java_home
ENV JAVA_HOME /opt/java_home
ENV HADOOP_CONF_DIR /opt/hadoop/etc/hadoop

# Install Pig
ENV PIG_VERSION 0.13.0
ADD pig-$PIG_VERSION.tar.gz /opt/
RUN mv /opt/pig-$PIG_VERSION /opt/pig
