# Base image for piggybank-squeal stuff
FROM centos:6
MAINTAINER James Lampton <jlampton@gmail.com>

# Install EPEL
RUN yum install -y http://download.fedoraproject.org/pub/epel/6/x86_64/epel-release-6-8.noarch.rpm
# Update and install Java
RUN yum update -y
RUN yum install -y java-1.8.0-openjdk

# Download and install Zookeeper
ENV ZOOKEEPER_VERSION 3.4.6
ADD zookeeper-$ZOOKEEPER_VERSION.tar.gz /opt/
RUN mv /opt/zookeeper-$ZOOKEEPER_VERSION /opt/zookeeper

# Download and install Storm
ENV STORM_VERSION 0.9.3
ADD apache-storm-$STORM_VERSION.tar.gz /opt/
RUN mv /opt/apache-storm-$STORM_VERSION /opt/storm

