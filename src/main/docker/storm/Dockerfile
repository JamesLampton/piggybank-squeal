# Storm rig for testing.
FROM centos:6
MAINTAINER James Lampton <jlampton@gmail.com>

# Install EPEL
RUN yum install -y http://download.fedoraproject.org/pub/epel/6/x86_64/epel-release-6-8.noarch.rpm
# Update and install java
RUN yum update -y
RUN yum install -y java-1.8.0-openjdk tar supervisor

# Download and install Storm
ENV STORM_VERSION 0.9.3

# Be a good netizen and don't pull dist -- apache folks: make your mirror auto-redirect...
#RUN mkdir -p /opt/downloads && cd /opt/downloads && curl -SsfLO "http://www.apache.org/dist/storm/apache-storm-$STORM_VERSION/apache-storm-$STORM_VERSION.tar.gz"
ADD apache-storm-$STORM_VERSION.tar.gz /opt/

RUN mv /opt/apache-storm-$STORM_VERSION /opt/storm

# Configure zookeeper
RUN echo 'storm.zookeeper.servers: ["zookeeper"]' >> /opt/storm/conf/storm.yaml

# Setup supervisord
ADD supervisor.d/*.ini /etc/supervisor.d/
RUN cat /etc/supervisor.d/*.ini >> /etc/supervisord.conf

# Expose ui
EXPOSE 8080
# Expose logviewer
EXPOSE 8000
# Expose nimbus 
EXPOSE 6627

ENTRYPOINT ["/usr/bin/supervisord", "-n"]
