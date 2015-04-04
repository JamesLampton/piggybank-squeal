# Zookeeper server for testing.
FROM centos:6
MAINTAINER James Lampton <jlampton@gmail.com>

# Update and install Java
RUN yum update -y
RUN yum install -y java-1.8.0-openjdk tar

# Download and install Zookeeper
ENV ZOOKEEPER_VERSION 3.4.6

#RUN mkdir -p /opt/downloads && cd /opt/downloads && curl -SsfLO "http://www.apache.org/dist/zookeeper/zookeeper-$ZOOKEEPER_VERSION/zookeeper-$ZOOKEEPER_VERSION.tar.gz"
#RUN cd /opt && tar xvfz /opt/downloads/zookeeper-$ZOOKEEPER_VERSION.tar.gz

ADD zookeeper-$ZOOKEEPER_VERSION.tar.gz /opt/
RUN mv /opt/zookeeper-$ZOOKEEPER_VERSION /opt/zookeeper

RUN sed -e 's,^#auto,auto,g' /opt/zookeeper/conf/zoo_sample.cfg > /opt/zookeeper/conf/zoo.cfg

EXPOSE 2181

ENTRYPOINT ["/opt/zookeeper/bin/zkServer.sh", "start-foreground"]
