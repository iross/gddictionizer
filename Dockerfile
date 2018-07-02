# Pull base image.
FROM ubuntu:14.04
USER root

RUN locale-gen en_US.UTF-8
RUN update-locale LANG=en_US.UTF-8

# Install basic stuff
RUN \
  sed -i 's/# \(.*multiverse$\)/\1/g' /etc/apt/sources.list && \
  apt-get update && \
  apt-get -y upgrade && \
  apt-get install -y build-essential && \
  apt-get install -y software-properties-common && \
  apt-get install -y supervisor && \
  mkdir -p /var/log/supervisor && \
  mkdir -p /etc/supervisor/conf.d && \
  apt-get install -y byobu curl git htop man unzip vim wget && \
  rm -rf /var/lib/apt/lists/*

ADD supervisor.conf /etc/supervisor.conf

# ---- Elasticsearch ----
# Add ES user
RUN groupadd -g 1000 elasticsearch && useradd elasticsearch -u 1000 -g 1000

WORKDIR /usr/share/elasticsearch

# install java
RUN apt-get install -y --no-install-recommends software-properties-common && add-apt-repository -y ppa:webupd8team/java && \
    apt-get update && \
    (echo oracle-java8-installer shared/accepted-oracle-license-v1-1 select true | sudo /usr/bin/debconf-set-selections) && \
    apt-get install --no-install-recommends -y oracle-java8-installer && \
    rm -rf /var/cache/oracle-jdk8-installer && \
    echo "networkaddress.cache.ttl=60" >> /usr/lib/jvm/java-8-oracle/jre/lib/security/java.security && \
    apt-get clean && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME /usr/lib/jvm/java-8-oracle

# Define default command.
RUN \
 curl -O https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-5.6.10.zip && \
 unzip elasticsearch-5.6.10.zip

WORKDIR /usr/share/elasticsearch/elasticsearch-5.6.10/
RUN \
 chown -R elasticsearch:elasticsearch .

COPY elasticsearch.yml /usr/share/elasticsearch/elasticsearch-5.6.10/config/

ENV PATH=$PATH:/usr/share/elasticsearch/elasticsearch-5.6.10/bin
ADD elasticsearch.conf /etc/supervisor/conf.d/
EXPOSE 9200 9300 5601

USER root

RUN mkdir /input
RUN mkdir /output

ADD GddConfig.py /usr/bin
ADD DictionaryHelper.py /usr/bin
ADD ElasticsearchHelper.py /usr/bin
ADD db_conn.cfg /usr/bin
ADD settings.py /usr/bin
ADD wrapper.py /usr/bin
ADD requirements.txt /usr/bin

RUN \
 curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py && \
 python get-pip.py && \
 pip install -r /usr/bin/requirements.txt


CMD ["supervisord", "-c", "/etc/supervisor.conf"]

