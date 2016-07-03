FROM java:8

RUN apt-get update && apt-get install postgresql-client -y && apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

RUN mkdir -p /toro_dist
WORKDIR /toro_dist
ENV TOROHOME /toro_dist

ADD docker_contents/setup.sh /toro_dist
ADD docker_contents/run.sh /toro_dist
ADD docker_contents/torodb.yml /toro_dist
EXPOSE 27018

ENV TOROPASS secret
RUN echo $TOROPASS > /root/.toropass
CMD /toro_dist/run.sh
