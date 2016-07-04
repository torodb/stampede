FROM java:8

RUN apt-get update && apt-get install postgresql-client -y && apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

RUN mkdir -p /toro_dist
WORKDIR /toro_dist
ENV TOROHOME /toro_dist

ADD .docker/setup.sh /toro_dist
ADD .docker/run.sh /toro_dist
ADD .docker/torodb.yml /toro_dist
EXPOSE 27018

ENV TOROPASS secret
RUN echo postgres:5432:torod:torodb:$TOROPASS > /root/.toropass
CMD /toro_dist/run.sh
