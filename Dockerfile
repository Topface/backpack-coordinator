FROM debian:jessie

ARG GIT_COMMIT=unkown
LABEL git-commit=$GIT_COMMIT

RUN apt-get update && apt-get install -y nodejs nodejs-legacy npm && \
    cp /usr/share/zoneinfo/Europe/Moscow /etc/localtime && \
    echo Europe/Moscow > /etc/timezone && \
    apt-get autoremove -y && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

COPY . /opt/backpack-coordinator

RUN cd /opt/backpack-coordinator && npm install

EXPOSE 13001

WORKDIR /opt/backpack-coordinator/bin

#nodejs /opt/backpack-coordinator/node_modules/.bin/backpack-coordinator 127.0.0.1:2181 /backpack 0.0.0.0 13001
