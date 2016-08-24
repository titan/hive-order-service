FROM hive/node-openrc-base:6.3.1
#FROM mhart/alpine-node:latest

WORKDIR /code

ADD dist/server.js .
ADD dist/processor.js .
ADD node_modules ./node_modules

ADD script/server /etc/init.d
ADD script/processor /etc/init.d

RUN rc-update add server default && rc-update add processor default

EXPOSE 4040
