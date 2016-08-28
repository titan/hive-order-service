FROM hive/node-base:6.3.1

WORKDIR /code

ADD dist .
ADD node_modules ./node_modules
ADD script/deployment.json .

CMD [ "forever", "deployment.json" ]
