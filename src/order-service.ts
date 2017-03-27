import { Service, Server, BusinessEventListener, Processor, Config } from "hive-service";
import { server } from "./order-server";
import { listener } from "./order-listener";
import { processor } from "./order-processor";
import { run as trigger_run } from "./order-trigger";
import * as bunyan from "bunyan";

const log = bunyan.createLogger({
  name: "order-service",
  streams: [
    {
      level: "info",
      path: "/var/log/order-service-info.log",  // log ERROR and above to a file
      type: "rotating-file",
      period: "1d",   // daily rotation
      count: 7        // keep 7 back copies
    },
    {
      level: "error",
      path: "/var/log/order-service-error.log",  // log ERROR and above to a file
      type: "rotating-file",
      period: "1w",   // daily rotation
      count: 3        // keep 7 back copies
    }
  ]
});

const loginfo = (...x) => log.info(...x);
const logerror = (...x) => log.error(...x);

const config: Config = {
  modname: "ORDER",
  serveraddr: process.env["ORDER"],
  queueaddr: "ipc:///tmp/order.ipc",
  cachehost: process.env["CACHE_HOST"],
  dbhost: process.env["DB_HOST"],
  dbuser: process.env["DB_USER"],
  dbport: process.env["DB_PORT"],
  database: process.env["DB_NAME"],
  dbpasswd: process.env["DB_PASSWORD"],
  queuehost: process.env["QUEUE_HOST"],
  queueport: process.env["QUEUE_PORT"],
  loginfo: loginfo,
  logerror: logerror
};

const svc: Service = new Service(config);
svc.registerServer(server);
svc.registerEventListener(listener);
svc.registerProcessor(processor);
svc.run();
trigger_run();
