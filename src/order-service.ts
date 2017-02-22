import { Service, Server, BusinessEventListener, Processor, Config } from "hive-service";
import { server } from "./order-server";
import { processor } from "./order-processor";
import { listener } from "./order-listener";
import { run as trigger_run } from "./order-trigger";

const config: Config = {
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
};

const svc: Service = new Service(config);
svc.registerServer(server);
svc.registerProcessor(processor);
svc.registerEventListener(listener);
svc.run();
trigger_run();
