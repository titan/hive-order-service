import { Service, Server, Processor, Config } from "hive-service";
import { server } from "./order-server";

const config: Config = {
  serveraddr: process.env["ORDER"],
  queueaddr: "ipc:///tmp/order.ipc",
  cachehost: process.env["CACHE_HOST"],
  dbhost: process.env["DB_HOST"],
  dbuser: process.env["DB_USER"],
  dbport: process.env["DB_PORT"],
  database: process.env["DB_NAME"],
  dbpasswd: process.env["DB_PASSWORD"],
};

const svc: Service = new Service(config);

svc.registerServer(server);

svc.run();
