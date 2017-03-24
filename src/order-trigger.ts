
import { BusinessEventContext, BusinessEventHandlerFunction, BusinessEventListener, rpcAsync, ProcessorFunction, AsyncServerFunction, CmdPacket, Permission, set_for_response, waiting, msgpack_decode_async as msgpack_decode, msgpack_encode_async as msgpack_encode } from "hive-service";
import * as bluebird from "bluebird";
import * as msgpack from "msgpack-lite";
import * as bunyan from "bunyan";
import { createClient, RedisClient} from "redis";
import { Socket, socket } from "nanomsg";

declare module "redis" {
  export interface RedisClient extends NodeJS.EventEmitter {
    hgetAsync(key: string, field: string): Promise<any>;
  }
  export interface Multi extends NodeJS.EventEmitter {
    execAsync(): Promise<any>;
  }
}

const log = bunyan.createLogger({
  name: "order-trigger",
  streams: [
    {
      level: "info",
      path: "/var/log/order-trigger-info.log",  // log ERROR and above to a file
      type: "rotating-file",
      period: "1d",   // daily rotation
      count: 7        // keep 7 back copies
    },
    {
      level: "error",
      path: "/var/log/order-trigger-error.log",  // log ERROR and above to a file
      type: "rotating-file",
      period: "1w",   // daily rotation
      count: 3        // keep 7 back copies
    }
  ]
});

export function run() {

  const cache: RedisClient = bluebird.promisifyAll(createClient(process.env["CACHE_PORT"], process.env["CACHE_HOST"])) as RedisClient;

  const vehicle_socket: Socket = socket("sub");
  vehicle_socket.connect(process.env["VEHICLE-TRIGGER"]);
  vehicle_socket.on("data", function (buf) {
    const obj = msgpack.decode(buf);
    log.info("obj" + JSON.stringify(obj));
    const vid = obj["vid"];
    const vehicle = obj["vehicle"];
    log.info(`Got vehicle ${vid} from trigger`);
    (async () => {
      try {
        const poid: string = await cache.hgetAsync("vid-poid", vid);
        const oid = poid.toString();
        log.info(`update order ${oid} with vehicle ${vid}`);
        const orderstr: string = await cache.hgetAsync("order-entities", oid);
        const order = await msgpack_decode(orderstr);
        order["vehicle"] = vehicle;
        const new_order = await msgpack_encode(order)
        await cache.hsetAsync("order-entities", oid, new_order);
        log.info(`update vehicle ${vid} of orders done`);
      } catch (e) {
        log.error(e);
      }
    })();
  });
  log.info(`order-trigger is running on ${process.env["VEHICLE-TRIGGER"]}`);
}

