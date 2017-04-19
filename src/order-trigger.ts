
import { BusinessEventContext, BusinessEventHandlerFunction, BusinessEventListener, rpcAsync, ProcessorFunction, AsyncServerFunction, CmdPacket, Permission, set_for_response, waiting, msgpack_decode_async, msgpack_encode_async } from "hive-service";
import * as bluebird from "bluebird";
import * as msgpack from "msgpack-lite";
import * as bunyan from "bunyan";
import { createClient, RedisClient } from "redis";
import { Socket, socket } from "nanomsg";


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

const person_socket: Socket = socket("sub");
export function run() {
  const cache: RedisClient = bluebird.promisifyAll(createClient(process.env["CACHE_PORT"], process.env["CACHE_HOST"], { "return_buffers": true })) as RedisClient;
  person_socket.connect(process.env["PERSON-TRIGGER"]);
  person_socket.on("data", function (buf) {
    (async () => {
      try {
        const obj = await msgpack_decode_async(buf);
        log.info("obj" + JSON.stringify(obj));
        const pid = obj["pid"];
        const person = obj["person"];
        log.info(`Got person ${pid} from trigger`);
        const o_buffer = await cache.hgetallAsync("order-entitties");
        const orders = [];
        for (const o of o_buffer) {
          const order = await msgpack_decode_async(o);
          orders.push(order);
        }
        const effective_orders = orders.filter(o => o["insured"]["id"] === pid);
        if (effective_orders.length > 0) {
          for (const effective_order of effective_orders) {
            effective_order["insured"] = person;
            const e_buffer = await msgpack_encode_async(effective_order);
            const oid = effective_order["id"];
            await cache.hsetAsync("order-entitties", oid, e_buffer);
          }
        }
      } catch (e) {
        log.error(e);
      }
    })();
  });
  log.info(`order-trigger is running on ${process.env["PERSON-TRIGGER"]}`);
}

