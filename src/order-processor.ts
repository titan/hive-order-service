import { BusinessEventContext, Processor, ProcessorContext, BusinessEventHandlerFunction, BusinessEventListener, rpc, ProcessorFunction, AsyncServerFunction, CmdPacket, Permission, waiting, msgpack_decode_async as msgpack_decode, msgpack_encode_async as msgpack_encode } from "hive-service";
import { Client as PGClient, QueryResult } from "pg";
import { RedisClient, Multi } from "redis";
import { CustomerMessage } from "recommend-library";
import * as bluebird from "bluebird";
import * as bunyan from "bunyan";
import * as uuid from "uuid";
import * as http from "http";
import * as queryString from "querystring";
import * as Disq from "hive-disque";


export const processor = new Processor();

const log = bunyan.createLogger({
  name: "order-processor",
  streams: [
    {
      level: "info",
      path: "/var/log/order-processor-info.log",  // log ERROR and above to a file
      type: "rotating-file",
      period: "1d",   // daily rotation
      count: 7        // keep 7 back copies
    },
    {
      level: "error",
      path: "/var/log/order-processor-error.log",  // log ERROR and above to a file
      type: "rotating-file",
      period: "1w",   // daily rotation
      count: 3        // keep 7 back copies
    }
  ]
});

processor.callAsync("refresh", async (ctx: ProcessorContext): Promise<any> => {
  try {
    const db: PGClient = ctx.db;
    const cache: RedisClient = ctx.cache;
    const uid = ctx.uid;
    const domain = ctx.domain;
    await async_plan_orders(db, cache, domain, uid);
    return { code: 200, data: "success" };
  } catch (e) {
    log.info(e);
    return { code: 500, msg: e.message };
  }
});


function trim(str: string) {
  if (str) {
    return str.trim();
  } else {
    return null;
  }
}

async function async_plan_orders(db: PGClient, cache: RedisClient, domain: string, uid: string, oid?: string): Promise<any> {
  const result: QueryResult = await db.query("SELECT o.id AS o_id, o.no AS o_no, o.uid AS o_uid, o.pgid AS o_pgid, o.qid AS o_qid, o.vid AS o_vid, o.state AS o_state, o.state_description AS o_state_description, o.summary AS o_summary, o.payment AS o_payment, o.insured AS o_insured, o.promotion AS o_promotion, o.service_ratio AS o_service_ratio, o.vehicle_real_value AS o_vehicle_real_value, o.ticket AS o_ticket, o.recommend AS o_recommend, o.expect_at AS o_expect_at, o.start_at AS o_start_at, o.stop_at AS o_stop_at, o.paid_at AS o_paid_at, o.created_at AS o_created_at, o.updated_at AS o_updated_at, o.evtid AS o_evtid, oi.id AS oi_id, oi.pid AS oi_pid, oi.price AS oi_price FROM plan_order_items AS oi INNER JOIN plan_orders AS o ON o.id = oi.oid WHERE o.deleted = FALSE" + (oid ? " AND o.id = $1" : ""), oid ? [oid] : []);
  const orders = {};
  try {
    for (const row of result.rows) {
      if (orders.hasOwnProperty(row.o_id)) {
        orders[row.o_id]["items"].push({
          id: row.oi_id,
          pid: row.oi_pid,
          price: row.oi_price,
          plan: null,
        });
      } else {
        const order = {
          id: row["o_id"],
          no: row["o_no"],
          uid: row["o_uid"],
          type: 1,
          pgid: row["o_pgid"],
          qid: row["o_qid"],
          vid: row["o_vid"],
          vehicle: null,
          state: row["o_state"],
          state_description: trim(row["o_state_description"]),
          summary: row["o_summary"],
          payment: row["o_payment"],
          insured: row["o_insured"],
          promotion: row["o_promotion"],
          service_ratio: row["o_service_ratio"],
          vehicle_real_value: row["o_vehicle_real_value"],
          ticket: trim(row["o_ticket"]),
          recommend: trim(row["o_recommend"]),
          drivers: null,
          items: [{
            id: row.oi_id,
            pid: row.oi_pid,
            plan: null,
            price: row.oi_price
          }],
          expect_at: row["o_expect_at"],
          start_at: row["o_start_at"],
          stop_at: row["o_stop_at"],
          paid_at: row["o_paid_at"],
          created_at: row["o_created_at"],
          updated_at: row["o_updated_at"],
          evtid: row["o_evtid"]
        };
        orders[row.o_id] = order;
      }
    }
    const oids = Object.keys(orders);
    const prep = await rpc<Object>(domain, process.env["PLAN"], uid, "getPlans");
    let plans = null;
    if (prep["code"] === 200) {
      plans = prep["data"];
    }
    for (const oid of oids) {
      // vehicle 信息 以及driver
      const vrep = await rpc<Object>(domain, process.env["VEHICLE"], uid, "getVehicle", orders[oid]["vid"]);
      if (vrep["code"] === 200) {
        const vehicle = vrep["data"];
        orders[oid]["vehicle"] = vehicle;
        orders[oid]["insured"] = vehicle["insured"];
      }
      // plan 信息
      for (const item of orders[oid]["items"]) {
        for (const plan of plans) {
          if (item["pid"] === plan["id"]) {
            item["plan"] = plan;
            delete item["pid"];
          }
        }
      }
    }
    const multi = bluebird.promisifyAll(cache.multi()) as Multi;
    for (const oid of oids) {
      const order = orders[oid];
      const order_no = order["no"];
      const vid = order["vid"];
      const qid = order["qid"];
      const updated_at = order["updated_at"].getTime();
      const newOrder = await msgpack_encode(order);
      multi.zadd("new-orders-id", updated_at, oid);
      multi.hset("vid-poid", vid, oid);
      multi.zadd("plan-orders", updated_at, oid);
      multi.zadd("orders", updated_at, oid);
      multi.zadd("orders-" + order["vehicle"]["uid"], updated_at, oid);
      multi.zadd("orders-" + order["vehicle"]["id"], updated_at, oid);
      multi.hset("orderNo-id", order_no, oid);
      multi.hset("order-vid-" + vid, qid, oid);
      multi.hset("qid-oid", qid, oid);
      multi.hset("orderid-vid", oid, vid);
      multi.hset("order-entities", oid, newOrder);
      multi.hset("VIN-orderID", order["vehicle"]["vin_code"], oid);
    }
    return multi.execAsync();
  } catch (e) {
    log.info(e);
    throw e.message;
  }
};