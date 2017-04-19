import { BusinessEventContext, Processor, ProcessorContext, BusinessEventHandlerFunction, BusinessEventListener, rpcAsync, ProcessorFunction, AsyncServerFunction, CmdPacket, Permission, waiting, msgpack_decode_async, msgpack_encode_async } from "hive-service";
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

processor.callAsync("getInsuredUid", async (ctx: ProcessorContext, insured: string): Promise<any> => {
  log.info(`getInsuredUid, insured: ${insured}`);
  try {
    const db: PGClient = ctx.db;
    const cache: RedisClient = ctx.cache;
    const dbrep = await db.query("SELECT uid FROM plan_orders WHERE insured = $1 AND state >2", [insured]);
    if (dbrep["rowCount"] !== 0) {
      const uid = dbrep["rows"][0]["uid"];
      return { code: 200, data: uid };
    } else {
      return { code: 404, msg: "获取订单信息失败(ocb404)" };
    }
  } catch (e) {
    log.info(e);
    return { code: 500, msg: e.message };
  }
});

processor.callAsync("cancel", async (ctx: ProcessorContext, order_id: string): Promise<any> => {
  log.info(`cancel, order_id: ${order_id}`);
  const db: PGClient = ctx.db;
  const cache: RedisClient = ctx.cache;
  try {
    const orep = await cache.hgetAsync("order-entities", order_id);
    if (orep !== null && orep !== "") {
      const order = await msgpack_decode_async(orep);
      const last_state = order["state"];
      let state = null;
      let state_description = null;
      if (last_state === 1) {
        state = 7;
        state_description = "已取消";
      } else if (last_state === 2) {
        state = 8;
        state_description = "支付后取消";
      } else if (last_state === 3) {
        state = 9;
        state_description = "核保后取消";
      } else if (last_state === 4) {
        state = 6;
        state_description = "已失效";
      }
      const time = new Date();
      await db.query("BEGIN");
      await db.query("UPDATE plan_orders SET state = $1, state_description = $2, updated_at = $3 WHERE id = $4", [state, state_description, time, order_id]);
      await db.query("COMMIT");
      await async_plan_orders(db, cache, ctx.domain, ctx.uid, order_id);
      return { code: 200, data: order_id };
    }
  } catch (e) {
    log.info(e);
    return { code: 500, msg: e.message };
  }
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
  const result: QueryResult = await db.query("SELECT o.id AS o_id, o.no AS o_no, o.uid AS o_uid, o.qid AS o_qid, o.vid AS o_vid, o.state AS o_state, o.state_description AS o_state_description, o.summary AS o_summary, o.payment AS o_payment, o.owner AS o_owner, o.insured AS o_insured, o.promotion AS o_promotion, o.service_ratio AS o_service_ratio, o.real_value AS o_real_value, o.inviter AS o_inviter, o.recommend AS o_recommend, o.expect_at AS o_expect_at, o.start_at AS o_start_at, o.stop_at AS o_stop_at, o.paid_at AS o_paid_at,o.driving_frontal_view AS o_driving_frontal_view, o.driving_rear_view AS o_driving_rear_view, o.created_at AS o_created_at, o.updated_at AS o_updated_at, o.evtid AS o_evtid, oi.id AS oi_id, oi.pid AS oi_pid, oi.price AS oi_price, oi.amount AS oi_amount FROM plan_order_items AS oi INNER JOIN plan_orders AS o ON o.id = oi.oid WHERE o.deleted = FALSE" + (oid ? " AND o.id = $1" : ""), oid ? [oid] : []);
  const orders = {};
  try {
    for (const row of result.rows) {
      if (orders.hasOwnProperty(row.o_id)) {
        orders[row.o_id]["items"].push({
          id: row.oi_id,
          pid: row.oi_pid,
          price: row.oi_price,
          amount: row.oi_amount,
          plan: null
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
          summary: parseFloat(row["o_summary"]),
          payment: parseFloat(row["o_payment"]),
          owner_id: row["o_owner"],
          insured_id: row["o_insured"],
          owner: null,
          insured: null,
          promotion: row["o_promotion"],
          service_ratio: row["o_service_ratio"],
          real_value: row["o_real_value"],
          inviter: ["o_inviter"],
          recommend: trim(row["o_recommend"]),
          drivers: [],
          items: [{
            id: row.oi_id,
            pid: row.oi_pid,
            plan: null,
            price: row.oi_price,
            amount: row.oi_amount
          }],
          expect_at: row["o_expect_at"],
          start_at: row["o_start_at"],
          stop_at: row["o_stop_at"],
          paid_at: row["o_paid_at"],
          created_at: row["o_created_at"],
          updated_at: row["o_updated_at"],
          evtid: row["o_evtid"],
          driving_frontal_view: row["o_driving_frontal_view"],
          driving_rear_view: row["o_driving_rear_view"]

        };
        orders[row.o_id] = order;
      }
    }
    const oids = Object.keys(orders);
    const prep = await rpcAsync<Object>(domain, process.env["PLAN"], uid, "getPlans");
    let plans = null;
    if (prep["code"] === 200) {
      plans = prep["data"];
    }
    for (const oid of oids) {
      // 获取drivers
      const drep = await db.query("SELECT pid FROM drivers WHERE oid = $1", [oid]);
      const dids = [];
      for (const row of drep["rows"]) {
        dids.push(row.pid);
      }
      for (const did of dids) {
        const orep = await rpcAsync<Object>(domain, process.env["PERSON"], uid, "getPerson", did);
        if (orep["code"] === 200) {
          orders[oid]["drivers"].push(orep["data"]);
        }
      }
      // vehicle 信息
      const vrep = await rpcAsync<Object>(domain, process.env["VEHICLE"], uid, "getVehicle", orders[oid]["vid"]);
      if (vrep["code"] === 200) {
        const vehicle = vrep["data"];
        orders[oid]["vehicle"] = vehicle;
      }
      // 车主信息
      const orep = await rpcAsync<Object>(domain, process.env["PERSON"], uid, "getPerson", orders[oid]["owner_id"]);
      if (orep["code"] === 200) {
        orders[oid]["owner"] = orep["data"];
      }
      // 投保人信息
      const irep = await rpcAsync<Object>(domain, process.env["PERSON"], uid, "getPerson", orders[oid]["insured_id"]);
      if (irep["code"] === 200) {
        orders[oid]["insured"] = irep["data"];
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
      orders[oid]["items"].sort(function (a, b) {
        return a["plan"]["id"] - b["plan"]["id"];
      });
    }
    const multi = bluebird.promisifyAll(cache.multi()) as Multi;
    for (const oid of oids) {
      const order = orders[oid];
      delete order["insured_id"];
      delete order["owner_id"];
      delete order["pids"];
      const order_no = order["no"];
      const vid = order["vid"];
      const qid = order["qid"];
      const updated_at = order["updated_at"].getTime();
      const newOrder = await msgpack_encode_async(order);
      multi.hset("vid-poid", vid, oid);
      multi.zadd("plan-orders", updated_at, oid);
      multi.zadd("orders", updated_at, oid);
      multi.zadd("orders-" + order["uid"], updated_at, oid);
      multi.zadd("orders-" + order["vid"], updated_at, oid);
      multi.hset("orderNo-id", order_no, oid);
      multi.hset("qid-oid", qid, oid);
      multi.hset("order-entities", oid, newOrder);
    }
    return multi.execAsync();
  } catch (e) {
    log.info(e);
    throw e;
  }
};