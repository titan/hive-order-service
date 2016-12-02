import { Server, ServerContext, ServerFunction, CmdPacket, Permission, wait_for_response } from "hive-service";
import { Client as PGClient } from "pg";
import { RedisClient } from "redis";
import * as bunyan from "bunyan";
import * as uuid from "node-uuid";
import * as http from "http";
import { verify, uuidVerifier, stringVerifier, numberVerifier } from "hive-verify";

declare module "redis" {
  export interface RedisClient extends NodeJS.EventEmitter {
    hgetAsync(key: string, field: string): Promise<any>;
    hincrbyAsync(key: string, field: string, value: number): Promise<any>;
    setexAsync(key: string, ttl: number, value: string): Promise<any>;
    zrevrangebyscoreAsync(key: string, start: number, stop: number): Promise<any>;
  }
  export interface Multi extends NodeJS.EventEmitter {
    execAsync(): Promise<any>;
  }
}

const allowAll: Permission[] = [["mobile", true], ["admin", true]];
const mobileOnly: Permission[] = [["mobile", true], ["admin", false]];

export const server = new Server();

const log = bunyan.createLogger({
  name: "order-server",
  streams: [
    {
      level: "info",
      path: "/var/log/order-server-info.log",  // log ERROR and above to a file
      type: "rotating-file",
      period: "1d",   // daily rotation
      count: 7        // keep 7 back copies
    },
    {
      level: "error",
      path: "/var/log/order-server-error.log",  // log ERROR and above to a file
      type: "rotating-file",
      period: "1w",   // daily rotation
      count: 3        // keep 7 back copies
    }
  ]
});

function formatNum(Source: string, Length: number): string {
  let strTemp = "";
  for (let i = 1; i <= Length - Source.length; i++) {
    strTemp += "0";
  }
  return strTemp + Source;
}

server.call("getAllOrders", allowAll, "获取所有订单", "可以根据条件对搜索结果过滤", (ctx: ServerContext, rep: ((result: any) => void), offset: number, limit: number, max_score: number, score: number, order_id: string, owner: string, phone: string, license: string, begin_time: Date, end_time: Date, state: string) => {
  log.info(`getAllOrders, offset: ${offset}, limit: ${limit}, max_score: ${max_score}, score: ${score}, order_id: ${order_id}, owner: ${owner}, phone: ${phone}, license: ${license}, begin_time: ${begin_time}, end_time: ${end_time}, state: ${state}`);
  if (!verify([numberVerifier("offset", offset), numberVerifier("limit", limit)], (errors: string[]) => {
    rep({
      code: 400,
      msg: errors.join("\n")
    });
  })) {
    return;
  }

  const cache = ctx.cache;

  (async () => {
    try {
      const oids = await cache.zrevrangebyscoreAsync("orders", max_score, 0);
      let cursor = offset;
      const len = (oids.length - 1 < limit) ? oids.length : limit - offset + 1;
      const orders = [];
      for (; cursor < len && orders.length < len; cursor ++ ) {
        const oid = oids[cursor];
        const orderjson = await cache.hgetAsync("order-entites", oid);
        if (!orderjson) continue;
        let order = null;
        try {
          order = JSON.parse(orderjson);
        } catch (e) {
          log.error(e);
          continue;
        }
        if (!order["vehicle"] || !order["vehicle"]["owner"]) continue;

        if (owner && order["vehicle"]["owner"]["name"] !== owner) continue;

        if (phone && order["vehicle"]["owner"]["phone"] !== phone) continue;

        if (license && order["vehicle"]["license_no"] !== license) continue;

        if (state && order["state"] !== state) continue;

        if (order_id && order["order_id"] !== order_id) continue;

        const created_at: Date = new Date(order["created_at"]);

        if (begin_time && begin_time.getTime() > created_at.getTime()) continue;

        if (end_time && end_time.getTime() < created_at.getTime()) continue;

        orders.push(order);
      }

      const newoids = await cache.zrevrangebyscoreAsync("orders", score, max_score);
      if (newoids) {
        rep({ code: 200, data: orders, len: oids.length, newOrders: newoids.length, cursor: cursor });
      } else {
        rep({ code: 200, data: orders, len: oids.length, newOrders: 0 });
      }
    } catch (err) {
      log.error(err);
      rep({ code: 500, msg: err.message });
    }
  })();
});


server.call("getOrder", allowAll, "获取订单详情", "获得订单详情", (ctx: ServerContext, rep: ((result: any) => void), oid: string) => {
  log.info(`getOrder oid: ${oid}`);
  if (!verify([uuidVerifier("oid", oid)], (errors: string[]) => {
    rep({
      code: 400,
      msg: errors.join("\n")
    });
  })) {
    return;
  }
  ctx.cache.hget("order-entities", oid, (err, result) => {
    if (err) {
      log.error(err);
      rep({ code: 500, msg: err });
    } else if (result && result !== "") {
      const nowDate = (new Date()).getTime() + 28800000;
      rep({ code: 200, data: JSON.parse(result), nowDate: nowDate });
    } else {
      rep({ code: 404, msg: "Order not found" });
    }
  });
});

server.call("getOrders", allowAll, "获取订单列表", "获得一个用户的所有订单", (ctx: ServerContext, rep: ((result: any) => void), offset: number, limit: number) => {
  log.info(`getOrders, offset: ${offset}, limit: ${limit}`);
  if (!verify([numberVerifier("offset", offset), numberVerifier("limit", limit)], (errors: string[]) => {
    rep({
      code: 400,
      msg: errors.join("\n")
    });
  })) {
    return;
  }
  ctx.cache.zrange(`orders-${ctx.uid}`, offset, limit, function (err, result) {
    if (err) {
      log.error(err);
      rep({ code: 500, msg: err.message });
    } else if (result !== null && result.length === 0) {
      const multi = ctx.cache.multi();
      for (const oid of result) {
        multi.hget("order-entities", oid);
      }
      multi.exec((err2, replies) => {
        if (err2 || replies === null || replies.length === 0) {
          rep({ code: 404, msg: "not found" });
        } else {
          let nowDate = (new Date()).getTime() + 28800000;
          rep({ code: 200, data: replies.map(e => JSON.parse(e)), nowDate: nowDate });
        }
      });
    } else {
      rep({ code: 404, msg: "not found" });
    }
  });
});

server.call("getOrderState", allowAll, "获取订单状态", "获得订单的状态", (ctx: ServerContext, rep: ((result: any) => void), vid: string, qid: string) => {
  log.info(`getOrderState, vid: ${vid}, qid: ${qid}`);
  if (!verify([uuidVerifier("vid", vid), uuidVerifier("qid", qid)], (errors: string[]) => {
    rep({
      code: 400,
      msg: errors.join("\n")
    });
  })) {
    return;
  }
  ctx.cache.hget(`order-vid-${vid}`, qid, (err, result) => {
    if (err) {
      rep({ code: 500, msg: err.message });
    } else if (result) {
      ctx.cache.hget("order-entities", result, function (err1, result1) {
        if (err1) {
          rep({ code: 500, msg: err1.message });
        } else if (result1) {
          rep({ code: 200, data: JSON.parse(result1) });
        } else {
          rep({ code: 404, msg: "Order not found" });
        }
      });
    } else {
      rep({ code: 404, msg: "Order not found" });
    }
  });
});

server.call("getDriverOrders", allowAll, "获取司机订单", "获得司机订单", (ctx: ServerContext, rep: ((result: any) => void), vid: string) => {
  log.info(`getDriverOrders, vid: ${vid}`);
  if (!verify([uuidVerifier("vid", vid)], (errors: string[]) => {
    rep({
      code: 400,
      msg: errors.join("\n")
    });
  })) {
    return;
  }
  ctx.cache.hget("driver-entities-", vid, function (err, result) {
    if (err) {
      log.error(err);
      rep({ code: 500, msg: err.message });
    } else if (result == null) {
      rep({ code: 404, msg: "Order not found" });
    } else {
      rep({ code: 200, data: JSON.parse(result) });
    }
  });
});
