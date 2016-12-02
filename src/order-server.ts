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
          const nowDate = (new Date()).getTime() + 28800000;
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

server.call("getDriverForVehicle", allowAll, "获得车辆的驾驶人信息", "获得车辆的驾驶人信息", (ctx: ServerContext, rep: ((result: any) => void), vid: string) => {
  log.info(`getDriverForVehicle, vid: ${vid}`);
  if (!verify([uuidVerifier("vid", vid)], (errors: string[]) => {
    rep({
      code: 400,
      msg: errors.join("\n")
    });
  })) {
    return;
  }
  ctx.cache.zrange(`orders-${ctx.uid}`, 0, -1, function (err, result) {
    if (err) {
      log.error(err);
      rep({ code: 500, msg: err.message });
    } else if (result !== null && result.length > 0) {
      const multi = ctx.cache.multi();
      for (const order_key of result) {
        multi.hget("order-entities", order_key);
      }
      multi.exec((err2, replies) => {
        if (err2 || replies === null || replies.length === 0) {
          rep({ code: 404, msg: "not found" });
        } else {
          const user_orders = replies.map(e => JSON.parse(e));
          const driver_orders = user_orders.filter(order => order !== null && order["type"] === 1 && order["vehicle"]["id"] === vid);
          const drivers = [];
          for (const driver_order of driver_orders) {
            for (const d of driver_order["drivers"]) {
              drivers.push(d);
            }
          }
          if (drivers.length === 0) {
            rep({ code: 404, msg: "Drivers not found" });
          } else {
            const nowDate = (new Date()).getTime() + 28800000;
            rep({ code: 200, data: drivers });
          }
        }
      });
    } else {
      rep({ code: 404, msg: "Driver orders not found" });
    }
  });
});

server.call("placeAnPlanOrder", allowAll, "用户下计划订单", "用户下计划订单", (ctx: ServerContext, rep: ((result: any) => void), vid: string, plans: any, qid: string, pmid: string, promotion: number, service_ratio: number, summary: number, payment: number, v_value: number, expect_at: Date) => {
  log.info(`placeAnPlanOrder vid: ${vid}, plans: ${JSON.stringify(plans)}, qid: ${qid}, pmid: ${pmid}, promotion: ${promotion}, service_ratio: ${service_ratio}, summary: ${summary}, payment: ${payment}, v_value: ${v_value}, expect_at: ${expect_at}`);
  if (!verify([uuidVerifier("vid", vid), uuidVerifier("qid", qid), numberVerifier("promotion", promotion), numberVerifier("service_ratio", service_ratio), numberVerifier("summary", summary), numberVerifier("payment", payment), numberVerifier("v_value", v_value)], (errors: string[]) => {
    rep({
      code: 400,
      msg: errors.join("\n")
    });
  })) {
    return;
  }
  const uid = ctx.uid;
  const order_id = uuid.v1();
  const callback = order_id;
  const domain = ctx.domain;
  const pkt: CmdPacket = { cmd: "placeAnPlanOrder", args: [domain, uid, order_id, vid, plans, qid, pmid, promotion, service_ratio, summary, payment, v_value, expect_at, callback] };
  ctx.publish(pkt);
  wait_for_response(ctx.cache, callback, rep);
});

server.call("placeAnDriverOrder", allowAll, "用户下司机划订单", "用户下司机订单", (ctx: ServerContext, rep: ((result: any) => void), vid: string, dids: any, summary: number, payment: number) => {
  log.info(`placeAnDriverOrder, vid: ${vid}, dids: ${JSON.stringify(dids)}, summary: ${summary}, payment: ${payment}`);
  if (!verify([uuidVerifier("vid", vid), numberVerifier("summary", summary), numberVerifier("payment", payment)], (errors: string[]) => {
    rep({
      code: 400,
      msg: errors.join("\n")
    });
  })) {
    return;
  }
  const callback = uuid.v1();
  const uid = ctx.uid;
  const domain = ctx.domain;
  const args = [domain, uid, vid, dids, summary, payment, callback];
  const pkt: CmdPacket = { cmd: "placeAnDriverOrder", args: [domain, uid, vid, dids, summary, payment, callback] };
  ctx.publish(pkt);
  wait_for_response(ctx.cache, callback, rep);
});

server.call("updateOrderState", allowAll, "更新订单状态", "更新订单状态", (ctx: ServerContext, rep: ((result: any) => void), uid: string, order_no: string, state_code: number, state: string) => {
  log.info(`updateOrderState, uid: ${uid}, order_no: ${order_no}, state_code: ${state_code}, state: ${state}`);
  if (!verify([uuidVerifier("uid", uid), stringVerifier("order_no", order_no), numberVerifier("state_code", state_code), stringVerifier("state", state)], (errors: string[]) => {
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
      const order_id = await cache.hgetAsync("orderNo-id", order_no);
      if (!order_id) {
        rep({ code: 404, msg: "Order no not found" });
        return;
      }

      const vid = await cache.hgetAsync("orderid-vid", order_id);
      if (!vid) {
        rep({ code: 404, msg: "Order not found" });
        return;
      }

      const callback = uuid.v1();
      const domain = ctx.domain;

      const pkt: CmdPacket = { cmd: "updateOrderState", args: [domain, uid, vid, order_id, state_code, state, callback] };
      ctx.publish(pkt);

      wait_for_response(ctx.cache, callback, rep);
    } catch (err) {
      log.error(err);
      rep({ code: 500, msg: err.message });
    }
  })();

});

server.call("updateOrderNo", allowAll, "更新订单编号", "更新订单编号", (ctx: ServerContext, rep: ((result: any) => void), order_no: string) => {
  log.info(`updateOrderNo, order_no: ${order_no}`);
  if (!verify([stringVerifier("order_no", order_no)], (errors: string[]) => {
    rep({
      code: 400,
      msg: errors.join("\n")
    });
  })) {
    return;
  }
  ctx.cache.incr("order-no", (err, strNo) => {
    if (err) {
      log.error(err);
      rep({ code: 500, msg: err.message });
    } else if (strNo) {
      const new_no = order_no.substring(0, 14);
      const strno = String(strNo);
      const no: string = formatNum(strno, 7);
      const new_order_no = new_no + no;
      const callback = uuid.v1();
      const pkt: CmdPacket = { cmd: "updateOrderNo", args: [order_no, new_order_no, callback] };
      ctx.publish(pkt);
      wait_for_response(ctx.cache, callback, rep);
    } else {
      rep({ code: 404, msg: "Order no not found" });
    }
  });
});

// 通过vid获取已生效计划单
server.call("getPlanOrderByVehicle", allowAll, "通过vid获取已生效计划单", "通过vid获取已生效计划单", (ctx: ServerContext, rep: ((result: any) => void), vid: string) => {
  log.info(`getPlanOrderByVehicle, vid: ${vid}`);
  if (!verify([uuidVerifier("vid", vid)], (errors: string[]) => {
    rep({
      code: 400,
      msg: errors.join("\n")
    });
  })) {
    return;
  }
  ctx.cache.hget("vid-poid", vid, function (err, result) {
    if (err) {
      log.error(err);
      rep({ code: 500, msg: err.message });
    } else if (result === null) {
      log.info("No plan order for the vid");
      rep({ code: 404, msg: "Order not found" });
    } else {
      ctx.cache.hget("order-entities", result, function (err1, result1) {
        if (err1) {
          log.error(err1);
          rep({ code: 500, msg: err1.message });
        } else if (result1) {
          let nowDate = (new Date()).getTime() + 28800000;
          rep({ code: 200, data: JSON.parse(result1), nowDate: nowDate });
        } else {
          rep({ code: 404, msg: "Order not found" });
        }
      });
    }
  });
});

// 通过vid获取司机单
server.call("getDriverOrderByVehicle", allowAll, "通过vid获取司机单", "通过vid获取司机单", (ctx: ServerContext, rep: ((result: any) => void), vid: string) => {
  log.info(`getDriverOrderByVehicle, vid: ${vid}`);
  if (!verify([uuidVerifier("vid", vid)], (errors: string[]) => {
    rep({
      code: 400,
      msg: errors.join("\n")
    });
  })) {
    return;
  }
  ctx.cache.hget("vid-doid", vid, function (err, result) {
    if (err) {
      log.error(err);
      rep({ code: 500, msg: err.message });
    } else if (result === null) {
      log.info("No driver order for the vid");
      rep({ code: 404, msg: "Order not found" });
    } else {
      let multi = ctx.cache.multi();
      multi.hget("order-entities", result);
      multi.exec((err1, replies1) => {
        if (err1) {
          log.error(err1);
        } else if (replies1 === null || replies1.length === 0) {
          rep({ code: 404, msg: "Orders not found" });
        } else {
          rep({ code: 200, data: replies1.map(e => JSON.parse(e)) });
        }
      });
    }
  });
});

server.call("placeAnSaleOrder", allowAll, "下第三方单", "下第三方单", (ctx: ServerContext, rep: ((result: any) => void), vid: string, pid: string, qid: string, items: any, summary: number, payment: number, opr_level: number) => {
  log.info("placeAnSaleOrder vid: %s, pid: %s, qid: %s, summary: %d, payment: %d, opr_level: %d", vid, pid, qid, summary, payment, opr_level);
  if (!verify([uuidVerifier("vid", vid), uuidVerifier("qid", qid)], (errors: string[]) => {
    log.info("arg not match" + errors);
    rep({
      code: 400,
      msg: errors.join("\n")
    });
  })) {
    return;
  }
  const uid = ctx.uid;
  const order_id = uuid.v1();
  const callback = order_id;
  const domain = ctx.domain;
  const pkt: CmdPacket = { cmd: "placeAnSaleOrder", args: [uid, domain, order_id, vid, pid, qid, items, summary, payment, opr_level, callback] };
  ctx.publish(pkt);
  wait_for_response(ctx.cache, callback, rep);
});
