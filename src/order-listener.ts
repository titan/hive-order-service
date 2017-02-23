import { BusinessEventContext, BusinessEventHandlerFunction, BusinessEventListener, rpc, ProcessorFunction, AsyncServerFunction, CmdPacket, Permission, set_for_response, waiting, msgpack_decode, msgpack_encode } from "hive-service";
import { Client as PGClient, QueryResult } from "pg";
import { RedisClient, Multi } from "redis";
import { CustomerMessage } from "recommend-library";
import * as bluebird from "bluebird";
import * as bunyan from "bunyan";
import * as uuid from "node-uuid";
import * as http from "http";
import * as queryString from "querystring";
import * as Disq from "hive-disque";


export const listener = new BusinessEventListener("order-events-disque");
const wxhost = process.env["WX_ENV"] === "test" ? "dev.fengchaohuzhu.com" : "m.fengchaohuzhu.com";
const log = bunyan.createLogger({
  name: "order-listener",
  streams: [
    {
      level: "info",
      path: "/var/log/order-listener-info.log",  // log ERROR and above to a file
      type: "rotating-file",
      period: "1d",   // daily rotation
      count: 7        // keep 7 back copies
    },
    {
      level: "error",
      path: "/var/log/order-listener-error.log",  // log ERROR and above to a file
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

function trim(str: string) {
  if (str) {
    return str.trim();
  } else {
    return null;
  }
}

async function sendTmsg(oid: string, cache: RedisClient): Promise<void> {
  try {
    const oJson = await cache.hgetAsync("order-entities", oid);
    const order = await msgpack_decode(oJson);
    const openid = await cache.hgetAsync("wxuser", order["vehicle"]["uid"]);
    const plan = order["plans"].filter(p => p["show_in_index"]);
    const plan_title = plan[0]["title"];
    const carNo = order["vehicle"]["license-no"];
    const order_id = order["id"];
    const postData = queryString.stringify({
      "user": openid,
      "CarNo": carNo,
      "Plan": plan_title,
      "order_id": order_id
    });
    const options = {
      hostname: wxhost,
      port: 80,
      path: "/wx/tmsgPlanEffective",
      method: "GET",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded",
        "Content-Length": Buffer.byteLength(postData)
      }
    };
    const req = http.request(options, (res) => {
      log.info("Status: " + res.statusCode);
      res.setEncoding("utf8");
      res.on("data", (chunk) => {
        console.log(`BODY: ${chunk}`);
      });
      res.on("end", () => {
        console.log("No more data in response.");
      });
    });
    req.on("error", (e) => {
      console.log(`problem with request: ${e.message}`);
    });
    req.write(postData);
    req.end();
    return;
  } catch (e) {
    log.info(e + "err in sendTmsg for orderEffective");
    return;
  }
}


async function increase_plan_order_no(cache, plans): Promise<string> {
  try {
    const strno = await cache.incrAsync("order-no");
    const newstrno = formatNum(String(strno), 7);
    let sum = 0;
    const pids = [];
    for (const plan in plans) {
      pids.push(plan);
    }
    for (const i of pids) {
      const str = i.substring(24);
      const num = parseInt(str);
      sum += num;
    }
    const sum2 = sum.toString();
    const sum1 = formatNum(sum2, 3);
    const date = new Date();
    const year = date.getFullYear();
    const order_no = "1" + "110" + "001" + sum1 + year + newstrno;
    return order_no;
  } catch (e) {
    log.info("error in increase_plan_order_no" + e);
    throw e;
  }
}




listener.onEvent(async (ctx: BusinessEventContext, data: any) => {
  const type = data["type"];
  const order_type = data["order_type"];
  if (type === 0) {
    const rep = await cancel_event(ctx, data);
    return rep;
  } else if (type === 1) {
    const rep = await create_event(ctx, data);
    return rep;
  } else if (type === 2) {
    const rep = await pay_event(ctx, data);
    return rep;
  } else if (type === 3) {
    const rep = await underWrite_event(ctx, data);
    return rep;
  } else if (type === 4) {
    const rep = await take_effect_event(ctx, data);
    return rep;
  } else if (type === 5) {
    const rep = await expired_event(ctx, data);
    return rep;
  } else if (type === 6) {
    const rep = await apply_withdraw_event(ctx, data);
    return rep;
  } else if (type === 7) {
    const rep = await refuse_withdraw_event(ctx, data);
    return rep;
  } else if (type === 8) {
    const rep = await agree_withdraw_event(ctx, data);
    return rep;
  } else if (type === 9) {
    const rep = await refund_event(ctx, data);
    return rep;
  } else if (type === 10) {
    const rep = await rename_no_event(ctx, data);
    return rep;
  } else if (type === 11) {
    const rep = await refresh_plan_order(ctx, data);
    return rep;
  } else if (type === 12) {
    const rep = await refresh_driver_order(ctx, data);
    return rep;
  } else if (type === 13) {
    const rep = await refresh_sale_order(ctx, data);
    return rep;
  } else if (type === 14) {
    const rep = await refresh(ctx);
    return rep;
  }
});

async function create_event(ctx: BusinessEventContext, data: any): Promise<any> {
  try {
    const db: PGClient = ctx.db;
    const cache: RedisClient = ctx.cache;
    const evtid = uuid.v4();
    const oid = uuid.v4();
    const occurred_at = new Date();
    const order_no = await increase_plan_order_no(cache, data["plans"]);
    let orderJson: Object = {};
    if (data["order_type"] === 1) {
      orderJson = { id: evtid, type: data["type"], opid: data["uid"], oid: oid, vid: data["vid"], plans: data["plans"], order_type: data["order_type"], payment: data["payment"], summary: data["summary"], qid: data["qid"], expect_at: data["expect_at"], real_value: data["v_value"], recommend: data["recommend"], ticket: data["ticket"], no: order_no, pm_price: data["promotion"] };
    } else if (data["order_type"] === 2) {
      orderJson = { id: evtid, type: data["type"], order_type: data["order_type"], oid: oid, vid: data["vid"], dids: data["dids"], payment: data["payment"], summary: data["summary"] };
    } else if (data["order_type"] === 3) {
      orderJson = { id: evtid, type: data["type"], order_type: data["order_type"], oid: oid, vid: data["vid"], pid: data["pid"], qid: data["qid"], items: data["items"], payment: data["payment"], summary: data["summary"], opr_level: data["opr_level"] };
    }
    await db.query("INSERT INTO order_events(id, oid, uid, last_state, data, occurred_at) VALUES($1, $2, $3, $4, $5, $6)", [evtid, oid, data["uid"], 0, orderJson, occurred_at]);
    if (data["order_type"] === 1) {
      const result = await create_plan_order(ctx, evtid);
      return result;
    } else if (data["order_type"] === 2) {
      const result = create_driver_order(ctx, evtid);
      return result;
    } else if (data["order_type"] === 3) {
      const result = create_sale_order(ctx, evtid);
      return result;
    }
  } catch (e) {
    log.info(e);
    return { code: 500, msg: e.message };
  }
}

async function cancel_event(ctx: BusinessEventContext, data: any): Promise<any> {
  try {
    const db: PGClient = ctx.db;
    const cache: RedisClient = ctx.cache;
    const evtid = uuid.v4();
    const oJson = await cache.hgetAsync("order-entities", data["order_id"]);
    if (oJson === null || oJson === "") {
      return { code: 404, msg: "未找到对应订单信息" };
    } else {
      const order_entities = await msgpack_decode(oJson);
      if (order_entities["state"] !== 1) {
        return { code: 500, msg: "该状态订单不支持直接取消" };
      } else {
        const occurred_at = new Date();
        await db.query("INSERT INTO order_events(id, oid, uid, last_state, occurred_at) VALUES($1,$2,$3,$4,$5)", [evtid, data["order_id"], ctx["uid"], 1, occurred_at]);
        const result = await underWrite(ctx, evtid);
        return result;
      }
    }
  } catch (e) {
    log.info(e);
    return { code: 500, msg: e.message };
  }
}


async function pay_event(ctx: BusinessEventContext, data: any): Promise<any> {
  try {
    const db: PGClient = ctx.db;
    const cache: RedisClient = ctx.cache;
    const evtid = uuid.v4();
    const oJson = await cache.hgetAsync("order-entities", data["order_id"]);
    if (oJson === null || oJson === "") {
      return { code: 404, msg: "未找到对应订单信息" };
    } else {
      const order_entities = await msgpack_decode(oJson);
      if (order_entities["state"] !== 1) {
        return { code: 500, msg: "该状态订单不支持支付" };
      } else {
        if (order_entities["payment"] > data["amount"]) {
          return { code: 500, msg: "实际支付金额有误" };
        } else {
          const occurred_at = new Date();
          const orderJson = { amount: data["amount"] };
          await db.query("INSERT INTO order_events(id, oid, uid, last_state, data, occurred_at) VALUES($1,$2,$3,$4,$5,$6)", [evtid, data["order_id"], ctx["uid"], 1, orderJson, occurred_at]);
          const result = await pay(ctx, evtid, order_entities["no"]);
          return result;
        }
      }
    }
  } catch (e) {
    log.info(e);
    return { code: 500, msg: e.message };
  }
}

async function underWrite_event(ctx: BusinessEventContext, data: any): Promise<any> {
  try {
    const db: PGClient = ctx.db;
    const cache: RedisClient = ctx.cache;
    const evtid = uuid.v4();
    const oJson = await cache.hgetAsync("order-entities", data["order_id"]);
    if (oJson === null || oJson === "") {
      return { code: 404, msg: "未找到对应订单信息" };
    } else {
      const order_entities = await msgpack_decode(oJson);
      if (order_entities["state"] !== 2) {
        return { code: 500, msg: "该状态订单不支持核保" };
      } else {
        const occurred_at = new Date();
        const orderJson = { start_at: data["start_at"], stop_at: data["stop_at"] };
        await db.query("INSERT INTO order_events(id, oid, uid, last_state, data, occurred_at) VALUES($1,$2,$3,$4,$5,$6)", [evtid, data["order_id"], ctx["uid"], 2, orderJson, occurred_at]);
        const result = await underWrite(ctx, evtid);
        return result;
      }
    }
  } catch (e) {
    log.info(e);
    return { code: 500, msg: e.message };
  }
}

async function take_effect_event(ctx: BusinessEventContext, data: any): Promise<any> {
  try {
    const db: PGClient = ctx.db;
    const cache: RedisClient = ctx.cache;
    const evtid = uuid.v4();
    const oJson = await cache.hgetAsync("order-entities", data["order_id"]);
    if (oJson === null || oJson === "") {
      return { code: 404, msg: "未找到对应订单信息" };
    } else {
      const order_entities = await msgpack_decode(oJson);
      if (order_entities["state"] !== 3) {
        return { code: 500, msg: "该状态订单不支持生效" };
      } else {
        const occurred_at = new Date();
        await db.query("INSERT INTO order_events(id, oid, uid, last_state, occurred_at) VALUES($1,$2,$3,$4,$5)", [evtid, data["order_id"], ctx["uid"], 3, occurred_at]);
        const result = await take_effect(ctx, evtid);
        return result;
      }
    }
  } catch (e) {
    log.info(e);
    return { code: 500, msg: e.message };
  }
}


async function expired_event(ctx: BusinessEventContext, data: any): Promise<any> {
  try {
    const db: PGClient = ctx.db;
    const cache: RedisClient = ctx.cache;
    const evtid = uuid.v4();
    const oJson = await cache.hgetAsync("order-entities", data["order_id"]);
    if (oJson === null || oJson === "") {
      return { code: 404, msg: "未找到对应订单信息" };
    } else {
      const order_entities = await msgpack_decode(oJson);
      const last_state = order_entities["state"];
      if (order_entities["state"] !== 4) {
        return { code: 500, msg: "该状态订单不支持" };
      } else {
        const occurred_at = new Date();
        await db.query("INSERT INTO order_events(id, oid, uid, last_state, occurred_at) VALUES($1,$2,$3,$4,$5)", [evtid, data["order_id"], ctx["uid"], last_state, occurred_at]);
        const result = await expired(ctx, evtid);
        return result;
      }
    }
  } catch (e) {
    log.info(e);
    return { code: 500, msg: e.message };
  }
}

async function apply_withdraw_event(ctx: BusinessEventContext, data: any): Promise<any> {
  try {
    const db: PGClient = ctx.db;
    const cache: RedisClient = ctx.cache;
    const evtid = uuid.v4();
    const oJson = await cache.hgetAsync("order-entities", data["order_id"]);
    if (oJson === null || oJson === "") {
      return { code: 404, msg: "未找到对应订单信息" };
    } else {
      const order_entities = await msgpack_decode(oJson);
      const last_state = order_entities["state"];
      const state = order_entities["state"];
      if (state !== 2 || state !== 3 || state !== 4) {
        return { code: 500, msg: "该状态订单不允许提现" };
      } else {
        const occurred_at = new Date();
        await db.query("INSERT INTO order_events(id, oid, uid, last_state, occurred_at) VALUES($1,$2,$3,$4,$5)", [evtid, data["order_id"], ctx["uid"], last_state, occurred_at]);
        const result = await apply_withdraw(ctx, evtid);
        return result;
      }
    }
  } catch (e) {
    log.info(e);
    return { code: 500, msg: e.message };
  }
}

async function refuse_withdraw_event(ctx: BusinessEventContext, data: any): Promise<any> {
  try {
    const db: PGClient = ctx.db;
    const cache: RedisClient = ctx.cache;
    const evtid = uuid.v4();
    const oJson = await cache.hgetAsync("order-entities", data["order_id"]);
    if (oJson === null || oJson === "") {
      return { code: 404, msg: "未找到对应订单信息" };
    } else {
      const order_entities = await msgpack_decode(oJson);
      const state = order_entities["state"];
      if (state !== 6) {
        return { code: 500, msg: "该状态订单异常" };
      } else {
        const occurred_at = new Date();
        await db.query("INSERT INTO order_events(id, oid, uid, last_state, occurred_at) VALUES($1,$2,$3,$4,$5)", [evtid, data["order_id"], ctx["uid"], 6, occurred_at]);
        const result = await refuse_withdraw(ctx, evtid);
        return result;
      }
    }
  } catch (e) {
    log.info(e);
    return { code: 500, msg: e.message };
  }
}

async function agree_withdraw_event(ctx: BusinessEventContext, data: any): Promise<any> {
  try {
    const db: PGClient = ctx.db;
    const cache: RedisClient = ctx.cache;
    const evtid = uuid.v4();
    const oJson = await cache.hgetAsync("order-entities", data["order_id"]);
    if (oJson === null || oJson === "") {
      return { code: 404, msg: "未找到对应订单信息" };
    } else {
      const order_entities = await msgpack_decode(oJson);
      const state = order_entities["state"];
      if (state !== 6) {
        return { code: 500, msg: "该状态订单异常" };
      } else {
        const occurred_at = new Date();
        await db.query("INSERT INTO order_events(id, oid, uid, last_state, occurred_at) VALUES($1,$2,$3,$4,$5)", [evtid, data["order_id"], ctx["uid"], 6, occurred_at]);
        const result = await agree_withdraw(ctx, evtid);
        return result;
      }
    }
  } catch (e) {
    log.info(e);
    return { code: 500, msg: e.message };
  }
}

async function refund_event(ctx: BusinessEventContext, data: any): Promise<any> {
  try {
    const db: PGClient = ctx.db;
    const cache: RedisClient = ctx.cache;
    const evtid = uuid.v4();
    const oJson = await cache.hgetAsync("order-entities", data["order_id"]);
    if (oJson === null || oJson === "") {
      return { code: 404, msg: "未找到对应订单信息" };
    } else {
      const order_entities = await msgpack_decode(oJson);
      const state = order_entities["state"];
      if (state !== 7) {
        return { code: 500, msg: "该状态订单不支持提现" };
      } else {
        const occurred_at = new Date();
        await db.query("INSERT INTO order_events(id, oid, uid, last_state, occurred_at) VALUES($1,$2,$3,$4,$5)", [evtid, data["order_id"], ctx["uid"], 7, occurred_at]);
        const result = await refund(ctx, evtid);
        return result;
      }
    }
  } catch (e) {
    log.info(e);
    return { code: 500, msg: e.message };
  }
}

async function rename_no_event(ctx: BusinessEventContext, data: any): Promise<any> {
  try {
    const db: PGClient = ctx.db;
    const cache: RedisClient = ctx.cache;
    const evtid = uuid.v4();
    const orderJson = { no: data["order_no"] };
    const oid = await cache.hgetAsync("orderNo-id", data["order_no"]);
    const oJson = await cache.hgetAsync("order-entities", data["order_id"]);
    if (oJson === null || oJson === "") {
      return { code: 404, msg: "未找到对应订单信息" };
    } else {
      const order_entities = await msgpack_decode(oJson);
      const last_state = order_entities["state"];
      if (oid === null || oid === "") {
        return { code: 404, msg: "未找到对应订单信息" };
      } else {
        const occurred_at = new Date();
        await db.query("INSERT INTO order_events(id, oid, uid, last_state, data, occurred_at) VALUES($1,$2,$3,$4,$5,$6)", [evtid, String(oid), ctx["uid"], last_state, orderJson, occurred_at]);
        const result = await rename_no(ctx, evtid);
        return result;
      }
    }
  } catch (e) {
    log.info(e);
    return { code: 500, msg: e.message };
  }
}


async function cancel(ctx: BusinessEventContext, evtid: string): Promise<any> {
  const db: PGClient = ctx.db;
  const cache: RedisClient = ctx.cache;
  try {
    const dbrep = await db.query("SELECT id, oid, uid, last_state, occurred_at FROM order_events WHERE id = $1", [evtid]);
    const result = dbrep["rows"][0];
    const oid = result["oid"];
    const time = new Date();
    await db.query("BEGIN");
    await db.query("UPDATE orders SET deleted updated_at = $3 WHERE id = $4", [true, time, result["oid"]]);
    const oJson = await cache.hgetAsync("order-entities", result["oid"]);
    const order_entities = await msgpack_decode(oJson);
    const vid = order_entities["vehicle"]["id"];
    const vin = order_entities["vehicle"]["vin_code"];
    const uid = result["uid"];
    const no = order_entities["no"];
    const qid = order_entities["qid"];
    const multi = bluebird.promisifyAll(cache.multi()) as Multi;
    multi.zremAsync("new-orders-id", oid);
    multi.hdelAsync("vid-poid", vid);
    multi.zremAsync("plan-orders", oid);
    multi.zremAsync("orders", oid);
    multi.zremAsync("orders" + uid, oid);
    multi.zremAsync("orders" + vid, oid);
    multi.hdelAsync("orderNo-id", no);
    multi.hdelAsync("order-vid-" + vid, qid);
    multi.hdelAsync("qid-oid", qid);
    multi.hdelAsync("orderid-vid", oid);
    multi.hdelAsync("order_entities", oid);
    multi.hdelAsync("VIN-orderID", vin);
    multi.execAsync();
    await db.query("COMMIT");
    return { code: 200, data: oid };
  } catch (e) {
    log.info(e);
    await db.query("ROLLBACK");
    throw e;
  }
}


async function refund(ctx: BusinessEventContext, evtid: string): Promise<any> {
  const db: PGClient = ctx.db;
  const cache: RedisClient = ctx.cache;
  try {
    const dbrep = await db.query("SELECT id, oid, uid, data, occurred_at FROM order_events WHERE id = $1", [evtid]);
    const result = dbrep["rows"][0];
    const data = result["data"];
    const state = 8;
    const state_description = "已提现";
    const time = new Date();
    await db.query("BEGIN");
    await db.query("UPDATE orders SET state = $1,state_description = $2, updated_at = $3 WHERE id = $4", [state, state_description, time, result["oid"]]);
    await async_plan_orders(db, cache, ctx.domain, ctx.uid, result["oid"]);
    await db.query("COMMIT");
    return { code: 200, data: result["oid"] };
  } catch (e) {
    log.info(e);
    await db.query("ROLLBACK");
    throw e;
  }
}

async function agree_withdraw(ctx: BusinessEventContext, evtid: string): Promise<any> {
  const db: PGClient = ctx.db;
  const cache: RedisClient = ctx.cache;
  try {
    const dbrep = await db.query("SELECT id, oid, uid, data, occurred_at FROM order_events WHERE id = $1", [evtid]);
    const result = dbrep["rows"][0];
    const state = 7;
    const state_description = "待提现";
    const time = new Date();
    await db.query("BEGIN");
    await db.query("UPDATE orders SET state = $1,state_description = $2, updated_at = $3 WHERE id = $4", [state, state_description, time, result["oid"]]);
    await async_plan_orders(db, cache, ctx.domain, ctx.uid, result["oid"]);
    await db.query("COMMIT");
    return { code: 200, data: result["oid"] };
  } catch (e) {
    log.info(e);
    db.query("ROLLBACK");
    throw e;
  }
}

async function refuse_withdraw(ctx: BusinessEventContext, evtid: string): Promise<any> {
  const db: PGClient = ctx.db;
  const cache: RedisClient = ctx.cache;
  try {
    const dbrep = await db.query("SELECT id, oid, uid, data, occurred_at FROM order_events WHERE id = $1", [evtid]);
    const result = dbrep["rows"][0];
    const dbrep_byTime = await db.query("SELECT id, oid, uid, last_state, data, occurred_at FROM order_entities WHERE id = $1 ORDER BY occurred_at", [result["oid"]]);
    const events = [];
    for (const row of dbrep_byTime["rows"]) {
      const event = {
        id: row["id"],
        oid: row["oid"],
        uid: row["uid"],
        last_state: row["last_state"]
      };
      events.push(event);
    }
    const len = events.length;
    const last_state = events[len - 1]["last_state"];
    const state = last_state;
    let state_description = null;
    if (last_state !== 2 || last_state !== 3 || last_state !== 4) {
      return { code: 500, msg: "该订单上一状态异常" };
    } else {
      if (last_state === 2) {
        state_description = "已支付";
      } else if (last_state === 3) {
        state_description = "已核保";
      } else if (last_state === 4) {
        state_description = "已生效";
      }
      const time = new Date();
      await db.query("BEGIN");
      await db.query("UPDATE orders SET state = $1,state_description = $2, updated_at = $3 WHERE id = $4", [state, state_description, time, result["oid"]]);
      await async_plan_orders(db, cache, ctx.domain, ctx.uid, result["oid"]);
      await db.query("COMMIT");
      return { code: 200, data: result["oid"] };
    }
  } catch (e) {
    log.info(e);
    db.query("ROLLBACK");
    throw e;
  }
}

async function apply_withdraw(ctx: BusinessEventContext, evtid: string): Promise<any> {
  const db: PGClient = ctx.db;
  const cache: RedisClient = ctx.cache;
  try {
    const dbrep = await db.query("SELECT id, oid, uid, data, occurred_at FROM order_events WHERE id = $1", [evtid]);
    const result = dbrep["rows"][0];
    const state = 6;
    const state_description = "提现审核中";
    const time = new Date();
    await db.query("BEGIN");
    await db.query("UPDATE orders SET state = $1,state_description = $2, updated_at = $3 WHERE id = $4", [state, state_description, time, result["oid"]]);
    await async_plan_orders(db, cache, ctx.domain, ctx.uid, result["oid"]);
    await db.query("COMMIT");
    return { code: 200, data: result["oid"] };
  } catch (e) {
    log.info(e);
    db.query("ROLLBACK");
    throw e;
  }
}

async function expired(ctx: BusinessEventContext, evtid: string): Promise<any> {
  const db: PGClient = ctx.db;
  const cache: RedisClient = ctx.cache;
  try {
    const dbrep = await db.query("SELECT id, oid, uid, data, occurred_at FROM order_events WHERE id = $1", [evtid]);
    const result = dbrep["rows"][0];
    const data = result["data"];
    const state = 5;
    const state_description = "已失效";
    const time = new Date();
    await db.query("BEGIN");
    await db.query("UPDATE orders SET state = $1,state_description = $2, updated_at = $3 WHERE id = $4", [state, state_description, time, result["oid"]]);
    await async_plan_orders(db, cache, ctx.domain, ctx.uid, result["oid"]);
    await db.query("COMMIT");
    return { code: 200, data: result["oid"] };
  } catch (e) {
    log.info(e);
    db.query("ROLLBACK");
    throw e;
  }
}

async function take_effect(ctx: BusinessEventContext, evtid: string): Promise<any> {
  const db: PGClient = ctx.db;
  const cache: RedisClient = ctx.cache;
  try {
    const dbrep = await db.query("SELECT id, oid, uid, data, occurred_at FROM order_events WHERE id = $1", [evtid]);
    const result = dbrep["rows"][0];
    const data = result["data"];
    const state = 4;
    const state_description = "已生效";
    const time = new Date();
    await db.query("BEGIN");
    await db.query("UPDATE orders SET state = $1,state_description = $2, updated_at = $3 WHERE id = $4", [state, state_description, time, result["oid"]]);
    await async_plan_orders(db, cache, ctx.domain, ctx.uid, result["oid"]);
    await db.query("COMMIT");
    await sendTmsg(result["oid"], cache);
    return { code: 200, data: result["oid"] };
  } catch (e) {
    log.info(e);
    db.query("ROLLBACK");
    throw e;
  }
}


async function underWrite(ctx: BusinessEventContext, evtid: string): Promise<any> {
  const db: PGClient = ctx.db;
  const cache: RedisClient = ctx.cache;
  try {
    const dbrep = await db.query("SELECT id, oid, uid, data, occurred_at FROM order_events WHERE id = $1", [evtid]);
    const result = dbrep["rows"][0];
    const data = result["data"];
    const state = 3;
    const state_description = "已核保";
    const time = new Date();
    await db.query("BEGIN");
    await db.query("UPDATE orders SET state = $1, state_description = $2, updated_at = $3, start_at =$4, stop_at = $5 WHERE id = $4", [state, state_description, time, data["start_at"], data["stop_at"], result["oid"]]);
    await async_plan_orders(db, cache, ctx.domain, ctx.uid, result["oid"]);
    await db.query("COMMIT");
    return { code: 200, data: result["oid"] };
  } catch (e) {
    log.info(e);
    await db.query("ROLLBACK");
    throw e;
  }
}


async function pay(ctx: BusinessEventContext, evtid: string, no: string): Promise<any> {
  const db: PGClient = ctx.db;
  const cache: RedisClient = ctx.cache;
  try {
    const dbrep = await db.query("SELECT id, oid, uid, data, occurred_at FROM order_events WHERE id = $1", [evtid]);
    const result = dbrep["rows"][0];
    const data = result["data"];
    const state = 2;
    const state_description = "已支付";
    const paid_at = new Date();
    await db.query("BEGIN");
    await db.query("UPDATE orders SET state = $1, state_description = $2, updated_at = $3 WHERE id = $4", [state, state_description, paid_at, result["oid"]]);
    // recharge
    const wrep = await rpc<Object>(ctx.domain, process.env["WALLET"], ctx.uid, "getUserByUserId", result["oid"]);
    if (wrep["code"] !== 200) {
      return { code: 500, msg: wrep["msg"] };
    } else {
      async_plan_orders(db, cache, ctx.domain, ctx.uid, result["oid"]);
      await db.query("COMMIT");
      const openid = await cache.hgetAsync("wxuser", ctx.uid);
      const postData = queryString.stringify({
        "user": String(openid),
        "OrderNo": no,
        "Plan": "好司机 互助计划",
        "Price": data["amount"],
        "orderId": result["oid"]
      });
      const options = {
        hostname: wxhost,
        port: 80,
        path: "/wx/tmsgPaySuccess",
        method: "GET",
        headers: {
          "Content-Type": "application/x-www-form-urlencoded",
          "Content-Length": Buffer.byteLength(postData)
        }
      };
      const req = http.request(options, (res) => {
        log.info("Status: " + res.statusCode);
        res.setEncoding("utf8");
        res.on("data", (chunk) => {
          console.log(`BODY: ${chunk}`);
        });
        res.on("end", () => {
          console.log("No more data in response.");
        });
      });
      req.on("error", (e) => {
        console.log(`problem with request: ${e.message}`);
      });
      req.write(postData);
      req.end();
      return { code: 200, data: result["oid"] };
    }
  } catch (e) {
    log.info(e);
    await db.query("ROLLBACK");
    throw e;
  }
}

async function rename_no(ctx: BusinessEventContext, evtid: string): Promise<any> {
  try {
    const db: PGClient = ctx.db;
    const cache: RedisClient = ctx.cache;
    const dbrep = await db.query("SELECT id, oid, uid, data, occurred_at FROM order_events WHERE id = $1", [evtid]);
    const result = dbrep["rows"][0];
    const data = result["data"];
    const order_no = data["no"];
    const strNo = await ctx.cache.incr("order-no");
    const new_no = order_no.substring(0, 14);
    const strno = String(strNo);
    const no: string = formatNum(strno, 7);
    const new_order_no = new_no + no;
    await db.query("UPDATE orders SET no = $1 WHERE id = $2", [new_order_no, result["oid"]]);
    await async_plan_orders(db, cache, ctx.domain, ctx.uid, result["oid"]);
    return { code: 200, data: new_order_no };
  } catch (e) {
    log.info(e);
    throw e.message;
  }
}


async function create_plan_order(ctx: BusinessEventContext, evtid: string): Promise<any> {
  const db: PGClient = ctx.db;
  const cache: RedisClient = ctx.cache;
  try {
    const start_at = new Date();
    const dbrep = await db.query("SELECT id, oid, uid, data, occurred_at FROM order_events WHERE id = $1", [evtid]);
    const result = dbrep["rows"][0];
    const data = result["data"];
    const vrep = await rpc<Object>(ctx["domain"], process.env["VEHICLE"], result["uid"], "getVehicle", data["vid"]);
    let pid = null;
    if (vrep["code"] === 200) {
      pid = vrep["data"]["applicant"]["id"];
    }
    await db.query("BEGIN");
    await db.query("INSERT INTO plan_orders(id, no, uid, qid, vid, type, state, state_description, summary, payment, applicant, promotion, service_ratio, vehicle_real_value, ticket, recommend, expect_at, created_at, updated_at, evtid) VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10 ,$11, $12, $13,$14, $15, $16, $17, $18, $19,$20)", [data["oid"], data["no"], result["uid"], data["qid"], data["vid"], data["type"], 1, "已创建", data["summary"], data["payment"], pid, data["pm_price"], data["service_ratio"], data["real_value"], data["ticket"], data["recommend"], data["expect_at"], start_at, start_at, evtid]);
    for (const plan in data["plans"]) {
      const id = uuid.v4();
      await db.query("INSERT INTO plan_order_items(id, oid, pid, price) VALUES($1, $2, $3, $4)", [id, data["oid"], plan, data["plans"][plan]]);
    }
    await async_plan_orders(db, cache, ctx.domain, result["uid"], data["oid"]);
    await db.query("COMMIT");
    // 分销系统消息
    const profile_response = await rpc<Object>(ctx.domain, process.env["PROFILE"], null, "getUserByUserId", ctx.uid);
    if (profile_response["code"] === 200 && profile_response["data"]["ticket"]) {
      const profile = profile_response["data"];
      const cm: CustomerMessage = {
        type: 3,
        ticket: profile["ticket"],
        cid: profile["id"],
        name: profile["nickname"],
        oid: data["oid"],
        qid: data["qid"],
        occurredAt: start_at
      };
      const new_cm = await msgpack_encode(cm);
      await cache.lpushAsync("agent-customer-msg-queue", new_cm);
      return { code: 200, data: data["oid"] };
    }
  } catch (e) {
    log.info(e);
    db.query("ROLLBACK");
    throw e.message;
  }
}


async function create_driver_order(ctx: BusinessEventContext, evtid: string): Promise<any> {
  const db: PGClient = ctx.db;
  const cache: RedisClient = ctx.cache;
  try {
    const dbrep = await db.query("SELECT id, oid, uid, data, occurred_at FROM order_events WHERE id = $1", [evtid]);
    const result = dbrep["rows"][0];
    const data = result["data"];
    const start_at = new Date();
    const vrep = await rpc<Object>(ctx["domain"], process.env["VEHICLE"], result["uid"], "getVehicle", data["vid"]);
    let pid = null;
    if (vrep["code"] === 200) {
      pid = vrep["data"]["applicant"]["id"];
    }
    await db.query("BEGIN");
    await db.query("INSERT INTO driver_orders(id, no, uid, vid, state, state_description, summary, payment, applicant, paid_at, created_at, updated_at, evtid) VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)", [data["oid"], data["no"], result["uid"], data["vid"], 2, "已支付", data["summary"], data["payment"], pid, start_at, start_at, start_at, evtid]);

    for (const did of data["pids"]) {
      const id = uuid.v4();
      await db.query("INSERT INTO driver_order_items(id, oid, pid) VALUES($1, $2, $3)", [id, data["oid"], did]);
    }
    async_driver_orders(db, cache, ctx.domain, result["uid"], data["oid"]);
    await db.query("COMMIT");
    return { code: 200, data: data["oid"] };
  } catch (e) {
    log.info(e);
    db.query("ROLLBACK");
    throw e.message;
  }
}

async function create_sale_order(ctx: BusinessEventContext, evtid: string): Promise<any> {
  const db: PGClient = ctx.db;
  const cache: RedisClient = ctx.cache;
  try {
    const dbrep = await db.query("SELECT id, oid, uid, data, occurred_at FROM order_events WHERE id = $1", [evtid]);
    const result = dbrep["rows"][0];
    const data = result["data"];
    const start_at = new Date();
    const vrep = await rpc<Object>(ctx["domain"], process.env["VEHICLE"], result["uid"], "getVehicle", data["vid"]);
    let pid = null;
    if (vrep["code"] === 200) {
      pid = vrep["data"]["applicant"]["id"];
    }
    await db.query("BEGIN");
    await db.query("INSERT INTO sale_orders(id, no, uid, vid, type, state, state_description, summary, payment, applicant, created_at, updated_at, evtid) VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)", [data["oid"], data["no"], result["uid"], data["vid"], data["type"], 1, "已创建", data["summary"], data["payment"], pid, start_at, start_at, data["evtid"]]);
    async_sale_orders(db, cache, ctx.domain, result["uid"], data["oid"]);
    await db.query("COMMIT");
    return { code: 200, data: data["oid"] };
  } catch (e) {
    log.info(e);
    await db.query("ROLLBACK");
    throw e.message;
  }
}


async function async_plan_orders(db: PGClient, cache: RedisClient, domain: string, uid: string, oid?: string): Promise<any> {
  const result: QueryResult = await db.query("SELECT o.id AS o_id, o.no AS o_no, o.uid AS o_uid, o.pgid AS o_pgid, o.qid AS o_qid, o.vid AS o_vid, o.state AS o_state, o.state_description AS o_state_description, o.summary AS o_summary, o.payment AS o_payment, o.applicant AS o_applicant, o.promotion AS o_promotion, o.service_ratio AS o_service_ratio, o.vehicle_real_value AS o_vehicle_real_value, o.outside_quotation1 AS o_outside_quotation1, o.outside_quotation2 AS o_outside_quotation2, o.screenshot1 AS o_screenshot1, o.screenshot2 AS o_screenshot2, o.ticket AS o_ticket, o.recommend AS o_recommend, o.expect_at AS o_expect_at, o.start_at AS o_start_at, o.stop_at AS o_stop_at, o.paid_at AS o_paid_at, o.created_at AS o_created_at, o.updated_at AS o_updated_at, o.evtid AS o_evtid, oi.id AS oi_id, oi.pid AS oi_pid, oi.price AS oi_price FROM plan_order_items AS oi INNER JOIN orders AS o ON o.id = oi.oid WHERE o.deleted = FALSE" + (oid ? " AND o.id = $1" : ""), oid ? [oid] : []);
  const orders = {};
  try {
    for (const row of result.rows) {
      if (orders.hasOwnProperty(row.o_id)) {
        orders[row.o_id]["items"].push({
          id: row.oi_id,
          oid: row.oi_oid,
          pid: row.oi_pid,
          price: row.oi_price,
          plan: null,
        });
      } else {
        const order = {
          id: row["o_id"],
          no: row["o_no"],
          uid: row["o_uid"],
          pgid: row["o_pgid"],
          qid: row["o_qid"],
          vid: row["o_vid"],
          vehicle: null,
          state: row["o_state"],
          state_description: trim(row["o_state_description"]),
          summary: row["o_summary"],
          payment: row["o_payment"],
          applicant: row["o_applicant"],
          promotion: row["o_promotion"],
          service_ratio: row["o_service_ratio"],
          vehicle_real_value: row["o_vehicle_real_value"],
          outside_quotation1: row["o_outside_quotation1"],
          outside_quotation2: row["o_outside_quotation2"],
          screenshot1: trim(row["o_screenshot1"]),
          screenshot2: trim(row["o_screenshot2"]),
          ticket: trim(row["o_ticket"]),
          recommend: trim(row["o_recommend"]),
          items: [{
            id: row.oi_id,
            oid: row.oi_oid,
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
    const maybe_driver_order_binaries = await cache.hvalsAsync("order-entities");
    const maybe_driver_orders = await Promise.all(maybe_driver_order_binaries.map(o => msgpack_decode(o)));
    const driver_orders = maybe_driver_orders.filter(o => o["type"] === 2);
    for (const oid of oids) {

      // vehicle 信息 以及driver
      const vrep = await rpc<Object>(domain, process.env["VEHICLE"], uid, "getVehicle", orders[oid]["vid"]);
      if (vrep["code"] === 200) {
        const vehicle = vrep["data"];
        const drivers = [];
        for (const driver_order of driver_orders) {
          if (driver_order["vehicle"]["id"] === vehicle["id"]) {
            for (const driver of driver_order["vehicle"]["drivers"]) {
              drivers.push(driver);
            }
          }
        }
        delete vehicle["drivers"];
        vehicle["drivers"] = drivers;
        orders[oid]["vehicle"] = vehicle;
        orders[oid]["applicant"] = vehicle["applicant"];
      }
      // plan 信息
      for (const item of orders[oid]["items"]) {
        const prep = await rpc<Object>(domain, process.env["PLAN"], uid, "getPlan", item["pid"]);
        if (prep["code"] === 200) {
          item["plan"] = prep["data"];
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
    }
  } catch (e) {
    log.info(e);
    throw e.message;
  }
};


async function async_driver_orders(db: PGClient, cache: RedisClient, domain: string, uid: string, oid?: string): Promise<void> {
  const result = await db.query("SELECT o.id AS o_id, o.no AS o_no, o.uid AS o_uid, o.vid AS o_vid, o.state AS o_state, o.state_description AS o_state_description,o.summary AS o_summary, o.payment AS o_payment, o.applicant AS o_applicant, o.paid_at AS o_paid_at, o.start_at AS o_start_at, o.stop_at AS o_stop_at, o.created_at AS o_created_at, o.updated_at AS o_updated_at, o.evtid AS o_evtid oi.id AS oi_id, oi.oid AS oi_oid, oi.pid AS oi_pid FROM driver_order_items AS oi LEFT JOIN orders AS o ON o.id = oi.oid WHERE o.deleted = FALSE AND oi.deleted = FALSE" + (oid ? " AND o.id = $1" : ""), oid ? [oid] : []);
  try {
    const orders = {};
    for (const row of result["rows"]) {
      if (orders.hasOwnProperty(row.o_id)) {
        orders[row.o_id]["dids"].push(row.oi_pid);
      } else {
        const order = {
          id: row["o_id"],
          no: row["o_no"],
          uid: row["o_uid"],
          vid: row["o_vid"],
          drivers: null,
          dids: [row.oi_pid],
          vehicle: null,
          state: row["o_state"],
          state_description: row["o_state_description"],
          summary: row["o_summary"],
          payment: row["o_payment"],
          applicant: row["o_applicant"],
          paid_at: row["o_paid_at"],
          start_at: row["o_start_at"],
          stop_at: row["o_stop_at"],
          created_at: row["o_created_at"],
          updated_at: row["o_updated_at"],
          evtid: row["o_evtid"]
        };
        orders[row.o_id] = order;
      }
    }
    const oids = Object.keys(orders);
    for (const oid of oids) {
      const vrep = await rpc<Object>(domain, process.env["VEHICLE"], uid, "getVehicle", orders[oid]["vid"]);
      if (vrep["code"] === 200) {
        orders[oid]["vehicle"] = vrep["data"];
        orders[oid]["applicant"] = vrep["data"]["applicant"];
      }
      const drivers = [];
      for (const did of orders[oid]["dids"]) {
        for (const driver of orders[oid]["vehicle"]["drivers"]) {
          if (did === driver["id"]) {
            drivers.push(driver);
          }
        }
      }
      orders[oid]["drivers"] = drivers;
    }
    const multi = bluebird.promisifyAll(cache.multi()) as Multi;
    for (const oid of oids) {
      let old_drivers = null;
      const order = orders[oid];
      const updated_at = order.updated_at.getTime();
      const uid = order["uid"];
      const vid = order["vid"];
      const order_no = order["no"];
      const dreps = await cache.hgetAsync("order-driver-entities", vid);
      if (dreps === null || dreps === "") {
        old_drivers = [];
      } else {
        old_drivers = await msgpack_decode(dreps);
      }
      const old_dids = await cache.hgetAsync("vid-doid", vid);
      for (const driver of order["drivers"]) {
        old_drivers.push(driver);
        old_dids.push(driver["id"]);
      }
      const new_drivers = await msgpack_encode(old_drivers);
      const newOrder = await msgpack_encode(order);
      multi.zadd("orders-" + order["vehicle"]["uid"], updated_at, oid);
      multi.zadd("driver_orders", updated_at, oid);
      multi.zadd("orders", updated_at, oid);
      multi.hset("vid-doid", vid, old_dids);
      multi.hset("driver-entities-", vid, JSON.stringify(order["drivers"]));
      multi.hset("order-entities", oid, newOrder);
      multi.hset("order-driver-entities", vid, old_drivers);
      multi.zadd("driver-orders", updated_at, oid);
      multi.zadd("orders-" + order["vehicle"]["id"], updated_at, oid);
      multi.hset("orderNo-id", order_no, oid);
    }
    return multi.execAsync();
  } catch (e) {
    log.info(e);
    throw e;
  }
}


async function async_sale_orders(db: PGClient, cache: RedisClient, domain: string, uid: string, oid?: string): Promise<void> {
  try {
    const result = await db.query("SELECT id, no, uid, vid, type, state, state_description, summary, payment, applicant, paid_at, start_at, stop_at, created_at, updated_at, evtid FROM sale_orders WHERE deleted = FALSE" + (oid ? " AND o.id = $1" : ""), oid ? [oid] : []);
    const orders = [];
    for (const row of result["rows"]) {
      const order = {
        id: row["id"],
        no: row["no"],
        uid: row["uid"],
        vid: row["vid"],
        vehicle: null,
        type: row["type"],
        state: row["state"],
        state_description: row["state_description"],
        summary: row["summary"],
        payment: row["payment"],
        applicant: row["applicant"],
        paid_at: row["paid_at"],
        start_at: row["start_at"],
        stop_at: row["stop_at"],
        created_at: row["created_at"],
        updated_at: row["updated_at"],
        evtid: row["evtid"]
      };
      orders.push(order);
    }
    for (const order of orders) {
      const vrep = await rpc<Object>(domain, process.env["VEHICLE"], uid, "getVehicle", orders[oid]["vid"]);
      if (vrep["code"] === 200) {
        order["vehicle"] = vrep["data"];
        order["applicant"] = vrep["data"]["applicant"];
      }
    }
    const multi = bluebird.promisifyAll(cache.multi()) as Multi;
    for (const order of orders) {
      const vid = order["vehicle"]["id"];
      const updated_at = order.updated_at.getTime();
      const newOrder = await msgpack_encode(order);
      multi.zadd("orders-" + order["vehicle"]["uid"], updated_at, oid);
      multi.zadd("orders", updated_at, oid);
      multi.hset("vid-soid", vid, oid);
      multi.hset("orderid-vid", oid, vid);
      multi.hset("order-entities", oid, newOrder);
      multi.zadd("sale-orders", updated_at, oid);
    }
    return multi.execAsync();
  } catch (e) {
    log.info(e);
    throw e;
  }
}

async function refresh_plan_order(ctx: BusinessEventContext, data: any): Promise<any> {
  try {
    const db: PGClient = ctx.db;
    const cache: RedisClient = ctx.cache;
    const uid = ctx.uid;
    const domain = ctx.domain;
    const oid = data["order_id"];
    await async_plan_orders(db, cache, domain, uid, oid);
    return { code: 200, data: "success" };
  } catch (e) {
    log.info(e);
    return { code: 500, msg: e.message };
  }
}

async function refresh_driver_order(ctx: BusinessEventContext, data: any): Promise<any> {
  try {
    const db: PGClient = ctx.db;
    const cache: RedisClient = ctx.cache;
    const uid = ctx.uid;
    const domain = ctx.domain;
    const oid = data["order_id"];
    await async_driver_orders(db, cache, domain, uid, oid);
    return { code: 200, data: "success" };
  } catch (e) {
    log.info(e);
    return { code: 500, msg: e.message };
  }
}

async function refresh_sale_order(ctx: BusinessEventContext, data: any): Promise<any> {
  try {
    const db: PGClient = ctx.db;
    const cache: RedisClient = ctx.cache;
    const uid = ctx.uid;
    const domain = ctx.domain;
    const oid = data["order_id"];
    await async_sale_orders(db, cache, domain, uid, oid);
    return { code: 200, data: "success" };
  } catch (e) {
    log.info(e);
    return { code: 500, msg: e.message };
  }
}

async function refresh(ctx: BusinessEventContext): Promise<any> {
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
}