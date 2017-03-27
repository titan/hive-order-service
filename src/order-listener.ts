import { BusinessEventContext, BusinessEventHandlerFunction, BusinessEventListener, rpcAsync, ProcessorFunction, AsyncServerFunction, CmdPacket, Permission, set_for_response, waiting, msgpack_decode_async as msgpack_decode, msgpack_encode_async as msgpack_encode } from "hive-service";
import { Client as PGClient, QueryResult } from "pg";
import { RedisClient, Multi } from "redis";
import { CustomerMessage } from "recommend-library";
import * as bluebird from "bluebird";
import * as bunyan from "bunyan";
import * as uuid from "uuid";
import * as moment from "moment";
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
    const openid = await cache.hgetAsync("wxuser", order["uid"]);
    const carNo = order["vehicle"]["license_no"];
    const st = order["start_at"];
    const sp = order["stop_at"];
    const start_at = `${st.getFullYear()}-${st.getMonth() + 1}-${st.getDate() + 1}` + " " + "00:00:00";
    const stop_at = `${sp.getFullYear()}-${sp.getMonth() + 1}-${sp.getDate()}` + " " + "23:59:59";
    const postData = queryString.stringify({
      "user": String(openid),
      "CarNo": carNo,
      "Plan": "好车主互助计划",
      "order_id": oid,
      "start_at": start_at,
      "stop_at": stop_at
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
      const num = parseInt(i);
      sum += num;
    }
    const sum2 = sum.toString();
    const sum1 = formatNum(sum2, 10);
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
  if (data !== null) {
    log.info("data" + JSON.stringify(data));
    const type = data["type"];
    const order_type = data["order_type"];
    if (type === 0) {
      const rep = await cancel_event(ctx, data);
      log.info("cancel rep:" + JSON.stringify(rep));
      if (rep["code"] === 500) {
        return rep;
      } else {
        return rep;
      }
    } else if (type === 1) {
      const rep = await create_event(ctx, data);
      log.info("create_plan_order rep:" + JSON.stringify(rep));
      if (rep["code"] === 500) {
        return rep;
      } else {
        return rep;
      }
    } else if (type === 2) {
      const rep = await pay_event(ctx, data);
      log.info("pay rep:" + JSON.stringify(rep));
      if (rep["code"] === 500) {
        return rep;
      } else {
        return rep;
      }
    } else if (type === 3) {
      const rep = await underWrite_event(ctx, data);
      log.info("underWrite rep:" + JSON.stringify(rep));
      if (rep["code"] === 500) {
        return rep;
      } else {
        return rep;
      }
    } else if (type === 4) {
      const rep = await take_effect_event(ctx, data);
      log.info("take_effect rep:" + JSON.stringify(rep));
      if (rep["code"] === 500) {
        return rep;
      } else {
        return rep;
      }
    } else if (type === 5) {
      const rep = await expired_event(ctx, data);
      log.info("expired rep:" + JSON.stringify(rep));
      if (rep["code"] === 500) {
        return rep;
      } else {
        return rep;
      }
    } else if (type === 6) {
      const rep = await apply_withdraw_event(ctx, data);
      log.info("apply_withdraw rep:" + JSON.stringify(rep));
      if (rep["code"] === 500) {
        return rep;
      } else {
        return rep;
      }
    } else if (type === 7) {
      const rep = await refuse_withdraw_event(ctx, data);
      log.info("refuse_withdraw rep:" + JSON.stringify(rep));
      if (rep["code"] === 500) {
        return rep;
      } else {
        return rep;
      }
    } else if (type === 8) {
      const rep = await agree_withdraw_event(ctx, data);
      log.info("agree_withdraw rep:" + JSON.stringify(rep));
      if (rep["code"] === 500) {
        return rep;
      } else {
        return rep;
      }
    } else if (type === 9) {
      const rep = await refund_event(ctx, data);
      log.info("refund rep:" + JSON.stringify(rep));
      if (rep["code"] === 500) {
        return rep;
      } else {
        return rep;
      }
    } else if (type === 10) {
      const rep = await rename_no_event(ctx, data);
      log.info("rename_no rep:" + JSON.stringify(rep));
      if (rep["code"] === 500) {
        return rep;
      } else {
        return rep;
      }
    } else if (type === 11) {
      const rep = await addDrivers_event(ctx, data);
      log.info("addDrivers rep:" + JSON.stringify(rep));
      if (rep["code"] === 500) {
        return rep;
      } else {
        return rep;
      }
    } else if (type === 12) {
      const rep = await delDrivers_event(ctx, data);
      log.info("delDrivers rep:" + JSON.stringify(rep));
      if (rep["code"] === 500) {
        return rep;
      } else {
        return rep;
      }
    } else if (type === 13) {
      const rep = await updateDrivingView_event(ctx, data);
      log.info("updateDrivingView rep:" + JSON.stringify(rep));
      if (rep["code"] === 500) {
        return rep;
      } else {
        return rep;
      }
    }
    else if (type === 20) {
      const rep = await refresh_plan_order(ctx, data);
      log.info("refresh_plan_order rep:" + JSON.stringify(rep));
      if (rep["code"] === 500) {
        return rep;
      } else {
        return rep;
      }
    } else if (type === 21) {
      const rep = await refresh(ctx);
      log.info("refresh rep:" + JSON.stringify(rep));
      if (rep["code"] === 500) {
        return rep;
      } else {
        return rep;
      }
    }
  }
});


async function imputedPrice(data, ctx): Promise<any> {
  log.info("imputedPrice");
  try {
    const qid = data["qid"];
    const plans = data["plans"];
    const prep = await rpcAsync<Object>(ctx["domain"], process.env["PROFILE"], ctx["uid"], "getUser", ctx.uid);
    if (prep["code"] === 200) {
      const ticket = prep["data"]["ticket"];
      const qrep = await rpcAsync<Object>(ctx["domain"], process.env["QUOTATION"], ctx["uid"], "getQuotation", qid);
      if (qrep["code"] === 200) {
        const vrep = await rpcAsync<Object>(ctx["domain"], process.env["VEHICLE"], ctx["uid"], "getVehicle", data["vid"]);
        const recommend = qrep["data"]["recommend"];
        let purchase_price = null;
        let real_value = 0;
        let amount = 0;
        if (vrep["code"] === 200) {
          purchase_price = parseFloat(vrep["data"]["model"]["purchase_price"]);
          const register_date = vrep["data"]["register_date"];
          if (register_date !== "" && register_date !== null) {
            const expect_at = data["expect_at"];
            const r_time = new Date(register_date);
            const e_time = new Date(expect_at);
            const r = moment([r_time.getFullYear(), r_time.getMonth(), r_time.getDate()]);
            const e = moment([e_time.getFullYear(), e_time.getMonth(), e_time.getDate()]);
            const days = e.diff(r, "days");
            log.info("days" + days);
            const month = Math.floor(days / 30);
            real_value = parseFloat((purchase_price * (1 - month * 0.006)).toFixed(2));
            const low_price = purchase_price * 0.20;
            if (low_price >= real_value) {
              real_value = low_price;
            };
            const now = new Date();
            const n = moment([now.getFullYear(), now.getMonth() + 1]);
            const e1 = moment([r_time.getFullYear(), r_time.getMonth() + 1]);
            const rtn = n.diff(e1, "year")
            console.log("years" + rtn);
            if (rtn >= 11) {
              amount = parseFloat((real_value * 0.85).toFixed(2));
            } else {
              amount = parseFloat((real_value * 0.9).toFixed(2));
            }
          } else {
            real_value = 0;
            amount = 0;
          }
        }
        const quotation = qrep["data"];
        const items = quotation["items"];
        const promotion = quotation["promotion"];
        let summary = 0;
        const p_keys = Object.keys(plans);
        const new_keys = p_keys.filter(key => key !== "67108864" || key !== "33554432");
        for (const key of new_keys) {
          for (const item of items) {
            if (String(item["plan"]["id"]) === key) {
              for (const pair of item["pairs"]) {
                if (plans[key] === pair["type"]) {
                  plans[key] = parseFloat(pair["real_price"]);
                  summary += parseFloat(pair["real_price"]);
                }
              }
            }
          }
        }
        const payment = summary - parseFloat(promotion);
        const new_data = { recommend: recommend, summary: summary, payment: payment, promotion: promotion, plans: plans, ticket: ticket, real_value: real_value, amount: amount };
        return { code: 200, data: new_data };
      } else {
        return { code: qrep["code"], msg: qrep["msg"] };
      }
    } else {
      return { code: prep["code"], msg: prep["msg"] };
    }
  } catch (e) {
    log.info(e);
    throw e;
  }
}


async function create_event(ctx: BusinessEventContext, data: any): Promise<any> {
  log.info("create_event");
  try {
    const db: PGClient = ctx.db;
    const cache: RedisClient = ctx.cache;
    const evtid = uuid.v4();
    const oid = uuid.v4();
    let ticket = null;
    const prep = await rpcAsync<Object>(ctx.domain, process.env["PROFILE"], data["uid"], "getUser", ctx["uid"]);
    if (prep["code"] === 200) {
      ticket = prep["data"]["ticket"];
    }
    const occurred_at = new Date();
    const order_no = await increase_plan_order_no(cache, data["plans"]);
    const result = await imputedPrice(data, ctx);
    if (result["code"] === 200) {
      const new_data = result["data"];
      let orderJson: Object = {};
      if (data["order_type"] === 1) {
        orderJson = { id: evtid, type: data["type"], opid: ctx["uid"], oid: oid, vid: data["vid"], plans: new_data["plans"], order_type: data["order_type"], payment: new_data["payment"], summary: new_data["summary"], qid: data["qid"], expect_at: data["expect_at"], amount: new_data["amount"], real_value: new_data["real_value"], recommend: new_data["recommend"], ticket: ticket, no: order_no, pm_price: new_data["promotion"], owner: data["owner"], insured: data["insured"] };
      }
      await db.query("INSERT INTO order_events(id, oid, uid, event_type, order_type, data, occurred_at) VALUES($1, $2, $3, $4, $5, $6, $7)", [evtid, oid, ctx["uid"], 1, 1, orderJson, occurred_at]);
      if (data["order_type"] === 1) {
        const result = await create_plan_order(ctx, evtid);
        return result;
      }
    } else {
      return { code: result["code"], msg: result["msg"] };
    }
  } catch (e) {
    log.info(e);
    return { code: 500, msg: e };
  }
}

async function cancel_event(ctx: BusinessEventContext, data: any): Promise<any> {
  log.info("cancel_event");
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
      if (state !== 1 && state !== 2 && state !== 3 && state !== 4) {
        return { code: 501, msg: "该状态订单不支持直接取消" };
      } else {
        const orderJson = { last_state: state };
        const occurred_at = new Date();
        const event_type = 0;
        const order_type = 1;
        const uid = order_entities["uid"];
        await db.query("INSERT INTO order_events(id, oid, uid, event_type, order_type,data, occurred_at) VALUES($1, $2, $3, $4, $5, $6, $7)", [evtid, data["order_id"], uid, event_type, order_type, orderJson, occurred_at]);
        const result = await cancel(ctx, evtid);
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
        return { code: 501, msg: "该状态订单不支持支付" };
      } else {
        if (parseFloat(order_entities["payment"]) !== parseFloat(data["amount"])) {
          return { code: 502, msg: "实际支付金额有误" };
        } else {
          const occurred_at = new Date();
          const identity_no = order_entities["insured"]["identity_no"];
          const orderJson = { amount: data["amount"], identity_no: identity_no };
          const event_type = 2;
          const order_type = 1;
          await db.query("INSERT INTO order_events(id, oid, uid, event_type, order_type, data, occurred_at) VALUES($1,$2,$3,$4,$5,$6,$7)", [evtid, data["order_id"], data["uid"], event_type, order_type, orderJson, occurred_at]);
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
      if (order_entities["state"] !== 2 && order_entities["state"] !== 3) {
        return { code: 501, msg: "该状态订单不支持核保" };
      } else {
        const occurred_at = new Date();
        const orderJson = { start_at: data["start_at"], stop_at: data["stop_at"] };
        const event_type = 3;
        const order_type = 1;
        const uid = order_entities["uid"];
        await db.query("INSERT INTO order_events(id, oid, uid, event_type, order_type, data, occurred_at) VALUES($1,$2,$3,$4,$5,$6,$7)", [evtid, data["order_id"], uid, event_type, order_type, orderJson, occurred_at]);
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
        return { code: 501, msg: "该状态订单不支持生效" };
      } else {
        const occurred_at = new Date();
        const event_type = 4;
        const order_type = 1;
        const uid = order_entities["uid"];
        await db.query("INSERT INTO order_events(id, oid, uid, event_type, order_type, occurred_at) VALUES($1,$2,$3,$4,$5,$6)", [evtid, data["order_id"], uid, event_type, order_type, occurred_at]);
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
        return { code: 501, msg: "该状态订单不支持" };
      } else {
        const occurred_at = new Date();
        const event_type = 5;
        const order_type = 1;
        const uid = order_entities["uid"];
        await db.query("INSERT INTO order_events(id, oid, uid, event_type, order_type,occurred_at) VALUES($1,$2,$3,$4,$5,$6)", [evtid, data["order_id"], uid, event_type, order_type, occurred_at]);
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
        return { code: 501, msg: "该状态订单不允许提现" };
      } else {
        const occurred_at = new Date();
        const event_type = 6;
        const order_type = 1;
        const uid = order_entities["uid"];
        await db.query("INSERT INTO order_events(id, oid, uid, event_type, order_type, occurred_at) VALUES($1,$2,$3,$4,$5,$6)", [evtid, data["order_id"], uid, event_type, order_type, occurred_at]);
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
        return { code: 501, msg: "该状态订单异常" };
      } else {
        const occurred_at = new Date();
        const event_type = 7;
        const order_type = 1;
        const uid = order_entities["uid"];
        await db.query("INSERT INTO order_events(id, oid, uid, event_type, order_type, occurred_at) VALUES($1,$2,$3,$4,$5,$6)", [evtid, data["order_id"], uid, event_type, order_type, occurred_at]);
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
        return { code: 501, msg: "该状态订单异常" };
      } else {
        const occurred_at = new Date();
        const event_type = 8;
        const order_type = 1;
        const uid = order_entities["uid"];
        await db.query("INSERT INTO order_events(id, oid, uid, event_type, order_type, occurred_at) VALUES($1,$2,$3,$4,$5,$6)", [evtid, data["order_id"], uid, event_type, order_type, occurred_at]);
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
        return { code: 501, msg: "该状态订单不支持提现" };
      } else {
        const occurred_at = new Date();
        const event_type = 9;
        const order_type = 1;
        const uid = order_entities["uid"];
        await db.query("INSERT INTO order_events(id, oid, uid, event_type, order_type, occurred_at) VALUES($1,$2,$3,$4,$5,$6)", [evtid, data["order_id"], uid, event_type, order_type, occurred_at]);
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
    if (oid === null || oid === "") {
      return { code: 404, msg: "未找到对应订单信息" };
    } else {
      const oJson = await cache.hgetAsync("order-entities", String(oid));
      if (oJson === null || oJson === "") {
        return { code: 404, msg: "未找到对应订单信息" };
      } else {
        const order_entities = await msgpack_decode(oJson);
        const last_state = order_entities["state"];
        if (last_state !== 1) {
          return { code: 501, msg: "该状态订单不支持修改订单号" };
        } else {
          const occurred_at = new Date();
          const event_type = 10;
          const order_type = 1;
          const uid = order_entities["uid"];
          await db.query("INSERT INTO order_events(id, oid, uid, event_type, order_type, data, occurred_at) VALUES($1, $2, $3, $4, $5, $6, $7)", [evtid, String(oid), uid, event_type, order_type, orderJson, occurred_at]);
          const result = await rename_no(ctx, evtid);
          log.info("result" + JSON.stringify(result));
          return result;
        }
      }
    }
  } catch (e) {
    log.info(e);
    return { code: 500, msg: e.message };
  }
}

async function addDrivers_event(ctx: BusinessEventContext, data: any): Promise<any> {
  log.info("addDrivers_event");
  try {
    const db: PGClient = ctx.db;
    const cache: RedisClient = ctx.cache;
    const evtid = uuid.v4();
    const orderJson = { dids: data["dids"] };
    const oJson = await cache.hgetAsync("order-entities", data["order_id"]);
    const order_entities = await msgpack_decode(oJson);
    if (order_entities["uid"] !== ctx.uid) {
      return { code: 502, msg: "您没有权限对该计划绑定驾驶人" };
    } else {
      const last_state = order_entities["state"];
      const occurred_at = new Date();
      const event_type = 11;
      const order_type = 1;
      await db.query("INSERT INTO order_events(id, oid, uid, event_type, order_type, data, occurred_at) VALUES($1, $2, $3, $4, $5, $6, $7)", [evtid, data["order_id"], ctx.uid, event_type, order_type, orderJson, occurred_at]);
      const result = await addDrivers(ctx, evtid);
      return result;
    }
  } catch (e) {
    log.info(e);
    return { code: 500, msg: e.message };
  }
};

async function delDrivers_event(ctx: BusinessEventContext, data: any): Promise<any> {
  try {
    const db: PGClient = ctx.db;
    const cache: RedisClient = ctx.cache;
    const evtid = uuid.v4();
    const orderJson = { dids: data["dids"] };
    const oJson = await cache.hgetAsync("order-entities", data["order_id"]);
    const order_entities = await msgpack_decode(oJson);
    if (order_entities["uid"] !== ctx.uid) {
      return { code: 502, msg: "您没有权限对该计划绑定驾驶人" };
    } else {
      const last_state = order_entities["state"];
      const occurred_at = new Date();
      const event_type = 12;
      const order_type = 1;
      await db.query("INSERT INTO order_events(id, oid, uid, event_type, order_type, data, occurred_at) VALUES($1, $2, $3, $4, $5, $6, $7)", [evtid, data["order_id"], ctx.uid, event_type, order_type, orderJson, occurred_at]);
      const result = await delDrivers(ctx, evtid);
      return result;
    }
  } catch (e) {
    log.info(e);
    return { code: 500, msg: e.message };
  }
};


async function updateDrivingView_event(ctx: BusinessEventContext, data: any): Promise<any> {
  try {
    const db: PGClient = ctx.db;
    const cache: RedisClient = ctx.cache;
    const evtid = uuid.v4();
    const orderJson = { driving_frontal_view: data["driving_frontal_view"], driving_rear_view: data["driving_rear_view"] };
    const oJson = await cache.hgetAsync("order-entities", data["order_id"]);
    const order_entities = await msgpack_decode(oJson);
    if (order_entities["uid"] !== ctx.uid) {
      return { code: 502, msg: "您没有权限对该计划绑定驾驶人" };
    } else {
      const last_state = order_entities["state"];
      const occurred_at = new Date();
      const event_type = 13;
      const order_type = 1;
      await db.query("INSERT INTO order_events(id, oid, uid, event_type, order_type, data, occurred_at) VALUES($1, $2, $3, $4, $5, $6, $7)", [evtid, data["order_id"], ctx.uid, event_type, order_type, orderJson, occurred_at]);
      const result = await updateDrivingView(ctx, evtid);
      return result;
    }
  } catch (e) {
    log.info(e);
    return { code: 500, msg: e.message };
  }
}

async function updateDrivingView(ctx: BusinessEventContext, evtid: string): Promise<any> {
  const db: PGClient = ctx.db;
  const cache: RedisClient = ctx.cache;
  try {
    const dbrep = await db.query("SELECT id, oid, uid, data, occurred_at FROM order_events WHERE id = $1", [evtid]);
    const result = dbrep["rows"][0];
    const driving_frontal_view = result["data"]["driving_frontal_view"];
    const driving_rear_view = result["data"]["driving_rear_view"];
    const oid = result["oid"];
    await db.query("BEGIN");
    await db.query("UPDATE plan_orders SET driving_frontal_view = $1,driving_rear_view = $2 WHERE id = $3", [driving_frontal_view, driving_rear_view, oid])
    await async_plan_orders(db, cache, ctx.domain, ctx.uid, result["oid"]);
    await db.query("COMMIT");
    return { code: 200, data: "success" };
  } catch (e) {
    log.info(e);
    await db.query("ROLLBACK");
    return { code: 500, msg: e.message };
  }
}

async function addDrivers(ctx: BusinessEventContext, evtid: string): Promise<any> {
  log.info("addDrivers");
  const db: PGClient = ctx.db;
  const cache: RedisClient = ctx.cache;
  try {
    const dbrep = await db.query("SELECT id, oid, uid, data, occurred_at FROM order_events WHERE id = $1", [evtid]);
    const result = dbrep["rows"][0];
    const oid = result["oid"];
    const dids = result["data"]["dids"];
    await db.query("BEGIN");
    for (const did of dids) {
      await db.query("INSERT INTO drivers(pid, oid) VALUES($1, $2)", [did, oid]);
    }
    await async_plan_orders(db, cache, ctx.domain, ctx.uid, result["oid"]);
    await db.query("COMMIT");
    return { code: 200, data: "success" };
  } catch (e) {
    log.info(e);
    await db.query("ROLLBACK");
    return { code: 500, msg: e.message };
  }
}

async function delDrivers(ctx: BusinessEventContext, evtid: string): Promise<any> {
  const db: PGClient = ctx.db;
  const cache: RedisClient = ctx.cache;
  try {
    const dbrep = await db.query("SELECT id, oid, uid, data, occurred_at FROM order_events WHERE id = $1", [evtid]);
    const result = dbrep["rows"][0];
    const oid = result["oid"];
    const dids = result["data"]["dids"];
    const updated_at = new Date();
    await db.query("BEGIN");
    for (const did of dids) {
      const id = uuid.v1();
      await db.query("DELETE FROM drivers WHERE pid = $3", [did]);
    }
    await async_plan_orders(db, cache, ctx.domain, ctx.uid, result["oid"]);
    await db.query("COMMIT");
  } catch (e) {
    log.info(e);
    await db.query("ROLLBACK");
    return { code: 500, msg: e.message };
  }
}

async function cancel(ctx: BusinessEventContext, evtid: string): Promise<any> {
  log.info("cancel");
  const db: PGClient = ctx.db;
  const cache: RedisClient = ctx.cache;
  try {
    const dbrep = await db.query("SELECT id, oid, uid, data, occurred_at FROM order_events WHERE id = $1", [evtid]);
    const result = dbrep["rows"][0];
    const last_state = result["data"]["last_state"];
    let state = null;
    let state_description = null;
    if (last_state === 1) {
      state = 7;
      state_description = "已取消";
    } else if (last_state === 2) {
      state = 8;
      state_description = "支付后取消";
    } else if (last_state === 3) {
      state = 9
      state_description = "核保后取消";
    } else if (last_state === 4) {
      state = 6;
      state_description = "已失效";
    }
    const oid = result["oid"];
    const time = new Date();
    await db.query("BEGIN");
    await db.query("UPDATE plan_orders SET state = $1, state_description = $2, deleted = $3, updated_at = $4 WHERE id = $5", [state, state_description, true, time, result["oid"]]);
    await db.query("COMMIT");
    const oJson = await cache.hgetAsync("order-entities", result["oid"]);
    const order_entities = await msgpack_decode(oJson);
    const vid = order_entities["vehicle"]["id"];
    const uid = result["uid"];
    const no = order_entities["no"];
    const qid = order_entities["qid"];
    const multi = bluebird.promisifyAll(cache.multi()) as Multi;
    multi.hdelAsync("vid-poid", vid);
    multi.zremAsync("plan-orders", oid);
    multi.zremAsync("orders", oid);
    multi.zremAsync("orders-" + uid, oid);
    multi.zremAsync("orders-" + vid, oid);
    multi.hdelAsync("orderNo-id", no);
    multi.hdelAsync("qid-oid", qid);
    multi.hdelAsync("order-entities", oid);
    await multi.execAsync();
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
    await db.query("UPDATE plan_orders SET state = $1,state_description = $2, updated_at = $3 WHERE id = $4", [state, state_description, time, result["oid"]]);
    await db.query("COMMIT");
    await async_plan_orders(db, cache, ctx.domain, ctx.uid, result["oid"]);
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
    await db.query("UPDATE plan_orders SET state = $1,state_description = $2, updated_at = $3 WHERE id = $4", [state, state_description, time, result["oid"]]);
    await db.query("COMMIT");
    await async_plan_orders(db, cache, ctx.domain, ctx.uid, result["oid"]);
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
    const dbrep_byTime = await db.query("SELECT id, oid, uid, data, occurred_at FROM plan_orders WHERE id = $1 ORDER BY occurred_at", [result["oid"]]);
    const events = [];
    for (const row of dbrep_byTime["rows"]) {
      const event = {
        id: row["id"],
        oid: row["oid"],
        uid: row["uid"],
      };
      events.push(event);
    }
    const len = events.length;
    const last_state = events[len - 1]["last_state"];
    const state = last_state;
    let state_description = null;
    if (last_state !== 2 || last_state !== 3 || last_state !== 4) {
      return { code: 501, msg: "该订单上一状态异常" };
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
      await db.query("UPDATE plan_orders SET state = $1,state_description = $2, updated_at = $3 WHERE id = $4", [state, state_description, time, result["oid"]]);
      await db.query("COMMIT");
      await async_plan_orders(db, cache, ctx.domain, ctx.uid, result["oid"]);
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
    await db.query("UPDATE plan_orders SET state = $1,state_description = $2, updated_at = $3 WHERE id = $4", [state, state_description, time, result["oid"]]);
    await db.query("COMMIT");
    await async_plan_orders(db, cache, ctx.domain, ctx.uid, result["oid"]);
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
    const state_description = "已到期";
    const time = new Date();
    await db.query("BEGIN");
    await db.query("UPDATE plan_orders SET state = $1,state_description = $2, updated_at = $3 WHERE id = $4", [state, state_description, time, result["oid"]]);
    await db.query("COMMIT");
    await async_plan_orders(db, cache, ctx.domain, ctx.uid, result["oid"]);
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
    await db.query("UPDATE plan_orders SET state = $1,state_description = $2, updated_at = $3 WHERE id = $4", [state, state_description, time, result["oid"]]);
    await db.query("COMMIT");
    await async_plan_orders(db, cache, ctx.domain, ctx.uid, result["oid"]);
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
    await db.query("UPDATE plan_orders SET state = $1, state_description = $2, updated_at = $3, start_at =$4, stop_at = $5 WHERE id = $6", [state, state_description, time, data["start_at"], data["stop_at"], result["oid"]]);
    await db.query("COMMIT");
    await async_plan_orders(db, cache, ctx.domain, result["uid"], result["oid"]);
    return { code: 200, data: result["oid"] };
  } catch (e) {
    log.info(e);
    await db.query("ROLLBACK");
    throw e;
  }
}


async function pay(ctx: BusinessEventContext, evtid: string, no: string): Promise<any> {
  log.info("pay")
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
    await db.query("UPDATE plan_orders SET state = $1, state_description = $2, paid_at= $3, updated_at = $4 WHERE id = $5", [state, state_description, paid_at, paid_at, result["oid"]]);
    await db.query("COMMIT");
    const wrep = await rpcAsync<Object>(ctx.domain, process.env["WALLET"], result["uid"], "recharge", result["oid"]);
    if (wrep["code"] === 200) {
      await async_plan_orders(db, cache, ctx.domain, result["uid"], result["oid"]);
      const prep = rpcAsync<Object>(ctx.domain, process.env["PERSON"], result["uid"], "setPersonVerified", data["identity_no"], true);
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
    } else {
      log.info("wrep" + JSON.stringify(wrep));
      return { code: wrep["code"], msg: "更新钱包数据失败: " + wrep["msg"] };
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
    const strNo = await ctx.cache.incrAsync("order-no");
    const new_no = order_no.substring(0, 21);
    const strno = String(strNo);
    const no: string = formatNum(strno, 7);
    const new_order_no = new_no + no;
    log.info("new_order_no" + new_order_no);
    const updated_at = new Date();
    await db.query("UPDATE plan_orders SET no = $1,updated_at = $2 WHERE id = $3", [new_order_no, updated_at, result["oid"]]);
    await ctx.cache.hdelAsync("orderNo-id", order_no);
    await async_plan_orders(db, cache, ctx.domain, ctx.uid, result["oid"]);
    return { code: 200, data: new_order_no };
  } catch (e) {
    log.info(e);
    throw e;
  }
}



async function create_plan_order(ctx: BusinessEventContext, evtid: string): Promise<any> {
  log.info("create_plan_order");
  const db: PGClient = ctx.db;
  const cache: RedisClient = ctx.cache;
  try {
    const start_at = new Date();
    const dbrep = await db.query("SELECT id, oid, uid, data, occurred_at FROM order_events WHERE id = $1", [evtid]);
    const result = dbrep["rows"][0];
    const data = result["data"];
    await db.query("BEGIN");
    await db.query("INSERT INTO plan_orders(id, no, uid, qid, vid, state, state_description, summary, payment,owner, insured, promotion, service_ratio, real_value, ticket, recommend, expect_at, created_at, updated_at, evtid) VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10 ,$11, $12, $13,$14, $15, $16, $17, $18, $19, $20)", [data["oid"], data["no"], result["uid"], data["qid"], data["vid"], 1, "已创建", data["summary"], data["payment"], data["owner"], data["insured"], data["promotion"], 0.2, data["real_value"], data["ticket"], data["recommend"], data["expect_at"], start_at, start_at, evtid]);
    const prep = await rpcAsync<Object>(ctx.domain, process.env["PLAN"], ctx.uid, "getPlans", ctx.uid);
    if (prep["code"] === 200) {
      const new_plans = prep["data"];
      for (const plan in data["plans"]) {
        for (const p of new_plans) {
          if (String(p["id"]) === plan) {
            const title = p["title"];
            const id = uuid.v4();
            await db.query("INSERT INTO plan_order_items(id, oid, pid, title,amount, price) VALUES($1, $2, $3, $4, $5, $6)", [id, data["oid"], plan, title, data["amount"], data["plans"][plan]]);
          }
        }
      }
      await async_plan_orders(db, cache, ctx.domain, result["uid"], data["oid"]);
      // 分销系统消息
      await db.query("COMMIT");
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
    const vrep = await rpcAsync<Object>(ctx["domain"], process.env["VEHICLE"], result["uid"], "getVehicle", data["vid"]);
    let pid = null;
    if (vrep["code"] === 200) {
      pid = vrep["data"]["insured"]["id"];
    }
    await db.query("BEGIN");
    await db.query("INSERT INTO driver_orders(id, no, uid, vid, state, state_description, summary, payment, insured, paid_at, created_at, updated_at, evtid) VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)", [data["oid"], data["no"], result["uid"], data["vid"], 2, "已支付", data["summary"], data["payment"], pid, start_at, start_at, start_at, evtid]);

    for (const did of data["pids"]) {
      const id = uuid.v4();
      await db.query("INSERT INTO driver_order_items(id, oid, pid) VALUES($1, $2, $3)", [id, data["oid"], did]);
    }
    await db.query("COMMIT");
    async_driver_orders(db, cache, ctx.domain, result["uid"], data["oid"]);
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
    const vrep = await rpcAsync<Object>(ctx["domain"], process.env["VEHICLE"], result["uid"], "getVehicle", data["vid"]);
    let pid = null;
    if (vrep["code"] === 200) {
      pid = vrep["data"]["insured"]["id"];
    }
    await db.query("BEGIN");
    await db.query("INSERT INTO sale_orders(id, no, uid, vid, type, state, state_description, summary, payment, insured, created_at, updated_at, evtid) VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)", [data["oid"], data["no"], result["uid"], data["vid"], data["type"], 1, "已创建", data["summary"], data["payment"], pid, start_at, start_at, data["evtid"]]);
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
  const result: QueryResult = await db.query("SELECT o.id AS o_id, o.no AS o_no, o.uid AS o_uid, o.qid AS o_qid, o.vid AS o_vid, o.state AS o_state, o.state_description AS o_state_description, o.summary AS o_summary, o.payment AS o_payment, o.owner AS o_owner, o.insured AS o_insured, o.promotion AS o_promotion, o.service_ratio AS o_service_ratio, o.real_value AS o_real_value, o.ticket AS o_ticket, o.recommend AS o_recommend, o.expect_at AS o_expect_at, o.start_at AS o_start_at, o.stop_at AS o_stop_at, o.paid_at AS o_paid_at,o.driving_frontal_view AS o_driving_frontal_view, o.driving_rear_view AS o_driving_rear_view, o.created_at AS o_created_at, o.updated_at AS o_updated_at, o.evtid AS o_evtid, oi.id AS oi_id, oi.pid AS oi_pid, oi.price AS oi_price, oi.amount AS oi_amount FROM plan_order_items AS oi INNER JOIN plan_orders AS o ON o.id = oi.oid WHERE o.deleted = FALSE" + (oid ? " AND o.id = $1" : ""), oid ? [oid] : []);
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
          ticket: trim(row["o_ticket"]),
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
      //　获取drivers
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
      const newOrder = await msgpack_encode(order);
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

async function async_driver_orders(db: PGClient, cache: RedisClient, domain: string, uid: string, oid?: string): Promise<void> {
  const result = await db.query("SELECT o.id AS o_id, o.no AS o_no, o.uid AS o_uid, o.vid AS o_vid, o.state AS o_state, o.state_description AS o_state_description,o.summary AS o_summary, o.payment AS o_payment, o.insured AS o_insured, o.paid_at AS o_paid_at, o.start_at AS o_start_at, o.stop_at AS o_stop_at, o.created_at AS o_created_at, o.updated_at AS o_updated_at, o.evtid AS o_evtid oi.id AS oi_id, oi.oid AS oi_oid, oi.pid AS oi_pid FROM driver_order_items AS oi LEFT JOIN orders AS o ON o.id = oi.oid WHERE o.deleted = FALSE AND oi.deleted = FALSE" + (oid ? " AND o.id = $1" : ""), oid ? [oid] : []);
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
          insured: row["o_insured"],
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
      const vrep = await rpcAsync<Object>(domain, process.env["VEHICLE"], uid, "getVehicle", orders[oid]["vid"]);
      if (vrep["code"] === 200) {
        orders[oid]["vehicle"] = vrep["data"];
        orders[oid]["insured"] = vrep["data"]["insured"];
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
    const result = await db.query("SELECT id, no, uid, vid, type, state, state_description, summary, payment, insured, paid_at, start_at, stop_at, created_at, updated_at, evtid FROM sale_orders WHERE deleted = FALSE" + (oid ? " AND o.id = $1" : ""), oid ? [oid] : []);
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
        insured: row["insured"],
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
      const vrep = await rpcAsync<Object>(domain, process.env["VEHICLE"], uid, "getVehicle", orders[oid]["vid"]);
      if (vrep["code"] === 200) {
        order["vehicle"] = vrep["data"];
        order["insured"] = vrep["data"]["insured"];
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
    const multi = bluebird.promisifyAll(cache.multi()) as Multi;
    multi.del("vid-poid");
    multi.del("plan-orders");
    multi.del("orders");
    multi.del("orderNo-id");
    multi.del("qid-oid");
    multi.del("order-entities");
    await multi.execAsync();
    await async_plan_orders(db, cache, domain, uid);
    return { code: 200, data: "success" };
  } catch (e) {
    log.info(e);
    return { code: 500, msg: e.message };
  }
}