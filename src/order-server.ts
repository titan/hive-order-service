import { Server, ServerContext, rpc, AsyncServerFunction, CmdPacket, Permission, waitingAsync, msgpack_decode, msgpack_encode } from "hive-service";
import { RedisClient, Multi } from "redis";
import * as bunyan from "bunyan";
import * as uuid from "node-uuid";
import * as http from "http";
import { verify, uuidVerifier, stringVerifier, numberVerifier } from "hive-verify";
import * as Disq from "hive-disque";
import * as bluebird from "bluebird";

const allowAll: Permission[] = [["mobile", true], ["admin", true]];
const mobileOnly: Permission[] = [["mobile", true], ["admin", false]];
const adminOnly: Permission[] = [["mobile", false], ["admin", true]];

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

server.callAsync("createPlanOrder", allowAll, "创建订单", "用户提交订单时创建", async (ctx: ServerContext, vid: string, plans: Object, qid: string, pm_price: number, service_ratio: number, summary: number, payment: number, v_value: number, recommend: string, ticket: string) => {
  log.info(`createPlanOrder, uid: ${ctx.uid}, vid: ${vid},plans: ${JSON.stringify(plans)}, qid: ${qid},pm_price: ${pm_price}, service_ratio: ${service_ratio}, summary: ${summary}, payment: ${payment}`);
  try {
    verify([uuidVerifier("vid", vid), uuidVerifier("qid", qid), numberVerifier("service_ratio", service_ratio), numberVerifier("summary", summary), numberVerifier("payment", payment), numberVerifier("v_value", v_value)]);
  } catch (e) {
    return { code: 400, msg: e.message };
  }
  try {
    const oid_buffer = await ctx.cache.hgetAsync("vid-poid", vid);
    const oid = String(oid_buffer);
    if (oid === null || oid === "") {
      const args = { type: 1, order_type: 1, vid: vid, plans: plans, qid: qid, pm_price: pm_price, service_ratio: service_ratio, payment: payment, summary: summary, v_value: v_value, recommend: recommend, ticket: ticket };
      ctx.push("order-events-disque", args);
      return waitingAsync(ctx);
    } else {
      const orderJson = await ctx.cache.hgetAsync("order-entities", oid);
      const order_entities = await msgpack_decode(orderJson);
      if (order_entities["state_code"] === 1 || order_entities["state_code"] === 2 || order_entities["state_code"] === 3) {
        return { code: 500, msg: "同一个车不能重复下单" };
      } else if (order_entities["state_code"] === 4) {
        const now_time = new Date().getTime();
        const stop_time = new Date(order_entities["stop_at"]).getTime();
        const time = stop_time - now_time;
        if (time > 30 * 24 * 60 * 60 * 1000) {
          return { code: 500, msg: "订单距失效时间超过三个月同一辆车不允许下重复下单" };
        }
      } else {
        const args = { type: 1, order_type: 1, vid: vid, plans: plans, qid: qid, pm_price: pm_price, service_ratio: service_ratio, payment: payment, summary: summary, v_value: v_value, recommend: recommend, ticket: ticket };
        ctx.push("order-events-disque", args);
        return waitingAsync(ctx);
      }
    }
  } catch (e) {
    log.info("createPlanOrder catch ERROR" + e);
    throw { code: 500, msg: e.message };
  }
});


server.callAsync("createDriverOrder", allowAll, "用户下司机订单", "用户下司机订单", async (ctx: ServerContext, vid: string, dids: any, summary: number, payment: number) => {
  log.info(`createDriverOrder, uid:${ctx.uid}, vid: ${vid}, dids: ${JSON.stringify(dids)}, summary: ${summary}, payment: ${payment}`);
  try {
    verify([uuidVerifier("vid", vid), numberVerifier("summary", summary), numberVerifier("payment", payment)]);
  } catch (e) {
    return { code: 400, msg: e.message };
  }
  try {
    let drivers = null;
    const uid = ctx.uid;
    const domain = ctx.domain;
    const len = dids.length;
    const drep = await ctx.cache.hgetAsync("order-driver-entities", vid);
    if (drep === null || drep === "") {
      drivers = [];
    } else {
      drivers = await msgpack_decode(drep);
    }
    const tlen = drivers.length + len;
    if (tlen > 3) {
      return { code: 500, msg: "添加司机不能超过三位" };
    } else {
      const args = { type: 1, order_type: 2, vid: vid, dids: dids, payment: payment, summary: summary };
      ctx.push("order-events-disque", args);
      return waitingAsync(ctx);
    }
  } catch (e) {
    log.info("createDriverOrder catch ERROR" + e);
    throw { code: 500, msg: e.message };
  }
});

server.callAsync("createSaleOrder", allowAll, "下第三方单", "下第三方单", async (ctx: ServerContext, vid: string, pid: string, qid: string, items: any, summary: number, payment: number, opr_level: number) => {
  log.info(`createSaleOrder, uid: ${ctx.uid}, vid:${vid}, pid: ${pid}, qid: ${qid},items: ${JSON.stringify(items)}, summary: ${summary}, payment: ${payment}, opr_level: ${opr_level}`);
  if (!verify([uuidVerifier("vid", vid), uuidVerifier("qid", qid)], (errors: string[]) => {
    log.info("arg not match" + errors);
    throw { code: 400, msg: errors.join("\n") };
  })) {
    return;
  }
  try {
    const uid = ctx.uid;
    const order_id = uuid.v1();
    const callback = order_id;
    const domain = ctx.domain;
    const args = { type: 1, order_type: 3, domain: ctx.domain, uid: ctx.uid, vid: vid, pid: pid, qid: qid, items: items, summary: summary, payment: payment, opr_level: opr_level };
    ctx.push("order-events-disque", args);
    return waitingAsync(ctx);
  } catch (e) {
    log.info("createSaleOrder catch ERROR" + e.message);
    throw { code: 500, msg: e.message };
  }
});

server.callAsync("renameNo", allowAll, "更新订单编号", "更新订单编号", async (ctx: ServerContext, order_no: string) => {
  log.info(`renameNo, uid: ${ctx.uid}, order_no: ${order_no}`);
  try {
    verify([stringVerifier("order_no", order_no)]);
  } catch (e) {
    return { code: 400, msg: e.message };
  }
  const args = { order_no: order_no };
  ctx.push("order-events-disque", args);
  return waitingAsync(ctx);
});

server.callAsync("refund", allowAll, "银行退款", "更改订单对应状态", async (ctx: ServerContext, order_id: string) => {
  log.info(`refund, uid: ${ctx.uid}, order_id: ${order_id}`);
  try {
    verify([stringVerifier("order_id", order_id)]);
  } catch (e) {
    return { code: 400, msg: e.message };
  }
  const args = { type: 9, order_id: order_id };
  ctx.push("order-events-disque", args);
  return waitingAsync(ctx);
});

server.callAsync("agreeWithdraw", allowAll, "同意提现申请", "更改订单状态", async (ctx: ServerContext, order_id: string) => {
  log.info(`agreeWithdraw, uid:${ctx.uid},order_id: ${order_id}`);
  try {
    verify([stringVerifier("order_id", order_id)]);
  } catch (e) {
    return { code: 400, msg: e.message };
  }
  const args = { type: 8, order_id: order_id };
  ctx.push("order-events-disque", args);
  return waitingAsync(ctx);
});

server.callAsync("refuseWithdraw", allowAll, "拒绝提现申请", "拒绝后更改订单状态", async (ctx: ServerContext, order_id: string, reason: string) => {
  log.info(`refuseWithdraw, uid: ${ctx.uid}, order_id: ${order_id}, reason: ${reason}`);
  try {
    verify([stringVerifier("order_id", order_id)]);
  } catch (e) {
    return { code: 400, msg: e.message };
  }
  const args = { type: 7, order_id: order_id, reason: reason };
  ctx.push("order-events-disque", args);
  return waitingAsync(ctx);
});

server.callAsync("applyWithdraw", allowAll, "申请提现", "申请提现时更改订单状态", async (ctx: ServerContext, order_id: string) => {
  log.info(`applyWithdraw, uid:${ctx.uid}, order_id: ${order_id}`);
  try {
    verify([stringVerifier("order_id", order_id)]);
  } catch (e) {
    log.info(e);
    return { code: 400, msg: e.message };
  }
  const args = { type: 6, order_id: order_id };
  ctx.push("order-events-disque", args);
  return waitingAsync(ctx);
});

server.callAsync("expire", allowAll, "订单到期", "对到期订单处理", async (ctx: ServerContext, order_id: string) => {
  log.info(`expire, uid:${ctx.uid}, order_id: ${order_id}`);
  try {
    verify([stringVerifier("order_id", order_id)]);
  } catch (e) {
    log.info(e);
    return { code: 400, msg: e.message };
  }
  const args = { type: 5, order_id: order_id };
  ctx.push("order-events-disque", args);
  return waitingAsync(ctx);
});

server.callAsync("takeEffect", allowAll, "订单生效", "对生效订单进行处理", async (ctx: ServerContext, order_id: string) => {
  log.info(`takeEffect, uid:${ctx.uid}, order_id: ${order_id}`);
  try {
    verify([stringVerifier("order_id", order_id)]);
  } catch (e) {
    log.info(e);
    return { code: 400, msg: e.message };
  }
  const args = { type: 4, order_id: order_id };
  ctx.push("order-events-disque", args);
  return waitingAsync(ctx);
});


server.callAsync("underwrite", allowAll, "订单核保", "对核保状态下订单进行处理", async (ctx: ServerContext, order_id: string, start_at: Date, stop_at: Date) => {
  log.info(`underwrite, uid:${ctx.uid}, order_id: ${order_id},start_at: ${start_at}, stop_at: ${stop_at}`);
  try {
    verify([stringVerifier("order_id", order_id)]);
  } catch (e) {
    log.info(e);
    return { code: 400, msg: e.message };
  }
  const args = { type: 3, order_id: order_id, start_at, stop_at };
  ctx.push("order-events-disque", args);
  return waitingAsync(ctx);
});

server.callAsync("pay", allowAll, "用户支付订单", "更改订单支付状态", async (ctx: ServerContext, uid: string, order_id: string, amount: number) => {
  log.info(`underwrite, uid:${ctx.uid}, order_id: ${order_id}`);
  try {
    verify([stringVerifier("order_id", order_id)]);
  } catch (e) {
    log.info(e);
    return { code: 400, msg: e.message };
  }
  const args = { type: 2, order_id: order_id, amount: amount };
  ctx.push("order-events-disque", args);
  return waitingAsync(ctx);
});


server.callAsync("cancel", adminOnly, "取消订单", "删除订单数据", async (ctx: ServerContext, order_id: string) => {
  log.info(`underwrite, uid:${ctx.uid}, order_id: ${order_id}`);
  try {
    verify([stringVerifier("order_id", order_id)]);
  } catch (e) {
    log.info(e);
    return { code: 400, msg: e.message };
  }
  const args = { type: 0, order_id: order_id };
  ctx.push("order-events-disque", args);
  return waitingAsync(ctx);
});



server.callAsync("getOrder", allowAll, "获取订单详情", "获得订单详情", async (ctx: ServerContext, oid: string) => {
  log.info(`getOrder, uid: ${ctx.uid}, oid: ${oid}`);
  if (!verify([uuidVerifier("oid", oid)], (errors: string[]) => {
    throw { code: 400, msg: errors.join("\n") };
  })) {
    return;
  }
  try {
    const orep = await ctx.cache.hgetAsync("order-entities", oid);
    if (orep === null || orep === "") {
      return { code: 404, msg: "未找到对应订单信息" };
    } else {
      const order_entities = await msgpack_decode(orep);
      const nowDate = (new Date()).getTime();
      return { code: 200, data: order_entities, nowDate: nowDate };
    }
  } catch (e) {
    log.info("getOrder catch ERROR" + e);
    throw { code: 500, msg: e.message };
  }
});


server.callAsync("getOrders", allowAll, "获取订单列表", "获得一个用户的所有订单", async (ctx: ServerContext, offset: number, limit: number) => {
  log.info(`getOrders, uid: ctx:${ctx.uid}, offset: ${offset}, limit: ${limit}`);
  if (!verify([numberVerifier("offset", offset), numberVerifier("limit", limit)], (errors: string[]) => {
    throw { code: 400, msg: errors.join("\n") };
  })) {
    return;
  }
  try {
    const oids = await ctx.cache.zrangeAsync(`orders-${ctx.uid}`, offset, limit);
    if (oids !== null && oids.length !== 0) {
      const multi = bluebird.promisifyAll(ctx.cache.multi()) as Multi;
      for (const oid of oids) {
        multi.hget("order_entities", String(oid));
      }
      const orders: Object[] = [];
      const nowDate = (new Date()).getTime();
      const oreps = await multi.execAsync();
      for (const pkt of oreps) {
        if (pkt !== null) {
          const order = await msgpack_decode(pkt);
          orders.push(order);
        }
      }
      return { code: 200, data: orders, nowDate: nowDate };
    } else {
      return { code: 404, msg: "未找到对应订单信息" };
    }
  } catch (e) {
    log.info("getOrders catch ERROR" + e.message);
    throw { code: 500, msg: e.message };
  }
});

server.callAsync("getPlanOrderByVehicle", allowAll, "获取计划单", "根据ｖｉｄ获取计划订单", async (ctx: ServerContext, vid) => {
  log.info(`getPlanOrderByVehicle, uid: ${ctx.uid}, vid: ${vid}`);
  if (!verify([uuidVerifier("vid", vid)], (errors: string[]) => {
    throw { code: 400, msg: errors.join("\n") };
  })) {
    return;
  }
  try {
    const poid = await ctx.cache.hgetAsync("vid-poid", vid);
    if (poid === null || poid === "") {
      return { code: 404, msg: "未找到对应订单信息" };
    } else {
      const orep = await ctx.cache.hgetAsync("order-entities", poid);
      const order_entities = await msgpack_decode(orep);
      return { code: 200, data: order_entities };
    }
  } catch (e) {
    log.info("getPlanOrderByVehicle catch ERROR" + e);
    throw { code: 500, msg: e.message };
  }
});

// 通过vid获取司机单
server.callAsync("getDriverOrderByVehicle", allowAll, "通过vid获取司机单", "通过vid获取司机单", async (ctx: ServerContext, vid: string) => {
  log.info(`getDriverOrderByVehicle, uid: ${ctx.uid}, vid: ${vid}`);
  if (!verify([uuidVerifier("vid", vid)], (errors: string[]) => {
    throw { code: 400, msg: errors.join("\n") };
  })) {
    return;
  }
  try {
    const dorep = await ctx.cache.hgetAsync("order-driver-entities", vid);
    if (dorep === null || dorep === "") {
      return { code: 404, msg: "未找到对应驾驶人" };
    } else {
      const drivers = await msgpack_decode(dorep);
      return { code: 200, data: drivers };
    }
  } catch (e) {
    log.info("getDriverOrderByVehicle catch ERROR" + e);
    throw { code: 500, msg: e.message };
  }
});

server.callAsync("getOrderByQid", allowAll, "获取订单详情", "根据ｑｉｄ获取订单详情", async (ctx: ServerContext, qid: string) => {
  log.info(`getOrderByQid, uid: ${ctx.uid}, qid: ${qid}`);
  if (!verify([uuidVerifier("qid", qid)], (errors: string[]) => {
    throw { code: 400, msg: errors.join("\n") };
  })) {
    return;
  }
  const oid = await ctx.cache.hgetAsync("qid-oid", qid);
  if (oid === null || oid === "") {
    return { code: 404, msg: "未找到对应订单信息" };
  } else {
    const orep = await ctx.cache.hgetAsync("order-entities", String(oid));
    if (orep === null || orep === "") {
      return { code: 404, msg: "未找到对应订单信息" };
    } else {
      const order_entities = await msgpack_decode(orep);
      return { code: 200, data: order_entities };
    }
  }
});


server.callAsync("getOrdersByVid", allowAll, "获取车辆对应所有订单", "获取车辆对应所有订单", async (ctx: ServerContext, vid: string) => {
  log.info(`getOrdersByVid, uid: ${ctx.uid}, vid: ${vid}`);
  if (!verify([uuidVerifier("vid", vid)], (errors: string[]) => {
    throw { code: 400, msg: errors.join("\n") };
  })) {
    return;
  }
  try {
    const oids = await ctx.cache.zrangeAsync(`orders-${vid}`, 0, -1);
    if (oids === null || oids === "") {
      return { code: 404, msg: "未找到对应订单信息" };
    } else {
      const multi = bluebird.promisifyAll(ctx.cache.multi()) as Multi;
      for (const oid of oids) {
        multi.hget("order-entities", String(oid));
      }
      const oreps = await multi.execAsync();
      const orders = [];
      for (const orep of oreps) {
        const o = await msgpack_decode(orep);
        if (o !== null) {
          orders.push(o);
        }
      }
      return { code: 200, data: orders };
    }
  } catch (e) {
    log.info("getOrdersByVid catch ERROR" + e);
    throw { code: 500, msg: e.message };
  }
});



server.call("refresh", allowAll, "refresh", "刷新所有订单数据", (ctx: ServerContext, rep: ((result: any) => void)) => {
  log.info("refresh");
  const domain = ctx.domain;
  const pkt: CmdPacket = { cmd: "refresh", args: [domain] };
  rep({ code: 200, msg: "success" });
});

server.callAsync("refresh", adminOnly, "refresh", "刷新订单数据", async (ctx: ServerContext, order_id?: string) => {
  log.info(`refresh, order_id: ${order_id}`);
  if (order_id) {
    const oJson = await ctx.cache.hgetAsync("order-entities", order_id);
    const order_entities = await msgpack_decode(oJson);
    const state = order_entities["state"];
    if (state === 1) {
      const args = { type: 11, order_id: order_id };
      ctx.push("order-events-disque", args);
      return waitingAsync(ctx);
    } else if (state === 2) {
      const args = { type: 12, order_id: order_id };
      ctx.push("order-events-disque", args);
      return waitingAsync(ctx);
    } else if (state === 3) {
      const args = { type: 13, order_id: order_id };
      ctx.push("order-events-disque", args);
      return waitingAsync(ctx);
    }
  } else {
    const args = { type: 14 };
    ctx.push("order-events-disque", args);
    return waitingAsync(ctx);
  }
});
