import { Server, ServerContext, rpcAsync, AsyncServerFunction, CmdPacket, wait_for_response, Permission, waitingAsync, msgpack_decode_async as msgpack_decode, msgpack_encode_async as msgpack_encode } from "hive-service";
import { RedisClient, Multi } from "redis";
import * as bunyan from "bunyan";
import * as uuid from "uuid";
import * as http from "http";
import { verify, uuidVerifier, stringVerifier, numberVerifier, dateVerifier, arrayVerifier, objectVerifier } from "hive-verify";
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

async function checkOrderEffectTime(orders): Promise<any> {
  try {
    let length = 0;
    for (const order of orders) {
      if (order["state"] === 4) {
        const now_time = new Date().getTime();
        const stop_time = new Date(order["stop_at"]).getTime();
        const time = stop_time - now_time;
        if (time < 3 * 30 * 24 * 60 * 60 * 1000) {
          length += 1;
        }
      }
    }
    return length;
  } catch (e) {
    log.info(e);
    throw e;
  }
}

async function checkOrderLimit(domain: string, uid: string, vid: string, owner: string, insured: string, cache: RedisClient): Promise<any> {
  try {
    const mrep = await rpcAsync<Object>(domain, process.env["MOBILE"], uid, "getOwnerShip", vid);
    if (mrep.code === 200) {
      const owner_id = mrep.data["owner"];
      if (owner_id === owner) {
        const oids = await cache.zrangeAsync(`orders-${uid}`, 0, -1);
        log.info("oid" + oids);
        if (oids !== null && oids.length !== 0) {
          const multi = bluebird.promisifyAll(cache.multi()) as Multi;
          for (const oid of oids) {
            multi.hget("order-entities", String(oid));
          }
          const result: Object[] = [];
          const oreps = await multi.execAsync();
          for (const pkt of oreps) {
            if (pkt !== null) {
              const order = await msgpack_decode(pkt);
              result.push(order);
            }
          }
          const wait_pay_orders = result.filter(o => o["state"] === 1);
          if (wait_pay_orders.length === 0) {
            const orders = result.filter(o => o["state"] !== 1 && o["state"] !== 5 && o["state"] !== 6 && o["state"] !== 7 && o["state"] !== 8 && o["state"] !== 9);
            let max_orders = 2;
            const vrep = await rpcAsync<Object>(domain, process.env["PROFILE"], uid, "getUser");
            if (vrep["code"] === 200) {
              max_orders = parseInt(vrep["data"]["max_orders"] || 2);
            };
            const len = orders.length;
            if (len < max_orders) {
              return { code: 200, data: "OK" };
            } else {
              const orderEffect = await checkOrderEffectTime(orders);
              if (len - orderEffect < max_orders) {
                return { code: 200, data: "OK" };
              } else {
                return { code: 501, msg: "该车主参加的互助车辆已达上限，不可继续添加" };
              }
            }
          } else {
            return { code: 501, msg: "您有尚未完成的订单，请先处理该订单" };
          }
        } else {
          return { code: 200, data: "ok" };
        }
      } else {
        log.info("该车绑定车主与下单车主不一致");
        return { code: 501, msg: "车主信息异常" };
      }
    } else {
      log.info("该车未建立绑定关系");
      return { code: 501, msg: "用户信息异常" };
    }
  } catch (e) {
    log.info(e);
    throw e;
  }
}



server.callAsync("createPlanOrder", allowAll, "创建订单", "用户提交订单时创建", async (ctx: ServerContext, qid: string, vid: string, owner: string, insured: string, plans: Object, expect_at: Date) => {
  log.info(`createPlanOrder, uid: ${ctx.uid}, vid: ${vid},plans: ${JSON.stringify(plans)}, qid: ${qid}, expect_at :${expect_at}},owner: ${owner},insured: ${insured}`);
  try {
    await verify([uuidVerifier("vid", vid), uuidVerifier("qid", qid), uuidVerifier("owner", owner), uuidVerifier("insured", insured), objectVerifier("plans", plans)]);
  } catch (e) {
    return { code: 400, msg: e.message };
  }
  try {
    const urep = await rpcAsync<Object>(ctx.domain, process.env["PROFILE"], ctx.uid, "getUser");
    if (urep["code"] === 200) {
      const inviter = urep["data"]["inviter"];
      if (insured === owner) {
        const oid = await ctx.cache.hgetAsync("vid-poid", vid);
        const new_expect_at = new Date(new Date(expect_at).getTime() - 8 * 60 * 60 * 1000);
        if (oid === null || oid === "") {
          const result = await checkOrderLimit(ctx.domain, ctx.uid, vid, owner, insured, ctx.cache);
          if (result["code"] === 200) {
            const args = { type: 1, order_type: 1, vid: vid, inviter: inviter, owner: owner, insured: insured, plans: plans, qid: qid, expect_at: new_expect_at };
            ctx.push("order-events-disque", args);
            return await waitingAsync(ctx);
          } else {
            log.info("checkOrderLimit" + JSON.stringify(result));
            return { code: result["code"], msg: result["msg"] };
          }
        } else {
          const orderJson = await ctx.cache.hgetAsync("order-entities", oid);
          const order_entities = await msgpack_decode(orderJson);
          if (order_entities["state"] === 2 || order_entities["state"] === 3) {
            return { code: 501, msg: "该车有计划生效订单，若要重新提交订单，请取消该订单" };
          } else if (order_entities["state"] === 4) {
            const now_time = new Date().getTime();
            const stop_time = new Date(order_entities["stop_at"]).getTime();
            const time = stop_time - now_time;
            if (time > 90 * 24 * 60 * 60 * 1000) {
              return { code: 501, msg: "订单距失效时间超过三个月同一辆车不允许下重复下单" };
            }
          } else if (order_entities["state"] === 1) {
            return { code: 501, msg: "该车有尚未完成订单，请先处理该订单" };
          } else {
            const result = await checkOrderLimit(ctx.domain, ctx.uid, vid, owner, insured, ctx.cache);
            if (result["code"] === 200) {
              const args = { type: 1, order_type: 1, vid: vid, inviter: inviter, owner: owner, insured: insured, plans: plans, qid: qid, expect_at: new_expect_at };
              ctx.push("order-events-disque", args);
              return await waitingAsync(ctx);
            } else {
              log.info("checkOrderLimit" + JSON.stringify(result));
              return result;
            }
          }
        }
      } else {
        log.info("投保人和车主信息异常");
        return { code: 500, msg: "用户信息异常" };
      }
    } else {
      log.info("urep" + JSON.stringify(urep));
      return { code: urep["code"], msg: "获取用户信息失败" };
    }

  } catch (e) {
    log.info("createPlanOrder catch ERROR" + e);
    return { code: 500, msg: e };
  }
});


server.callAsync("createDriverOrder", allowAll, "用户下司机订单", "用户下司机订单", async (ctx: ServerContext, vid: string, dids: any, summary: number, payment: number) => {
  log.info(`createDriverOrder, uid:${ctx.uid}, vid: ${vid}, dids: ${JSON.stringify(dids)}, summary: ${summary}, payment: ${payment}`);
  try {
    await verify([uuidVerifier("vid", vid), numberVerifier("summary", summary), numberVerifier("payment", payment)]);
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
      return await waitingAsync(ctx);
    }
  } catch (e) {
    log.info("createDriverOrder catch ERROR" + e);
    throw { code: 500, msg: e.message };
  }
});

server.callAsync("createSaleOrder", allowAll, "下第三方单", "下第三方单", async (ctx: ServerContext, vid: string, pid: string, qid: string, items: any, summary: number, payment: number, opr_level: number) => {
  log.info(`createSaleOrder, uid: ${ctx.uid}, vid:${vid}, pid: ${pid}, qid: ${qid},items: ${JSON.stringify(items)}, summary: ${summary}, payment: ${payment}, opr_level: ${opr_level}`);
  try {
    await verify([stringVerifier("vid", qid), stringVerifier("qid", qid)]);
  } catch (e) {
    return { code: 400, msg: e.message };
  }
  try {
    const uid = ctx.uid;
    const order_id = uuid.v1();
    const callback = order_id;
    const domain = ctx.domain;
    const args = { type: 1, order_type: 3, domain: ctx.domain, uid: ctx.uid, vid: vid, pid: pid, qid: qid, items: items, summary: summary, payment: payment, opr_level: opr_level };
    ctx.push("order-events-disque", args);
    return await waitingAsync(ctx);
  } catch (e) {
    log.info("createSaleOrder catch ERROR" + e.message);
    throw { code: 500, msg: e.message };
  }
});

server.callAsync("renameNo", allowAll, "更新订单编号", "更新订单编号", async (ctx: ServerContext, order_no: string) => {
  log.info(`renameNo, uid: ${ctx.uid}, order_no: ${order_no}`);
  try {
    await verify([stringVerifier("order_no", order_no)]);
  } catch (e) {
    return { code: 400, msg: e.message };
  }
  const args = { type: 10, order_no: order_no };
  ctx.push("order-events-disque", args);
  return await waitingAsync(ctx);
});

server.callAsync("refund", allowAll, "银行退款", "更改订单对应状态", async (ctx: ServerContext, order_id: string) => {
  log.info(`refund, uid: ${ctx.uid}, order_id: ${order_id}`);
  try {
    await verify([stringVerifier("order_id", order_id)]);
  } catch (e) {
    return { code: 400, msg: e.message };
  }
  const args = { type: 9, order_id: order_id };
  ctx.push("order-events-disque", args);
  return await waitingAsync(ctx);
});

server.callAsync("agreeWithdraw", allowAll, "同意提现申请", "更改订单状态", async (ctx: ServerContext, order_id: string) => {
  log.info(`agreeWithdraw, uid:${ctx.uid},order_id: ${order_id}`);
  try {
    await verify([stringVerifier("order_id", order_id)]);
  } catch (e) {
    return { code: 400, msg: e.message };
  }
  const args = { type: 8, order_id: order_id };
  ctx.push("order-events-disque", args);
  return await waitingAsync(ctx);
});

server.callAsync("refuseWithdraw", allowAll, "拒绝提现申请", "拒绝后更改订单状态", async (ctx: ServerContext, order_id: string, reason: string) => {
  log.info(`refuseWithdraw, uid: ${ctx.uid}, order_id: ${order_id}, reason: ${reason}`);
  try {
    await verify([stringVerifier("order_id", order_id)]);
  } catch (e) {
    return { code: 400, msg: e.message };
  }
  const args = { type: 7, order_id: order_id, reason: reason };
  ctx.push("order-events-disque", args);
  return await waitingAsync(ctx);
});

server.callAsync("applyWithdraw", allowAll, "申请提现", "申请提现时更改订单状态", async (ctx: ServerContext, order_id: string) => {
  log.info(`applyWithdraw, uid:${ctx.uid}, order_id: ${order_id}`);
  try {
    await verify([stringVerifier("order_id", order_id)]);
  } catch (e) {
    log.info(e);
    return { code: 400, msg: e.message };
  }
  const args = { type: 6, order_id: order_id };
  ctx.push("order-events-disque", args);
  return await waitingAsync(ctx);
});

server.callAsync("expire", allowAll, "订单到期", "对到期订单处理", async (ctx: ServerContext, order_id: string) => {
  log.info(`expire, uid:${ctx.uid}, order_id: ${order_id}`);
  try {
    await verify([stringVerifier("order_id", order_id)]);
  } catch (e) {
    log.info(e);
    return { code: 400, msg: e.message };
  }
  const args = { type: 5, order_id: order_id };
  ctx.push("order-events-disque", args);
  return await waitingAsync(ctx);
});

server.callAsync("takeEffect", allowAll, "订单生效", "对生效订单进行处理", async (ctx: ServerContext, order_id: string) => {
  log.info(`takeEffect, uid:${ctx.uid}, order_id: ${order_id}`);
  try {
    await verify([stringVerifier("order_id", order_id)]);
  } catch (e) {
    log.info(e);
    return { code: 400, msg: e.message };
  }
  const args = { type: 4, order_id: order_id };
  ctx.push("order-events-disque", args);
  return await waitingAsync(ctx);
});


server.callAsync("underwrite", allowAll, "订单核保", "对核保状态下订单进行处理", async (ctx: ServerContext, order_id: string, opid: string, start_at: Date, stop_at: Date) => {
  log.info(`underwrite, uid:${ctx.uid}, opid:${opid}, order_id: ${order_id},start_at: ${start_at}, stop_at: ${stop_at}`);
  try {
    await verify([uuidVerifier("order_id", order_id), uuidVerifier("opid", opid)]);
  } catch (e) {
    log.info(e);
    return { code: 400, msg: e.message };
  }
  const args = { type: 3, order_id: order_id, opid: opid, start_at: start_at, stop_at: stop_at };
  ctx.push("order-events-disque", args);
  return await waitingAsync(ctx);
});





server.callAsync("pay", allowAll, "用户支付订单", "更改订单支付状态", async (ctx: ServerContext, uid: string, order_id: string, amount: number, payment_method: number) => {
  log.info(`pay, uid:${ctx.uid}, order_id: ${order_id}, amount: ${amount}, payment_method: ${payment_method}`);
  try {
    await verify([stringVerifier("order_id", order_id)]);
  } catch (e) {
    log.info(e);
    return { code: 400, msg: e.message };
  }
  if (ctx.domain === "admin") {
    const args = { type: 2, uid: uid, order_id: order_id, amount: amount, payment_method: payment_method };
    ctx.push("order-events-disque", args);
    return await waitingAsync(ctx);
  } else {
    if (uid !== ctx.uid) {
      return { code: 500, msg: "仅限本人操作" };
    } else {
      const args = { type: 2, uid: ctx.uid, order_id: order_id, amount: amount, payment_method: payment_method };
      ctx.push("order-events-disque", args);
      return await waitingAsync(ctx);
    }
  }
});


server.callAsync("cancel", allowAll, "取消订单", "删除订单数据", async (ctx: ServerContext, order_id: string, mobile?: boolean) => {
  log.info(`cancel, uid:${ctx.uid}, order_id: ${order_id}`);
  try {
    await verify([stringVerifier("order_id", order_id)]);
  } catch (e) {
    log.info(e);
    return { code: 400, msg: e.message };
  }
  if (mobile && mobile === true) {
    const pkt: CmdPacket = { cmd: "cancel", args: [order_id] };
    ctx.publish(pkt);
    return await waitingAsync(ctx);
  } else {
    const args = { type: 0, order_id: order_id };
    ctx.push("order-events-disque", args);
    return await waitingAsync(ctx);
  }
});

server.callAsync("addDrivers", allowAll, "添加司机", "添加司机", async (ctx: ServerContext, order_id: string, dids: Object[]) => {
  log.info(`addDrivers, uid: ${ctx.uid}, order_id: ${order_id}, dids: ${dids}`);
  try {
    await verify([stringVerifier("order_id", order_id), arrayVerifier("dids", dids)]);
  } catch (e) {
    log.info(e);
    return { code: 400, msg: e.message };
  }
  try {
    const args = { type: 11, order_id: order_id, dids: dids };
    ctx.push("order-events-disque", args);
    return await waitingAsync(ctx);
  } catch (e) {
    log.info(e);
    throw e;
  }
});


server.callAsync("delDrivers", allowAll, "删除司机", "删除司机", async (ctx: ServerContext, order_id: string, dids: Object[]) => {
  log.info(`delDrivers, uid: ${ctx.uid}, order_id: ${order_id}, dids: ${dids}`);
  try {
    await verify([stringVerifier("order_id", order_id), arrayVerifier("dids", dids)]);
  } catch (e) {
    log.info(e);
    return { code: 400, msg: e.message };
  }
  try {
    const args = { type: 12, order_id: order_id, dids: dids };
    ctx.push("order-events-disque", args);
    return await waitingAsync(ctx);
  } catch (e) {
    log.info(e);
    throw e;
  }
});


server.callAsync("updateDrivingView", allowAll, "修改行驶证信息", "修改行驶证信息", async (ctx: ServerContext, order_id: string, driving_frontal_view: string, driving_rear_view: string, pid: string, identity_frontal_view: string, identity_rear_view: string, driver_views: Object) => {
  log.info(`updateDrivingView, uid: ${ctx.uid}, order_id: ${order_id}, pid: ${pid}, driving_frontal_view: ${driving_frontal_view}, driving_rear_view: ${driving_rear_view}, identity_frontal_view: ${identity_frontal_view}, identity_rear_view: ${identity_rear_view}, driver_views: ${JSON.stringify(driver_views)}`);
  try {
    await verify([stringVerifier("order_id", order_id)]);
  } catch (e) {
    log.info(e);
    return { code: 400, msg: e.message };
  }
  try {
    const args = { type: 13, order_id: order_id, driving_frontal_view, driving_rear_view };
    const images = [];
    const owner = { pid: pid, identity_frontal_view: identity_frontal_view, identity_rear_view: identity_rear_view, license_frontal_view: "" };
    for (const p in driver_views) {
      if (String(pid) === String(p)) {
        owner["license_frontal_view"] = driver_views[p];
      } else {
        const driver_view = { pid: p, identity_frontal_view: "", identity_rear_view: "", license_frontal_view: driver_views[p] };
        images.push(driver_view);
      }
    }
    images.push(owner);
    const prep = await rpcAsync<Object>(ctx.domain, process.env["PERSON"], ctx.uid, "updateViews", images);
    if (prep["code"] === 200) {
      ctx.push("order-events-disque", args);
      return await waitingAsync(ctx);
    } else {
      return { code: prep["code"], msg: prep["msg"] };
    }
  } catch (e) {
    log.info(e);
    throw e;
  }
});

server.callAsync("getPlanOrder", allowAll, "获取订单详情", "获得订单详情", async (ctx: ServerContext, oid: string) => {
  log.info(`getOrder, uid: ${ctx.uid}, oid: ${oid}`);
  try {
    await verify([stringVerifier("oid", oid)]);
  } catch (e) {
    log.info(e);
    return { code: 400, msg: e.message };
  }
  try {
    const orep = await ctx.cache.hgetAsync("order-entities", oid);
    if (orep === null || orep === "") {
      return { code: 404, msg: "未找到对应订单信息" };
    } else {
      const order_entities = await msgpack_decode(orep);
      const nowDate = new Date();
      const uid = order_entities["uid"];
      if (ctx.domain === "admin") {
        log.info("admin");
        return { code: 200, data: order_entities, now: nowDate };
      } else {
        if (uid === ctx.uid) {
          log.info("mobile");
          return { code: 200, data: order_entities, now: nowDate };
        } else {
          return { code: 501, msg: "用户信息获取错误，请联系客服协助处理，客服电话：4006869809" };
        }
      }
    }
  } catch (e) {
    log.info("getOrder catch ERROR" + e);
    return { code: 500, msg: e.message };
  }
});


server.callAsync("getPlanOrdersByUser", allowAll, "获取订单列表", "获得一个用户的所有订单", async (ctx: ServerContext, uid?: string) => {
  log.info(`getPlanOrdersByUser, uid${uid ? uid : ctx.uid}`);
  try {
    let u = null;
    if (ctx.domain === "admin") {
      u = uid ? uid : ctx.uid;
    } else {
      u = ctx.uid;
    }
    const oids = await ctx.cache.zrangeAsync(`orders-${u}`, 0, -1);
    if (oids !== null && oids.length !== 0) {
      const multi = bluebird.promisifyAll(ctx.cache.multi()) as Multi;
      for (const oid of oids) {
        multi.hget("order-entities", String(oid));
      }
      const orders: Object[] = [];
      const nowDate = new Date();
      const oreps = await multi.execAsync();
      for (const pkt of oreps) {
        if (pkt !== null) {
          const order = await msgpack_decode(pkt);
          orders.push(order);
        }
      }
      if (orders.length === 0) {
        return { code: 404, msg: "未找到对应订单信息" };
      } else {
        return { code: 200, data: orders, now: nowDate };
      }
    } else {
      return { code: 404, msg: "未找到对应订单信息" };
    }
  } catch (e) {
    log.info("getOrders catch ERROR" + e.message);
    return { code: 500, msg: e.message };
  }
});

server.callAsync("getPlanOrderByVehicle", allowAll, "获取计划单", "根据vid获取计划订单", async (ctx: ServerContext, vid: string) => {
  log.info(`getPlanOrderByVehicle, uid: ${ctx.uid}, vid: ${vid}`);
  try {
    await verify([stringVerifier("vid", vid)]);
  } catch (e) {
    log.info(e);
    return { code: 400, msg: e.message };
  }
  try {
    const poid = await ctx.cache.hgetAsync("vid-poid", vid);
    const now = new Date();
    if (poid === null || poid === "") {
      return { code: 404, msg: "未找到对应订单信息" };
    } else {
      const orep = await ctx.cache.hgetAsync("order-entities", String(poid));
      const order_entities = await msgpack_decode(orep);
      const uid = order_entities["uid"];
      if (ctx.domain === "admin") {
        return { code: 200, data: order_entities, now: now };
      } else {
        if (uid === ctx.uid) {
          return { code: 200, data: order_entities, now: now };
        } else {
          return { code: 501, msg: "用户信息获取错误，请联系客服协助处理，客服电话：4006869809" };
        }
      }
    }
  } catch (e) {
    log.info("getPlanOrderByVehicle catch ERROR" + e);
    return { code: 500, msg: e.message };
  }
});


server.callAsync("getPlanOrderByQid", allowAll, "获取订单详情", "根据ｑｉｄ获取订单详情", async (ctx: ServerContext, qid: string) => {
  log.info(`getPlanOrderByQid, uid: ${ctx.uid}, qid: ${qid}`);
  try {
    await verify([stringVerifier("qid", qid)]);
  } catch (e) {
    log.info(e);
    return { code: 400, msg: e.message };
  }
  const oid = await ctx.cache.hgetAsync("qid-oid", qid);
  const now = new Date();
  if (oid === null || oid === "") {
    return { code: 404, msg: "未找到对应订单信息" };
  } else {
    const orep = await ctx.cache.hgetAsync("order-entities", String(oid));
    if (orep === null || orep === "") {
      return { code: 404, msg: "未找到对应订单信息" };
    } else {
      const order_entities = await msgpack_decode(orep);
      const uid = order_entities["uid"];
      if (ctx.domain === "admin") {
        return { code: 200, data: order_entities, now: now };
      } else {
        if (uid === ctx.uid) {
          return { code: 200, data: order_entities, now: now };
        } else {
          return { code: 501, msg: "用户信息获取错误，请联系客服协助处理，客服电话：4006869809" };
        }
      }
    }
  }
});


server.callAsync("getOrdersByVid", allowAll, "获取车辆对应所有订单", "获取车辆对应所有订单", async (ctx: ServerContext, vid: string) => {
  log.info(`getOrdersByVid, uid: ${ctx.uid}, vid: ${vid}`);
  try {
    await verify([stringVerifier("vid", vid)]);
  } catch (e) {
    log.info(e);
    return { code: 400, msg: e.message };
  }
  try {
    const oids: any = await ctx.cache.zrangeAsync(`orders-${vid}`, 0, -1);
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
      const uid = orders[0]["uid"];
      if (ctx.domain === "admin") {
        return { code: 200, data: orders };
      } else {
        if (uid === ctx.uid) {
          return { code: 200, data: orders };
        } else {
          return { code: 501, msg: "用户信息获取错误，请联系客服协助处理，客服电话：4006869809" };
        }
      }
    }
  } catch (e) {
    log.info("getOrdersByVid catch ERROR" + e);
    return { code: 500, msg: e.message };
  }
});

server.callAsync("getDeletedPlanOrder", allowAll, "获取订单信息", "获取数据库已取消订单信息", async (ctx: ServerContext, oid: string): Promise<any> => {
  log.info(`getInnerOrder, oid: ${oid}`);
  try {
    await verify([uuidVerifier("oid", oid)]);
  } catch (e) {
    log.info(e);
    ctx.report(1, e);
    return { code: 400, msg: e.message };
  }
  const pkt: CmdPacket = { cmd: "getDeletedPlanOrder", args: [oid] };
  ctx.publish(pkt);
  return await waitingAsync(ctx);
});

server.callAsync("refresh", adminOnly, "refresh", "刷新订单数据", async (ctx: ServerContext, order_id?: string) => {
  log.info(`refresh, order_id: ${order_id}`);
  if (order_id) {
    const args = { type: 20, order_id: order_id };
    ctx.push("order-events-disque", args);
    return await waitingAsync(ctx);
  } else {
    const args = { type: 21 };
    ctx.push("order-events-disque", args);
    return await waitingAsync(ctx);
  }
});


