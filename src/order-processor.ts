import { Processor, ProcessorFunction, ProcessorContext, rpc } from "hive-service";
import { Client as PGClient, QueryResult } from "pg";
import { RedisClient } from "redis";
import { CustomerMessage } from "recommend-library";
import * as bunyan from "bunyan";
import * as uuid from "node-uuid";

export const processor = new Processor();

declare module "redis" {
  export interface RedisClient extends NodeJS.EventEmitter {
    incrAsync(key: string): Promise<any>;
    hgetAsync(key: string, field: string): Promise<any>;
    hincrbyAsync(key: string, field: string, value: number): Promise<any>;
    lpushAsync(key: string, value: string | number): Promise<any>;
    setexAsync(key: string, ttl: number, value: string): Promise<any>;
    zrevrangebyscoreAsync(key: string, start: number, stop: number): Promise<any>;
  }
  export interface Multi extends NodeJS.EventEmitter {
    execAsync(): Promise<any>;
  }
}

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

async function increase_order_no(cache): Promise<number> {
  return cache.incrAsync("order-no");
}

async function sync_plan_orders(db: PGClient, cache: RedisClient, domain: string, uid: string, oid?: string): Promise<void> {
  const result: QueryResult = await db.query("SELECT o.id AS o_id, o.no AS o_no, o.vid AS o_vid, o.type AS o_type, o.state_code AS o_state_code, o.state AS o_state, o.summary AS o_summary, o.payment AS o_payment, o.start_at AS o_start_at, o.stop_at AS o_stop_at, o.paid_at AS o_paid_at, o.created_at AS o_created_at, o.updated_at AS o_updated_at, e.qid AS e_qid, e.pid AS e_pid, e.service_ratio AS e_service_ratio, e.expect_at AS e_expect_at, oi.id AS oi_id, oi.price AS oi_price, oi.piid AS oi_piid FROM plan_order_ext AS e INNER JOIN orders AS o ON o.id = e.oid INNER JOIN plans AS p ON e.pid = p.id INNER JOIN plan_items AS pi ON p.id = pi.pid INNER JOIN order_items AS oi ON oi.piid = pi.id AND oi.oid = o.id WHERE o.deleted = FALSE AND e.deleted = FALSE AND oi.deleted = FALSE AND p.deleted = FALSE AND pi.deleted = FALSE" + (oid ? " AND o.id = $1" : ""), oid ? [oid] : []);
  const orders = {};
  for (const row of result.rows) {
    if (orders.hasOwnProperty(row.o_id)) {
      orders[row.o_id]["items"].push({
        id: row.oi_id,
        oid: row.oi_oid,
        piid: row.oi_piid,
        plan_item: null,
        price: row.oi_price
      });
      orders[row.o_id]["pids"][row.e_pid] = 0;
    } else {
      const pids = {};
      pids[row.e_pid] = 0;
      const order = {
        id: row.o_id,
        no: trim(row.o_no),
        type: row.o_type,
        state_code: row.o_state_code,
        state: trim(row.o_state),
        summary: row.o_summary,
        payment: row.o_payment,
        v_value: row.vehicle_real_value,
        start_at: row.o_start_at,
        stop_at: row.o_stop_at,
        vid: row.o_vid,
        vehicle: null,
        pids: pids,
        plans: [],
        qid: row.e_qid,
        quotation: null,
        service_ratio: row.e_service_ratio,
        expect_at: row.e_expect_at,
        items: [{
          id: row.oi_id,
          oid: row.oi_oid,
          piid: row.oi_piid,
          plan_item: null,
          price: row.oi_price
        }],
        created_at: row.o_created_at,
        updated_at: row.o_updated_at,
        paid_at: row.o_paid_at
      };
      orders[row.o_id] = order;
    }
  }

  const oids = Object.keys(orders);
  const vidstmp = [];
  const qidstmp = [];
  const pidstmp = [];
  const piidstmp = [];
  for (const oid of oids) {
    vidstmp.push(orders[oid]["vid"]);
    qidstmp.push(orders[oid]["qid"]);
    for (const pid of Object.keys(orders[oid]["pids"])) {
      pidstmp.push(pid);
    }
    for (const item of orders[oid]["items"]) {
      piidstmp.push(item["plan_item"]);
    }
  }
  const vids = [... new Set(vidstmp)];
  const qids = [... new Set(qidstmp)];
  const pids = [... new Set(pidstmp)];

  const pvs = vids.map(vid => rpc<Object>(domain, process.env["VEHICLE"], uid, "getVehicle", vid)); // fetch vehicles in parallel way
  const vreps = await Promise.all(pvs);
  const vehicles = vreps.filter(v => v["code"] === 200).map(v => v["data"]);
  for (const vehicle of vehicles) {
    for (const oid of oids) {
      const order = orders[oid];
      if (vehicle["id"] === order["vid"]) {
        order["vehicle"] = vehicle; // a vehicle may belong to many orders
      }
    }
  }

  const pqs = qids.map(qid => rpc<Object>(domain, process.env["QUOTATION"], uid, "getQuotation", qid)); // fetch quotations in parallel way
  const qreps = await Promise.all(pqs);
  const quotations = qreps.filter(q => q["code"] === 200).map(q => q["data"]);
  for (const quotation of quotations) {
    for (const oid of oids) {
      const order = orders[oid];
      if (quotation["id"] === order["qid"]) {
        order["quotation"] = quotation;
        break; // a quotation only belongs to an order
      }
    }
  }

  const pps = pids.map(pid => rpc<Object>(domain, process.env["PLAN"], uid, "getPlan", pid)); // fetch plan in parallel way
  const preps = await Promise.all(pps);
  const plans = preps.filter(p => p["code"] === 200).map(p => p["data"]);
  for (const oid of oids) {
    const order = orders[oid];
    for (const plan of plans) {
      if (order["pids"].hasOwnProperty(plan["id"])) {
        order["plans"].push(plan); // a plan may belong to many orders
      }
      for (const planitem of plan["items"]) {
        for (const orderitem of order["items"]) {
          if (planitem["id"] === orderitem["piid"]) {
            orderitem["plan_item"] = planitem;
          }
        }
      }
    }
  }

  const multi = cache.multi();
  for (const oid of oids) {
    const order = orders[oid];
    delete order["pids"];
    const order_no = order["no"];
    const vid = order["vid"];
    const qid = order["qid"];
    const updated_at = order["updated_at"].getTime();
    multi.zadd("new-orders-id", updated_at, oid);
    multi.hset("vid-poid", vid, oid);
    multi.zadd("plan-orders", updated_at, oid);
    multi.zadd("orders", updated_at, oid);
    multi.zadd("orders-" + order["vehicle"]["user_id"], updated_at, oid);
    multi.hset("orderNo-id", order_no, oid);
    multi.hset("order-vid-" + vid, qid, oid);
    multi.hset("orderid-vid", oid, vid);
    multi.hset("order-entities", oid, JSON.stringify(order));
    multi.zadd("plan-orders", updated_at, oid);
    multi.hset("VIN-orderID", order["vehicle"]["vin_code"], oid);
  }

  return multi.execAsync();
}

processor.call("placeAnPlanOrder", (ctx: ProcessorContext, domain: string, uid: string, order_id: string, vid: string, plans: Object[], qid: string, pmid: string, promotion: number, service_ratio: number, summary: number, payment: number, v_value: number, expect_at: Date, cbflag: string) => {
  log.info(`placeAnPlanOrder, domain: ${domain}, uid: ${uid}, order_id: ${order_id}, vid: ${vid}, plans: ${JSON.stringify(plans)}, qid: ${qid}, pmid: ${pmid}, promotion: ${promotion}, service_ratio: ${service_ratio}, summary: ${summary}, payment: ${payment}, v_value: ${v_value}, expect_at: ${expect_at}, cbflag: ${cbflag}`);
  const db: PGClient = ctx.db;
  const cache: RedisClient = ctx.cache;
  const done = ctx.done;

  const event_id = uuid.v1();
  const state_code = 1;
  const state = "已创建订单";
  const type = 0;
  const plan_data1 = "新增plan计划";
  const plan_data = JSON.stringify(plan_data1);

  const date = new Date();
  const year = date.getFullYear();
  const pids = [];
  for (const plan in plans) {
    pids.push(plan);
  }

  (async () => {
    try {
      const strno = await increase_order_no(cache);

      let sum = 0;
      for (const i of pids) {
        const str = i.substring(24);
        const num = parseInt(str);
        sum += num;
      }
      const sum2 = sum + "";
      const sum1 = formatNum(sum2, 3);
      const order_no = "1" + "110" + "001" + sum1 + year + strno;
      await db.query("BEGIN");
      await db.query("INSERT INTO orders(id, vid, type, state_code, state, summary, payment, no) VALUES($1,$2,$3,$4,$5,$6,$7,$8)", [order_id, vid, type, state_code, state, summary, payment, order_no]);
      for (const pid of Object.keys(plans)) {
        await db.query("INSERT INTO plan_order_ext(oid, pmid, promotion, pid, qid, service_ratio, expect_at, vehicle_real_value) VALUES($1,$2,$3,$4,$5,$6,$7,$8)", [order_id, pmid, promotion, pid, qid, service_ratio, expect_at, v_value]);
        for (const piid in plans[pid]) {
          const item_id = uuid.v1();
          await db.query("INSERT INTO order_items(id, oid, piid, price) VALUES($1,$2,$3,$4)", [item_id, order_id, piid, plans[pid][piid]]);
        }
      }
      await db.query("COMMIT");
      await cache.setexAsync(cbflag, 30, JSON.stringify({
        code: 200,
        data: { id: order_id, no: order_no }
      })); // Notify server to return result to client
      await sync_plan_orders(db, cache, domain, uid, order_id);
    } catch (e) {
      log.error(e);
      try {
        await db.query("ROLLBACK");
      } catch (e1) {
        log.error(e1);
      }
      cache.setex(cbflag, 30, JSON.stringify({
        code: 500,
        msg: e.message
      }), (err, result) => {
        done();
      });
      return;
    }
    done(); // release database
    try {
      const profile_response = await rpc<Object>(domain, process.env["PROFILE"], null, "getUserByUserId", uid);
      if (profile_response["code"] === 200 && profile_response["data"]["ticket"]) {
        const profile = profile_response["data"];
        const cm: CustomerMessage = {
          type: 3,
          ticket: profile["ticket"],
          cid: profile["id"],
          name: profile["nickname"],
          oid: order_id,
          qid: qid,
          occurredAt: date
        };
        await cache.lpushAsync("agent-customer-msg-queue", JSON.stringify(cm));
      }
    } catch (e) {
      log.error(e);
    }
  })();
});

processor.call("updateOrderNo", (ctx: ProcessorContext, order_no: string, new_order_no: string, cbflag: string) => {
  log.info(`updateOrderNo, order_no: ${order_no}, new_order_no: ${new_order_no}`);
  const db: PGClient = ctx.db;
  const cache: RedisClient = ctx.cache;
  const done = ctx.done;
  (async () => {
    try {
      const oid = await cache.hgetAsync("orderNo-id", order_no);
      if (oid === null || oid === "") {
        await cache.setexAsync(cbflag, 30, JSON.stringify({
          code: 404,
          msg: "Order id not found"
        }));
        done();
        return;
      }
      await db.query("UPDATE orders SET no = $1 WHERE id = $2", [new_order_no, oid]);
      const orderjson = await cache.hgetAsync("order-entities", oid);
      if (orderjson === null || orderjson === "") {
        await cache.setexAsync(cbflag, 30, JSON.stringify({
          code: 404,
          msg: "Order not found"
        }));
        done();
        return;
      }
      const order = JSON.parse(orderjson);
      order["no"] = new_order_no;
      const multi = cache.multi();
      multi.hdel("orderNo-id", order_no);
      multi.hset("orderNo-id", new_order_no, oid);
      multi.hset("order-entities", oid, JSON.stringify(order));
      await multi.execAsync();
      await cache.setexAsync(cbflag, 30, JSON.stringify({
        code: 200,
        data: new_order_no
      }));
      done();
    } catch (e) {
      log.error(e);
      cache.setex(cbflag, 30, JSON.stringify({
        code: 500,
        msg: e.message
      }), (err, result) => {
        done();
      });
    }
  })();
});

async function sync_driver_orders(db: PGClient, cache: RedisClient, domain: string, uid: string, oid?: string): Promise<void> {
  const result = await db.query("SELECT o.id AS o_id, o.no AS o_no, o.vid AS o_vid, o.type AS o_type, o.state_code AS o_state_code, o.state AS o_state, o.summary AS o_summary, o.payment AS o_payment, o.start_at AS o_start_at, o.stop_at AS o_stop_at, o.created_at AS o_created_at, o.updated_at AS o_updated_at, e.pid AS e_pid FROM driver_order_ext AS e LEFT JOIN orders AS o ON o.id = e.oid WHERE o.deleted = FALSE AND e.deleted = FALSE" + (oid ? " AND o.id = $1" : ""), oid ? [oid] : []);
  const orders = {};
  for (const row of result.rows) {
    if (orders.hasOwnProperty(row.o_id)) {
      orders[row.o_id]["dids"].push(row.e_pid);
    } else {
      const order = {
        order_id: row.o_id,
        id: trim(row.o_no),
        type: row.o_type,
        state_code: row.o_state_code,
        state: trim(row.o_state),
        summary: row.o_summary,
        payment: row.o_payment,
        start_at: row.o_start_at,
        stop_at: row.o_stop_at,
        vid: row.o_vid,
        vehicle: null,
        drivers: [],
        dids: [row.e_pid],
        created_at: row.o_created_at,
        updated_at: row.o_created_at
      };
      orders[row.o_id] = order;
    }
  }

  const oids = Object.keys(orders);
  const pvs = oids.map(oid => rpc<Object>(domain, process.env["VEHICLE"], null, "getVehicle", orders[oid].vid)); // fetch vehicles in parallel
  const vreps = await Promise.all(pvs);
  const vehicles = vreps.filter(v => v["code"] === 200).map(v => v["data"]);
  for (const vehicle of vehicles) {
    for (const oid of oids) {
      const order = orders[oid];
      if (vehicle["id"] === order.vid) {
        order.vehicle = vehicle;
      }
    }
  }
  const pds = oids.reduce((acc, oid) => {
    const order = orders[oid];
    for (const did of order.dids) {
      const p = rpc<Object>(domain, process.env["VEHICLE"], null, "getDrivers", order.vid, did);
      acc.push(p);
    }
    return acc;
  }, []);
  const drvreps = await Promise.all(pds);
  const drivers = drvreps.filter(d => d["code"] === 200).map(d => d["data"]);
  for (const driver of drivers) {
    for (const oid of oids) {
      const order = orders[oid];
      for (const drv of order.drivers) {
        if (drv === driver["id"]) {
          order.drivers.push(driver);
        }
      }
    }
  }

  const multi = cache.multi();
  for (const oid of oids) {
    const order = orders[oid];
    const updated_at = order.updated_at.getTime();
    const uid = order["uid"];
    const vid = order["vid"];
    multi.zadd("driver_orders", updated_at, oid);
    multi.zadd("orders", updated_at, oid);
    multi.hset("vid-doid", vid, JSON.stringify(order["drivers"].map(d => d["id"])));
    multi.hset("driver-entities-", vid, JSON.stringify(order["drivers"]));
    multi.hset("order-entities", oid, JSON.stringify(order));
    multi.zadd("driver-orders", updated_at, oid);
  }
  return multi.execAsync();
}

processor.call("placeAnDriverOrder", (ctx: ProcessorContext, domain: string, uid: string, vid: string, dids: string[], summary: number, payment: number, cbflag: string) => {
  log.info(`placeAnDriverOrder, domain: ${domain}, uid: ${uid}, vid: ${vid}, dids: ${JSON.stringify(dids)}, summary: ${summary}, payment: ${payment}, cbflag: ${cbflag}`);
  const db: PGClient = ctx.db;
  const cache: RedisClient = ctx.cache;
  const done = ctx.done;
  const order_id = uuid.v1();
  const item_id = uuid.v1();
  const event_id = uuid.v1();
  const state_code = 2;
  const state = "已支付";
  const type = 1;
  (async () => {
    try {
      await db.query("BEGIN");
      await db.query("INSERT INTO order_events(id, oid, uid, data) VALUES($1,$2,$3,$4)", [event_id, order_id, uid, "添加驾驶人"]);
      await db.query("INSERT INTO orders(id, vid, type, state_code, state, summary, payment) VALUES($1,$2,$3,$4,$5,$6,$7)", [order_id, vid, type, state_code, state, summary, payment]);

      for (const did of dids) {
        await db.query("INSERT INTO driver_order_ext(oid,pid) VALUES($1,$2)", [order_id, did]);
      }
      await db.query("COMMIT");
      await sync_driver_orders(db, cache, domain, order_id);
      await cache.setexAsync(cbflag, 30, JSON.stringify({
        code: 200,
        data: order_id
      }));
    } catch (err) {
      cache.setex(cbflag, 30, JSON.stringify({
        code: 500,
        msg: err.message
      }), (err, result) => {
        done();
      });
    }
  });
});

async function createAccount(domain: string, order: Object, uid: string): Promise<any> {
  const vid = order["vehicle"]["id"];
  const balance = order["summary"];
  const balance0 = balance * 0.2;
  const balance1 = balance * 0.8;
  return rpc(domain, process.env["WALLET"], null, "createAccount", uid, 1, vid, balance0, balance1);
}

processor.call("updateOrderState", (ctx: ProcessorContext, domain: string, uid: string, vid: string, order_id: string, state_code: number, state: string, cbflag: string) => {
  log.info(`updateOrderState domain: ${domain}, uid: ${uid}, vid: ${vid}, order_id: ${order_id}, state_code: ${state_code}, state: ${state}, cbflag: ${cbflag}`);
  const db: PGClient = ctx.db;
  const cache: RedisClient = ctx.cache;
  const done = ctx.done;
  const code = state_code;
  const type1 = 1;
  const balance: number = null;
  const start_at = null;
  const paid_at = new Date();

  (async () => {
    try {
      if (state_code === 2) {
        await db.query("UPDATE orders SET state_code = $1, state = $2, paid_at = $3, updated_at = $4 WHERE id = $5", [state_code, state, paid_at, paid_at, order_id]);
      } else {
        await db.query("UPDATE orders SET state_code = $1, state = $2, updated_at = $3 WHERE id = $4", [state_code, state, paid_at, order_id]);
      }
      const orderjson = await cache.hgetAsync("order-entities", order_id);
      if (orderjson === null || orderjson === "") {
        await cache.setexAsync(cbflag, 30, JSON.stringify({
          code: 404,
          msg: `Order ${order_id} not found in order-entities`
        }));
        done();
        return;
      }
      const order = JSON.parse(orderjson);
      const multi = cache.multi();
      const updated_at = (new Date()).getTime();
      order["state_code"] = state_code;
      order["state"] = state;
      order["updated_at"] = paid_at;
      if (state_code === 2) {
        order["paid_at"] = paid_at;
        multi.zrem("new-orders-id", order_id);
        multi.zadd("new-pays-id", updated_at, order_id);
      }
      multi.hset("order-entities", order_id, JSON.stringify(order));
      await multi.execAsync();
      if (state_code === 2) {
        await createAccount(domain, order, uid);
      }
      cache.setex(cbflag, 30, JSON.stringify({
        code: 200,
        data: order_id
      }));
      done();
    } catch (err) {
      cache.setex(cbflag, 30, JSON.stringify({
        code: 500,
        msg: err.message
      }), (err, result) => {
        done();
      });
    }
  })();
});

async function sync_sale_orders(db: PGClient, cache: RedisClient, domain: string, uid: string, oid?: string): Promise<void> {
  const result = await db.query("SELECT o.id AS o_id, o.no AS o_no, o.vid AS o_vid, o.type AS o_type, o.state_code AS o_state_code, o.state AS o_state, o.summary AS o_summary, o.payment AS o_payment, o.start_at AS o_start_at, o.stop_at AS o_stop_at, o.created_at AS o_created_at, o.updated_at AS o_updated_at, e.qid AS e_qid, e.opr_level AS e_opr_level, oi.id AS oi_id, oi.pid AS oi_pid, oi.price AS oi_price, oi.piid AS oi_piid  FROM sale_order_ext AS e INNER JOIN orders AS o ON o.id = e.oid INNER JOIN plans AS p ON e.pid = p.id INNER JOIN plan_items AS pi ON p.id = pi.pid INNER JOIN order_items AS oi ON oi.piid = pi.id AND oi.oid = o.id WHERE o.deleted = FALSE AND e.deleted = FALSE AND p.deleted = FALSE AND pi.deleted = FALSE AND oi.deleted = FALSE" + (oid ? " AND o.id = $1" : ""), oid ? [oid] : []);
  const orders = {};
  for (const row of result.rows) {
    if (orders.hasOwnProperty(row.o_id)) {
      orders[row.o_id]["items"].push({
        id: row.oi_id,
        oid: row.o_id,
        piid: row.oi_piid,
        plan_item: null,
        price: row.oi_price
      });
    } else {
      const order = {
        id: row.o_id,
        no: trim(row.o_no),
        type: row.o_type,
        state_code: row.o_state_code,
        state: trim(row.o_state),
        summary: row.o_summary,
        payment: row.o_payment,
        start_at: row.o_start_at,
        stop_at: row.o_stop_at,
        vid: row.o_vid,
        vehicle: null,
        pid: row.e_pid,
        opr_level: row.e_opr_level,
        plan: null,
        qid: row.e_qid,
        quotation: null,
        items: [{
          id: row.oi_id,
          oid: row.o_id,
          piid: row.oi_piid,
          plan_item: null,
          price: row.oi_price
        }],
        created_at: row.o_created_at,
        updated_at: row.o_updated_at
      };
      orders[row.o_id] = order;
    }
  }

  const oids = Object.keys(orders);
  const vidstmp = [];
  const qidstmp = [];
  const pidstmp = [];
  const piidstmp = [];
  for (const oid of oids) {
    vidstmp.push(orders[oid]["vid"]);
    qidstmp.push(orders[oid]["qid"]);
    pidstmp.push(orders[oid]["pid"]);
    for (const item of orders[oid]["items"]) {
      piidstmp.push(item["plan_item"]);
    }
  }
  const vids = [... new Set(vidstmp)];
  const qids = [... new Set(qidstmp)];
  const pids = [... new Set(pidstmp)];

  const pvs = vids.map(vid => rpc<Object>(domain, process.env["VEHICLE"], null, "getVehicle", vid)); // fetch vehicle in parallel
  const vreps = await Promise.all(pvs);
  const vehicles = vreps.filter(v => v["code"] === 200).map(v => v["data"]);
  for (const vehicle of vehicles) {
    for (const oid of oids) {
      const order = orders[oid];
      if (vehicle["id"] === order["vid"]) {
        order["vehicle"] = vehicle; // a vehicle may belong to many orders
      }
    }
  }
  const pqs = qids.map(qid => rpc<Object>(domain, process.env["QUOTATION"], null, "getQuotation", qid)); // fetch quotation in parallel
  const qreps = await Promise.all(pqs);
  const quotations = qreps.filter(q => q["code"] === 200).map(q => q["data"]);
  for (const quotation of quotations) {
    for (const oid of oids) {
      const order = orders[oid];
      if (quotation["id"] === order["qid"]) {
        order["quotation"] = quotation;
        break; // a quotation only belongs to an order
      }
    }
  }
  const pps = pids.map(pid => rpc<Object>(domain, process.env["plan"], null, "getPlan", pid)); // fetch plan in parallel
  const preps = await Promise.all(pps);
  const plans = preps.filter(p => p["code"] === 200).map(p => p["data"]);
  for (const oid of oids) {
    const order = orders[oid];
    for (const plan of plans) {
      if (plan["id"] === order["pid"]) {
        order["plan"] = plan; // a plan may belong to many orders
      }
      for (const planitem of plan["items"]) {
        for (const orderitem of order["items"]) {
          if (planitem["id"] === orderitem["piid"]) {
            orderitem["plan_item"] = planitem;
          }
        }
      }
    }
  }
  const multi = cache.multi();
  for (const oid of oids) {
    const order = orders[oid];
    const vid = order["vehicle"]["vid"];
    const updated_at = order.updated_at.getTime();
    multi.zadd("orders", updated_at, oid);
    multi.hset("vid-soid", vid, oid);
    multi.hset("orderid-vid", oid, vid);
    multi.hset("order-entities", oid, JSON.stringify(order));
    multi.zadd("sale-orders", updated_at, oid);
  }
  return multi.execAsync();
}

processor.call("placeAnSaleOrder", (ctx: ProcessorContext, uid: string, domain: any, order_id: string, vid: string, pid: string, qid: string, items: Object[], summary: number, payment: number, opr_level: number, cbflag: string) => {
  log.info(`placeAnSaleOrder, uid: ${uid}, domain: ${domain}, order_id: ${order_id}, vid: ${vid}, pid: ${pid}, qid: ${qid}, items: ${items}, summary: ${summary}, payment: ${payment}, opr_level: ${opr_level}, cbflag: ${cbflag}`);
  const db: PGClient = ctx.db;
  const cache: RedisClient = ctx.cache;
  const done = ctx.done;
  const item_id = uuid.v1();
  const event_id = uuid.v1();
  const state_code = 1;
  const state = "已创建订单";
  const type = 2;
  const sale_id = uuid.v1();
  const sale_data = JSON.stringify("新增第三方代售订单");
  const piids = [];
  for (const item in items) {
    piids.push(item);
  }
  (async () => {
    try {
      await db.query("BEGIN");
      await db.query("INSERT INTO orders(id, vid, type, state_code, state, summary, payment) VALUES($1, $2, $3, $4, $5, $6, $7)", [order_id, vid, type, state_code, state, summary, payment]);
      await db.query("INSERT INTO order_events(id, oid, uid, data) VALUES($1, $2, $3, $4)", [event_id, order_id, uid, sale_data]);
      await db.query("INSERT INTO sale_order_ext(oid, pid, qid, opr_level) VALUES($1, $2, $3, $4)", [order_id, pid, qid, opr_level]);
      for (const piid of piids) {
        const item_id = uuid.v1();
        await db.query("INSERT INTO order_items(id, piid, oid, price) VALUES($1, $2, $3, $4)", [item_id, piid, order_id, items[piid]]);
      }
      await db.query("COMMIT");
    } catch (err) {
      log.error(err);
      try {
        await db.query("ROLLBACK");
      } catch (err1) {
        log.error(err1);
      }
      cache.setex(cbflag, 30, JSON.stringify({
        code: 500,
        msg: err.message
      }), (err, result) => {
        done();
      });
    }
    try {
      await cache.setexAsync(cbflag, 30, JSON.stringify({
        code: 200,
        data: order_id
      }));
      await sync_sale_orders(db, cache, domain, order_id);
      done();
    } catch (e) {
      log.error(e);
    }
  })();
});
