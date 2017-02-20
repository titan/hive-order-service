import { Processor, ProcessorContext, rpc, ProcessorFunction, AsyncServerFunction, CmdPacket, Permission, set_for_response, waiting, msgpack_decode, msgpack_encode } from "hive-service";
import { Client as PGClient, QueryResult } from "pg";
import { RedisClient, Multi } from "redis";
import { CustomerMessage } from "recommend-library";
import * as bluebird from "bluebird";
import * as bunyan from "bunyan";
import * as uuid from "node-uuid";
import * as http from "http";
import * as queryString from "querystring";
import * as Disq from "hive-disque";

export const processor = new Processor();
const disque = new Disq({ nodes: ["127.0.0.1", "127.0.0.1"] });

const wxhost = process.env["WX_ENV"] === "test" ? "dev.fengchaohuzhu.com" : "m.fengchaohuzhu.com";

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

const timer = setTimeout(function () {
  disque.getjob("order-events-disque", (result) => {
    // if (result)
  });
}, 1000);


async function create(data) {

}


async function cancel(data) {

}

async function pay(data) {

}

async function underWrite(data) {

}

async function take_effect(data) {

}


async function expired(data) {

}

async function apply_withdraw(data) {

}

async function refuse_withdraw(data) {

}

async function agree_withdraw(data) {

}

async function refund(data) {

}

async function rename_no(data) {

}


// id no uid pgid qid vid state state_description summary payment
// applicant
// promotion
// service_ratio
// vehicle_real_value
// outside_quotation1
// outside_quotation2
// screenshot1
// screenshot2
// ticket
// recommend
// expect_at
// start_at
// stop_at
// paid_at
// created_at
// updated_at
// evtid




async function new_async_plan_orders(db: PGClient, cache: RedisClient, domain: string, uid: string, oid?: string): Promise<void> {
  const result: QueryResult = await db.query("SELECT o.id AS o_id, o.no AS o_no, o.uid AS o_uid, o.pgid AS o_pgid, o.qid AS o_qid, o.vid AS o_vid, o.state AS o_state, o.state_description AS o_state_description, o.summary AS o_summary, o.payment AS o_payment, o.applicant AS o_applicant, o.promotion AS o_promotion, o.service_ratio AS o_service_ratio, o.vehicle_real_value AS o_vehicle_real_value, o.outside_quotation1 AS o_outside_quotation1, o.outside_quotation2 AS o_outside_quotation2, o.screenshot1 AS o_screenshot1, o.screenshot2 AS o_screenshot2, o.ticket AS o_ticket, o.recommend AS o_recommend, o.expect_at AS o_expect_at, o.start_at AS o_start_at, o.stop_at AS o_stop_at, o.paid_at AS o_paid_at, o.created_at AS o_created_at, o.update_at AS o_updated_at, o.evtid AS o_evtid, oi.id AS oi_id, oi.pid AS oi_pid, oi.price AS oi_price FROM plan_order_items AS oi INNER JOIN orders AS o ON o.id = oi.oid WHERE o.deleted = FALSE" + (oid ? " AND o.id = $1" : ""), oid ? [oid] : []);
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
        multi.zadd("plan-orders", updated_at, oid);
        multi.hset("VIN-orderID", order["vehicle"]["vin_code"], oid);
      }
      return multi.execAsync();
    }
  } catch (e) {
    log.info(e);
    throw e.message;
  }
};


async function new_async_driver_orders(db: PGClient, cache: RedisClient, domain: string, uid: string, oid?: string): Promise<void> {
  const result = await db.query("SELECT o.id AS o_id, o.no AS o_no, o.uid AS o_uid, o.vid AS o_vid, o.state AS o_state, o.state_description AS o_state_description,o.summary AS o_summary, o.payment AS o_payment, o.applicant AS o_applicant, o.paid_at AS o_paid_at, o.start_at AS o_start_at, o.stop_at AS o_stop_at, o.created_at AS o_created_at, o.update_at AS o_updated_at, o.evtid AS o_evtid oi.id AS oi_id, oi.oid AS oi_oid, oi.pid AS oi_pid FROM driver_order_items AS oi LEFT JOIN orders AS o ON o.id = oi.oid WHERE o.deleted = FALSE AND oi.deleted = FALSE" + (oid ? " AND o.id = $1" : ""), oid ? [oid] : []);
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
    const order = orders[oid];
    const updated_at = order.updated_at.getTime();
    const uid = order["uid"];
    const vid = order["vid"];
    const order_no = order["no"];
    const dreps = await cache.hgetAsync("order-driver-entities", vid);
    const old_drivers = await msgpack_decode(dreps);
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
}


async function new_async_sale_orders(db: PGClient, cache: RedisClient, domain: string, uid: string, oid?: string): Promise<void> {
  const result = await db.query("SELECT id, no, uid, vid, type, state, state_description, summary, payment, applicant, paid_at, start_at, stop_at, created_at, update_at, evtid FROM sale_orders WHERE deleted = FALSE" + (oid ? " AND o.id = $1" : ""), oid ? [oid] : []);
  const orders = []
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
      updated_at: row["update_at"],
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
}

async function async_sale_orders(db: PGClient, cache: RedisClient, domain: string, uid: string, oid?: string): Promise<void> {
  const result = await db.query("SELECT o.id AS o_id, o.no AS o_no, o.vid AS o_vid, o.type AS o_type, o.state_code AS o_state_code, o.state AS o_state, o.summary AS o_summary, o.payment AS o_payment, o.start_at AS o_start_at, o.stop_at AS o_stop_at, o.created_at AS o_created_at, o.updated_at AS o_updated_at, e.qid AS e_qid, e.opr_level AS e_opr_level, oi.id AS oi_id, oi.price AS oi_price, oi.piid AS oi_piid  FROM sale_order_ext AS e INNER JOIN orders AS o ON o.id = e.oid INNER JOIN plans AS p ON e.pid = p.id INNER JOIN plan_items AS pi ON p.id = pi.pid INNER JOIN order_items AS oi ON oi.piid = pi.id AND oi.oid = o.id WHERE o.deleted = FALSE AND e.deleted = FALSE AND p.deleted = FALSE AND pi.deleted = FALSE AND oi.deleted = FALSE" + (oid ? " AND o.id = $1" : ""), oid ? [oid] : []);
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

  const vehicles = [];

  for (const vid of vids) {
    const vrep = await rpc<Object>(domain, process.env["VEHICLE"], null, "getVehicle", vid);
    if (vrep["code"] === 200) {
      vehicles.push(vrep["data"]);
    }
  }

  for (const vehicle of vehicles) {
    for (const oid of oids) {
      const order = orders[oid];
      if (vehicle["id"] === order["vid"]) {
        order["vehicle"] = vehicle; // a vehicle may belong to many orders
      }
    }
  }

  const quotations = [];

  for (const qid of qids) {
    const qrep = await rpc<Object>(domain, process.env["QUOTATION"], null, "getQuotation", qid);
    if (qrep["code"] === 200) {
      quotations.push(qrep["data"]);
    }
  }

  for (const quotation of quotations) {
    for (const oid of oids) {
      const order = orders[oid];
      if (quotation["id"] === order["qid"]) {
        order["quotation"] = quotation;
        break; // a quotation only belongs to an order
      }
    }
  }

  const pps = pids.map(pid => rpc<Object>(domain, process.env["PLAN"], null, "getPlan", pid)); // fetch plan in parallel
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
  const multi = bluebird.promisifyAll(cache.multi()) as Multi;
  for (const oid of oids) {
    const order = orders[oid];
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
}

async function async_driver_orders(db: PGClient, cache: RedisClient, domain: string, uid: string, oid?: string): Promise<void> {
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
  const vehicles = [];
  const drivers = [];
  for (const oid of oids) {
    const order = orders[oid];
    const vrep = await rpc<Object>(domain, process.env["VEHICLE"], null, "getVehicle", order.vid);
    if (vrep["code"] === 200) {
      vehicles.push(vrep["data"]);
    }
    const dids = [... new Set(order.dids)];
    for (const did of dids) {
      const drvrep = await rpc<Object>(domain, process.env["VEHICLE"], null, "getDriver", order.vid, did);
      if (drvrep["code"] === 200) {
        drivers.push(drvrep["data"]);
      }
    }
  }
  for (const vehicle of vehicles) {
    for (const oid of oids) {
      const order = orders[oid];
      if (vehicle["id"] === order.vid) {
        vehicle["drivers"] = []
        order.vehicle = vehicle;
      }
    }
  }
  for (const driver of drivers) {
    for (const oid of oids) {
      const order = orders[oid];
      for (const drv of order.dids) {
        if (drv === driver["id"]) {
          order.vehicle.drivers.push(driver);
          order.drivers.push(driver);
        }
      }
    }
  }

  const multi = bluebird.promisifyAll(cache.multi()) as Multi;
  for (const oid of oids) {
    const order = orders[oid];
    const updated_at = order.updated_at.getTime();
    const uid = order["uid"];
    const vid = order["vid"];
    const newOrder = await msgpack_encode(order);
    multi.zadd("orders-" + order["vehicle"]["uid"], updated_at, oid);
    multi.zadd("driver_orders", updated_at, oid);
    multi.zadd("orders", updated_at, oid);
    multi.hset("vid-doid", vid, JSON.stringify(order["drivers"].map(d => d["id"])));
    multi.hset("driver-entities-", vid, JSON.stringify(order["drivers"]));
    multi.hset("order-entities", oid, newOrder);
    multi.zadd("driver-orders", updated_at, oid);
  }
  return multi.execAsync();
}




async function sync_plan_orders(db: PGClient, cache: RedisClient, domain: string, uid: string, oid?: string): Promise<void> {
  const result: QueryResult = await db.query("SELECT o.id AS o_id, o.no AS o_no, o.vid AS o_vid, o.type AS o_type, o.state_code AS o_state_code, o.state AS o_state, o.summary AS o_summary, o.payment AS o_payment, o.start_at AS o_start_at, o.stop_at AS o_stop_at, o.paid_at AS o_paid_at, o.created_at AS o_created_at, o.updated_at AS o_updated_at, e.qid AS e_qid, e.pid AS e_pid, e.service_ratio AS e_service_ratio, e.expect_at AS e_expect_at, oi.id AS oi_id, oi.price AS oi_price, oi.piid AS oi_piid FROM plan_order_ext AS e INNER JOIN orders AS o ON o.id = e.oid INNER JOIN plans AS p ON e.pid = p.id INNER JOIN plan_items AS pi ON p.id = pi.pid INNER JOIN order_items AS oi ON oi.piid = pi.id AND oi.oid = o.id WHERE o.deleted = FALSE AND e.deleted = FALSE AND oi.deleted = FALSE AND p.deleted = FALSE AND pi.deleted = FALSE" + (oid ? " AND o.id = $1" : ""), oid ? [oid] : []);
  const orders = {};
  try {
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

    const vehicles = [];
    const quotations = [];

    const maybe_driver_order_binaries = await cache.hvalsAsync("order-entities");
    const maybe_driver_orders = await Promise.all(maybe_driver_order_binaries.map(o => msgpack_decode(o)));
    const driver_orders = maybe_driver_orders.filter(o => o["type"] === 1);

    for (const vid of vids) {
      const vrep = await rpc<Object>(domain, process.env["VEHICLE"], uid, "getVehicle", vid);
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
        vehicles.push(vehicle);
      }
    }

    for (const vehicle of vehicles) {
      for (const oid of oids) {
        const order = orders[oid];
        if (vehicle["id"] === order["vid"]) {
          order["vehicle"] = vehicle; // a vehicle may belong to many orders
        }
      }
    }

    for (const qid of qids) {
      const qrep = await rpc<Object>(domain, process.env["QUOTATION"], uid, "getQuotation", qid);
      if (qrep["code"] === 200) {
        quotations.push(qrep["data"]);
      }
    }

    for (const quotation of quotations) {
      for (const oid of oids) {
        const order = orders[oid];
        if (quotation["id"] === order["qid"]) {
          order["quotation"] = quotation;
          break; // a quotation only belongs to an order
        }
      }
    }

    const pps = pids.map(pid => rpc<Object>(domain, process.env["PLAN"], uid, "getPlan", pid, true)); // fetch plan in parallel way
    const preps = await Promise.all(pps);
    const plans = preps.filter(p => p["code"] === 200).map(p => p["data"]);
    for (const oid of oids) {
      const order = orders[oid];
      for (const plan of plans) {
        delete plan["rules"];
        if (order["pids"].hasOwnProperty(plan["id"])) {
          order["plans"].push(plan); // a plan may belong to many orders
        }
        for (const planitem of plan["items"]) {
          delete planitem["description"];
          for (const orderitem of order["items"]) {
            if (planitem["id"] === orderitem["piid"]) {
              orderitem["plan_item"] = planitem;
            }
          }
        }
      }
    }

    const multi = bluebird.promisifyAll(cache.multi()) as Multi;
    for (const oid of oids) {
      const order = orders[oid];
      delete order["pids"];
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
      multi.hset("orderNo-id", order_no, oid);
      multi.hset("order-vid-" + vid, qid, oid);
      multi.hset("orderid-vid", oid, vid);
      multi.hset("order-entities", oid, newOrder);
      multi.zadd("plan-orders", updated_at, oid);
      multi.hset("VIN-orderID", order["vehicle"]["vin_code"], oid);
    }

    return multi.execAsync();
  } catch (e) {
    log.info(e + "in async plan_orders");
    throw e;
  }
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
      const newstrno = formatNum(String(strno), 7);
      let sum = 0;
      for (const i of pids) {
        const str = i.substring(24);
        const num = parseInt(str);
        sum += num;
      }
      const sum2 = sum + "";
      const sum1 = formatNum(sum2, 3);
      const order_no = "1" + "110" + "001" + sum1 + year + newstrno;
      await db.query("BEGIN");
      await db.query("INSERT INTO orders(id, vid, type, state_code, state, summary, payment, no) VALUES($1,$2,$3,$4,$5,$6,$7,$8)", [order_id, vid, type, state_code, state, summary, payment, order_no]);
      for (const pid of Object.keys(plans)) {
        await db.query("INSERT INTO plan_order_ext(oid, pmid, promotion, pid, qid, service_ratio, expect_at, vehicle_real_value) VALUES($1,$2,$3,$4,$5,$6,$7,$8)", [order_id, pmid, promotion, pid, qid, service_ratio, expect_at, v_value]);
        for (const piid in plans[pid]) {
          const item_id = uuid.v1();
          await db.query("INSERT INTO order_items(id, oid, piid, price) VALUES($1,$2,$3,$4)", [item_id, order_id, piid, plans[pid][piid]]);
        }
      }
      const prep = await rpc(domain, process.env["PLAN"], uid, "getAvailablePlans");
      if (prep["code"] === 200) {
        const pids = Object.keys(plans);
        const pls = prep["data"].filter(p => p["show_in_index"]).filter(p => pids.indexOf(p["id"]) != -1);
        for (const p of pls) {
          await createAccount(domain, vid, p["id"], uid);
        }
      }
      await db.query("COMMIT");
      await set_for_response(cache, cbflag, {
        code: 200,
        data: { id: order_id, no: order_no }
      });
      await sync_plan_orders(db, cache, domain, uid, order_id);
    } catch (e) {
      log.error(e);
      try {
        await db.query("ROLLBACK");
      } catch (e1) {
        log.error(e1);
      }
      await set_for_response(cache, cbflag, {
        code: 500,
        data: e.message
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
        const new_cm = await msgpack_encode(cm);
        await cache.lpushAsync("agent-customer-msg-queue", new_cm);
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
        await set_for_response(cache, cbflag, {
          code: 404,
          msg: "Order_id not found"
        });
        done();
        return;
      }
      await db.query("UPDATE orders SET no = $1 WHERE id = $2", [new_order_no, String(oid)]);
      const orderjson = await cache.hgetAsync("order-entities", oid);
      if (orderjson === null || orderjson === "") {
        await set_for_response(cache, cbflag, {
          code: 404,
          msg: "Order not found"
        });
        done();
        return;
      }
      const order = await msgpack_decode(orderjson);
      order["no"] = new_order_no;
      const newOrder = await msgpack_encode(order);
      const multi = bluebird.promisifyAll(cache.multi()) as Multi;
      multi.hdel("orderNo-id", order_no);
      multi.hset("orderNo-id", new_order_no, oid);
      multi.hset("order-entities", oid, newOrder);
      await multi.execAsync();
      await set_for_response(cache, cbflag, {
        code: 200,
        data: new_order_no
      });
      done();
    } catch (e) {
      log.error(e);
      await set_for_response(cache, cbflag, {
        code: 500,
        msg: e.message
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
  const vehicles = [];
  const drivers = [];
  for (const oid of oids) {
    const order = orders[oid];
    const vrep = await rpc<Object>(domain, process.env["VEHICLE"], null, "getVehicle", order.vid);
    if (vrep["code"] === 200) {
      vehicles.push(vrep["data"]);
    }
    const dids = [... new Set(order.dids)];
    for (const did of dids) {
      const drvrep = await rpc<Object>(domain, process.env["VEHICLE"], null, "getDriver", order.vid, did);
      if (drvrep["code"] === 200) {
        drivers.push(drvrep["data"]);
      }
    }
  }
  for (const vehicle of vehicles) {
    for (const oid of oids) {
      const order = orders[oid];
      if (vehicle["id"] === order.vid) {
        vehicle["drivers"] = []
        order.vehicle = vehicle;
      }
    }
  }
  for (const driver of drivers) {
    for (const oid of oids) {
      const order = orders[oid];
      for (const drv of order.dids) {
        if (drv === driver["id"]) {
          order.vehicle.drivers.push(driver);
          order.drivers.push(driver);
        }
      }
    }
  }

  const multi = bluebird.promisifyAll(cache.multi()) as Multi;
  for (const oid of oids) {
    const order = orders[oid];
    const updated_at = order.updated_at.getTime();
    const uid = order["uid"];
    const vid = order["vid"];
    const newOrder = await msgpack_encode(order);
    multi.zadd("orders-" + order["vehicle"]["uid"], updated_at, oid);
    multi.zadd("driver_orders", updated_at, oid);
    multi.zadd("orders", updated_at, oid);
    multi.hset("vid-doid", vid, JSON.stringify(order["drivers"].map(d => d["id"])));
    multi.hset("driver-entities-", vid, JSON.stringify(order["drivers"]));
    multi.hset("order-entities", oid, newOrder);
    multi.zadd("driver-orders", updated_at, oid);
  }
  return multi.execAsync();
}

processor.callAsync("placeAnDriverOrder", async (ctx: ProcessorContext, domain: string, uid: string, vid: string, dids: string[], summary: number, payment: number) => {
  log.info(`placeAnDriverOrder, domain: ${domain}, uid: ${uid}, vid: ${vid}, dids: ${JSON.stringify(dids)}, summary: ${summary}, payment: ${payment}`);
  const db: PGClient = ctx.db;
  const cache: RedisClient = ctx.cache;
  const order_id = uuid.v1();
  const item_id = uuid.v1();
  const event_id = uuid.v1();
  const state_code = 2;
  const state = "已支付";
  const type = 1;
  const data = {
    msg: "添加驾驶人"
  };
  try {
    await db.query("BEGIN");
    await db.query("INSERT INTO order_events(id, oid, uid, data) VALUES($1,$2,$3,$4)", [event_id, order_id, uid, data]);
    await db.query("INSERT INTO orders(id, vid, type, state_code, state, summary, payment) VALUES($1,$2,$3,$4,$5,$6,$7)", [order_id, vid, type, state_code, state, summary, payment]);

    for (const did of dids) {
      await db.query("INSERT INTO driver_order_ext(oid,pid) VALUES($1,$2)", [order_id, did]);
    }
    await db.query("COMMIT");
    //await updateAccount(domain, vid, uid, payment, cache);
    await sync_driver_orders(db, cache, domain, uid, order_id);
    const result = await db.query("SELECT p.oid FROM orders AS o INNER JOIN plan_order_ext AS p ON o.id = p.oid WHERE o.vid = $1", [vid]);
    if (result.rows.length > 0) {
      const poid = result.rows[0].oid;
      await sync_plan_orders(db, cache, domain, uid, poid);
    }
    log.info("placeAnDriverOrder done");
    return {
      code: 200,
      data: order_id
    };
  } catch (e) {
    log.error(e);
    await db.query("ROLLBACK");
    return {
      code: 500,
      msg: e.message
    };
  }
});

async function updateAccount(domain: string, vid: string, pid: string, plan: string, openid: string, no: string, oid: string, uid: string, title: string, payment: number, cache: any): Promise<any> {
  log.info("plan title" + plan);
  const balance = payment;
  let balance0: number = null;
  let balance1: number = null;
  const gid = await cache.hgetAsync("vid-gid", vid);
  if (gid !== null) {
    const group_entities = await cache.hgetAsync("group-entities", gid);
    const apportion: number = group_entities["apportion"];
    balance0 = payment * apportion;
    balance1 = payment * (1 - apportion);
  } else {
    balance0 = payment * 0.2;
    balance1 = payment * 0.8;
  }
  const postData = queryString.stringify({
    "user": openid,
    "OrderNo": no,
    "Plan": "好司机 互助计划",
    "Price": payment,
    "orderId": oid
  });
  const options = {
    hostname: wxhost,
    port: 80,
    path: "/wx/wxpay/tmsgPaySuccess",
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
  return rpc(domain, process.env["WALLET"], uid, "updateAccountBalance", vid, pid, 1, 1, balance0, balance1, 0, title, oid, uid);
}

async function createAccount(domain: string, vid: string, pid: string, uid: string): Promise<any> {
  return rpc(domain, process.env["WALLET"], uid, "createAccount", vid, pid, uid);
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
      const orep = await db.query("SELECT state_code FROM orders WHERE id = $1", [order_id]);
      const old_state_code = parseInt(orep["rows"][0]["state_code"]);
      if (state_code === 2) {
        if (old_state_code == state_code) {
          set_for_response(cache, cbflag, {
            code: 300,
            msg: "重复更新订单状态"
          });
          done();
          return;
        } else if (old_state_code === 1) {
          await db.query("UPDATE orders SET state_code = $1, state = $2, paid_at = $3, updated_at = $4 WHERE id = $5", [state_code, state, paid_at, paid_at, order_id]);
        } else {
          set_for_response(cache, cbflag, {
            code: 300,
            msg: "不能倒序更改订单状态"
          });
          done();
          return;
        }
      } else {
        await db.query("UPDATE orders SET state_code = $1, state = $2, updated_at = $3 WHERE id = $4", [state_code, state, paid_at, order_id]);
      }
      const orderjson = await cache.hgetAsync("order-entities", order_id);
      if (orderjson === null || orderjson === "") {
        await set_for_response(cache, cbflag, {
          code: 404,
          msg: `Order ${order_id} not found in order-entities`
        });
        done();
        return;
      }
      const order = await msgpack_decode(orderjson);
      const openid = await cache.hgetAsync("wxuser", uid);
      const multi = bluebird.promisifyAll(cache.multi()) as Multi;
      const updated_at = (new Date()).getTime();
      order["state_code"] = state_code;
      order["state"] = state;
      order["updated_at"] = paid_at;
      if (state_code === 2) {
        order["paid_at"] = paid_at;
        multi.zrem("new-orders-id", order_id);
        multi.zadd("new-pays-id", updated_at, order_id);
      }
      const newOrder = await msgpack_encode(order);
      multi.hset("order-entities", order_id, newOrder);
      await multi.execAsync();
      if (state_code === 2 && old_state_code === 1) {
        const title = "加入计划充值";
        const plan = order["plans"].filter(p => p["show_in_index"]);
        log.info("plan" + JSON.stringify(plan));
        await updateAccount(domain, order["vehicle"]["id"], plan ? plan[0]["id"] : null, plan ? plan[0]["title"] : "好车主/互助计划", String(openid), order["no"], order["id"], uid, title, order["summary"], cache);
      }
      await set_for_response(cache, cbflag, {
        code: 200,
        data: order_id
      });
      done();
    } catch (err) {
      await set_for_response(cache, cbflag, {
        code: 500,
        msg: err.message
      });
      done();
    }
  })();
});



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
      await set_for_response(cache, cbflag, {
        code: 500,
        msg: err.message
      });
      done();
    }
    try {
      await set_for_response(cache, cbflag, {
        code: 200,
        data: order_id
      });
      await sync_sale_orders(db, cache, domain, uid, order_id);
      done();
    } catch (e) {
      log.error(e);
      done();
    }
  })();
});

processor.call("updateSaleOrder", (ctx: ProcessorContext, domain: any, order_id: string, items: Object[], summary: number, payment: number, cbflag: string) => {
  log.info(`upateSaleOrder, domain: ${domain}, order_id: ${order_id}, items: ${items}, summary: ${summary}, payment: ${payment}, cbflag: ${cbflag}`);
  const db: PGClient = ctx.db;
  const cache: RedisClient = ctx.cache;
  const done = ctx.done;
  const update_at = new Date();
  const piids: Object[] = [];
  const prices: Object[] = [];
  for (const item in items) {
    piids.push(item);
    prices.push(items[item]);
  }
  (async () => {
    try {
      await db.query("BEGIN");
      await db.query("UPDATE orders SET summary = $1, payment = $2, updated_at= $3 WHERE id = $4", [summary, payment, update_at, order_id]);

      for (let i = 0, len = Math.min(prices.length, piids.length); i < len; i++) {
        const piid = piids[i];
        const price = prices[i];
        await db.query("UPDATE order_items SET price = $1 WHERE piid = $2 AND oid = $3", [price, piid, order_id]);
      }
      await db.query("COMMIT");
    } catch (err) {
      log.error(err);
      try {
        await db.query("ROLLBACK");
      } catch (err1) {
        log.error(err1);
      }
      await set_for_response(cache, cbflag, {
        code: 500,
        msg: err.message
      });
      done();
    }
    try {
      await set_for_response(cache, cbflag, {
        code: 200,
        data: order_id
      });
      await sync_sale_orders(db, cache, domain, order_id);
      done();
    } catch (e) {
      log.error(e);
      done();
    }
  })();
});

// 刷新所有
async function refresh_driver_orders(db: PGClient, cache: RedisClient, domain: string): Promise<void> {
  return sync_driver_orders(db, cache, domain, null);
}

async function refresh_plan_orders(db: PGClient, cache: RedisClient, domain: string): Promise<void> {
  return sync_plan_orders(db, cache, domain, null);
}

async function refresh_sale_orders(db: PGClient, cache: RedisClient, domain: string): Promise<void> {
  return sync_sale_orders(db, cache, domain, null);
}

// 刷新一个
async function refresh_driver_order(db: PGClient, cache: RedisClient, domain: string, uid: string, oid: string): Promise<void> {
  return sync_driver_orders(db, cache, domain, uid, oid);
}

async function refresh_plan_order(db: PGClient, cache: RedisClient, domain: string, uid: string, oid: string): Promise<void> {
  return sync_plan_orders(db, cache, domain, uid, oid);
}

async function refresh_sale_order(db: PGClient, cache: RedisClient, domain: string, uid: string, oid: string): Promise<void> {
  return sync_sale_orders(db, cache, domain, uid, oid);
}


processor.call("refresh_order", (ctx: ProcessorContext, domain: string, type: number, uid: string, oid: string) => {
  log.info("refresh a plan order");
  const db: PGClient = ctx.db;
  const cache: RedisClient = ctx.cache;
  const done = ctx.done;
  (async () => {
    try {
      if (type === 1) {
        await refresh_plan_order(db, cache, domain, uid, oid);
      } else if (type === 2) {
        await refresh_driver_order(db, cache, domain, uid, oid);
      } else if (type === 3) {
        await refresh_sale_order(db, cache, domain, uid, oid);
      } else {
        log.info("type error");
      }
      log.info("refresh plan order done!");
      done();
    } catch (e) {
      log.error(e);
    }
  })();
});

processor.call("refresh", (ctx: ProcessorContext, domain: string) => {
  log.info("refresh");
  const db: PGClient = ctx.db;
  const cache: RedisClient = ctx.cache;
  const done = ctx.done;
  (async () => {
    try {
      await cache.delAsync("order-entities");
      await refresh_driver_orders(db, cache, domain);
      await refresh_plan_orders(db, cache, domain);
      await refresh_sale_orders(db, cache, domain);
      log.info("refresh done!");
      done();
    } catch (e) {
      log.error(e);
    }
  })();
});
