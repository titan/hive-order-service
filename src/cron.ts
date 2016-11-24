import { Processor, Config, ModuleFunction, DoneFunction, rpc, async_serial, async_serial_ignore } from "hive-processor";
import * as nanomsg from "nanomsg";
import * as msgpack from "msgpack-lite";
import * as bunyan from "bunyan";
import * as uuid from "node-uuid";
import { servermap, triggermap } from "hive-hostmap";
import * as schedule from "node-schedule";
import { verify, uuidVerifier, stringVerifier, arrayVerifier, objectVerifier, booleanVerifier } from "hive-verify";
import { createClient, RedisClient } from "redis";
const pg = require("pg");
const ResultSet = require("pg");



let log = bunyan.createLogger({
  name: "timing-cron",
  streams: [
    {
      level: "info",
      path: "/var/log/timing-cron-info.log",  // log ERROR and above to a file
      type: "rotating-file",
      period: "1d",   // daily rotation
      count: 7        // keep 7 back copies
    },
    {
      level: "error",
      path: "/var/log/timing-cron-error.log",  // log ERROR and above to a file
      type: "rotating-file",
      period: "1w",   // daily rotation
      count: 3        // keep 7 back copies
    }
  ]
});
let redis = createClient(process.env["CACHE_PORT"], process.env["CACHE_HOST"]);
let config = {
  user: process.env["DB_USER"], // env var: PGUSER
  host: process.env["DB_HOST"],
  database: process.env["DB_NAME"], // env var: PGDATABASE
  password: process.env["DB_PASSWORD"], // env var: PGPASSWORD
  port: process.env["DB_PORT"], // env var: PGPORT
  max: 10, // max number of clients in the pool
  idleTimeoutMillis: 30000, // how long a client is allowed to remain idle before being closed
};
let pool = new pg.Pool(config);

function checkEffectiveTime(start_at) {
  let startTime = start_at.getTime();
  let nowDate = (new Date()).getTime();
  if (startTime < nowDate) {
    return true;
  } else {
    return false;
  }
}


function checkInvalidTime(created_at) {
  let createdTime = new Date(created_at).getTime();
  let nowDate = (new Date()).getTime();
  let timeDifference = nowDate - createdTime;
  if (timeDifference > 86400000) {
    return true;
  } else {
    return false;
  }
}

function trim(str: string) {
  if (str) {
    return str.trim();
  } else {
    return null;
  }
}

function proportion(len: number) {
  let basis: number = 20;
  let result: number = null;
  for (let i = 0; i < len - 1; i++) {
    result += 5;
  }
  return ((basis + result) / 100);
}



function update_group_vehicles_recursive(db, done, nowdate, vids, acc, cb) {
  if (vids.length === 0) {
    done();
    cb(acc);
  } else {
    let vid = vids.shift();
    db.query("UPDATE group_vehicles SET type = $1, updated_at = $2 WHERE id = $3", [1, nowdate, vid], (err, result) => {
      if (err) {
        log.info(err);
        update_group_vehicles_recursive(db, done, nowdate, vids, acc, cb);
      }
      else {
        redis.hget("vid-gid", vid, function (err, result) {
          if (err) {
            log.info(`this ${vid} err to get gid`);
          } else if (result === "" || result === null) {
            log.info(`not found gid ,this vid is ${vid}`);
          } else {
            redis.hget("group-entities", result, function (err1, result1) {
              if (err || result1 === "" || result === null) {
                log.info(`use this gid is ${result} to get group-entities err`);
              } else {
                let group_entities = JSON.parse(result1);
                let waiting_vehicles = group_entities["waiting_vehicles"];
                let new_waiting_vehicles = waiting_vehicles.filter(v => v["id"] !== vid);
                let new_join_vehicle = waiting_vehicles.filter(v1 => v1["id"] === vid);
                let new_joined_vehicles = group_entities["joined_vehicles"].push(new_join_vehicle);
                group_entities["waiting_vehicles"] = new_waiting_vehicles;
                group_entities["joined_vehicles"] = new_joined_vehicles;
                let group = { gid: result, group: group_entities };
                acc.push(group);
                update_group_vehicles_recursive(db, done, nowdate, vids, acc, cb);
              }
            });
          }
        });
      }
    });
  }
}


function update_order_recursive(db, done, nowdate, oids, acc, cb) {
  if (oids.length === 0) {
    cb(acc);
    done();
  }
  else {
    let oid = oids.shift();
    db.query("UPDATE orders SET state_code = $1,state = $2, updated_at = $3 WHERE id = $4", [4, "已生效", nowdate, oid], (err, result) => {
      if (err) {
        log.info(err);
        done();
      }
      else {
        let order_entities: Object = {};
        redis.hget("order-entities", oid, function (err, result) {
          if (err) {
            log.info(`get order_entities for this oid = ${oid} err`);
            done();
          } else if (result === "" || result === null) {
            log.info(`not found order_entities to this ${oid}`);
            done();
          } else {
            let p = rpc("admin", servermap["plan"], null, "increaseJoinedCount", "00000000-0000-0000-0000-000000000001");
            p.then((p) => {
              if (err || p["code"] !== 200) {
                log.info(`call plan error for this ${oid}`);
                oids.push(oid);
                update_order_recursive(db, done, nowdate, oids, acc, cb);
              } else {
                order_entities = JSON.parse(result);
                order_entities["state"] = "已生效";
                order_entities["state_code"] = 4;
                let order = { oid: oid, order: order_entities };
                acc.push(order);
                update_order_recursive(db, done, nowdate, oids, acc, cb);
              }
            });
          }
        });
      }
    });
  }
}
function get_order_uid_recursive(db, done, nowdate, orders, acc, cb) {
  if (orders.length === 0) {
    cb(acc);
  } else {
    let order = orders.shift();
    db.query("UPDATE orders SET state_code = $1,state = $2, updated_at = $3,deleted = $4 WHERE id = $5", [5, "已失效", nowdate, true, order["id"]], (err, result) => {
      if (err) {
        log.info(err);
        get_order_uid_recursive(db, done, nowdate, orders, acc, cb);
      }
      else {
        let p = rpc<Object>("mobile", servermap["vehicle"], null, "getVehicle", order["vid"]);
        p.then((v) => {
          if (v["code"] === 200) {
            let vehicle = v["data"];
            order["uid"] = vehicle["user_id"];
            acc.push(order);
            get_order_uid_recursive(db, done, nowdate, orders, acc, cb);
          } else {
            get_order_uid_recursive(db, done, nowdate, orders, acc, cb);
          }
        });
      }
    });
  }
}

//-----订单生效------
let rule = new schedule.RecurrenceRule();
rule.hour = 0;
rule.minute = 1;
rule.second = 0;
let timing = schedule.scheduleJob(rule, function () {
  const pdo = orderEffective();
  let ps = [pdo];
  async_serial_ignore<void>(ps, [], () => {
    log.info("orderEffective done!");
  });
});

//-----订单失效------
let rule1 = new schedule.RecurrenceRule();
rule1.hour = 0;
rule1.minute = 3;
rule1.second = 0;
let timing1 = schedule.scheduleJob(rule1, function () {
  const pdo = orderInvalid();
  let ps = [pdo];
  async_serial_ignore<void>(ps, [], () => {
    log.info("orderInvalid done");
  });
});

//------互助组每日比例变化
// let rule2 = new schedule.RecurrenceRule();
// rule1.hour = 0;
// rule1.minute = 5;
// rule1.second = 0;
// let timing2 = schedule.scheduleJob(rule2, function () {
//   const pdo = orderInvalid();
//   let ps = [pdo];
//   async_serial_ignore<void>(ps, [], () => {
//     log.info("");
//   });
// });




function orderEffective() {
  return new Promise<void>((resolve, reject) => {
    pool.connect(function (err, db, done) {
      if (err) {
        log.info("error fetching client from pool" + err);
      } else {
        db.query("SELECT id, no, vid, type, state_code, state, summary, payment, start_at FROM orders WHERE state_code = 3 AND type = 0 ", [], (err, result) => {
          if (err) {
            reject(err);
            log.info("SELECT orders err" + err);
          } else if (result === null) {
            reject("404 not found");
            log.info("not found prepare effective order");
          } else {
            const orders: Object[] = [];
            for (const row of result.rows) {
              const order = {
                id: row.id,
                no: row.no,
                vid: row.vid,
                type: row.type,
                state_code: row.state_code,
                state: row.state,
                summary: row.summary,
                payment: row.payment,
                start_at: row.start_at,
              };
              orders.push(order);
            }
            const effectiveOrders = [];
            let efos = orders.filter(o => checkEffectiveTime(o["start_at"]) === true).map(o => o);
            let oids = [];
            let vids = [];
            for (let efo of efos) {
              efo["state"] = "已生效";
              efo["state_code"] = 4;
              oids.push(efo["id"]);
              vids.push(efo["vid"]);
            }
            let nowdate = new Date();
            update_order_recursive(db, done, nowdate, oids.map(oid => oid), [], (order_entities) => {
              update_group_vehicles_recursive(db, done, nowdate, vids.map(vid => vid), [], (group_entities) => {
                let multi = redis.multi();
                for (let group_entitie of group_entities) {
                  multi.hset("group-entities", group_entitie["gid"], JSON.stringify(group_entitie["group"]));
                }
                for (let order_entitie of order_entities) {
                  multi.hset("order-entities", order_entitie["oid"], JSON.stringify(order_entitie["order"]));
                }
                multi.exec((err, replise) => {
                  if (err) {
                    log.info(err);
                    reject(err);
                    done();
                  }
                  else {
                    log.info("all exec success  and  done");
                    resolve();
                    done();
                  }
                });
              });
            });
          }
        });
      }
    });
  });
};

function orderInvalid() {
  return new Promise<void>((resolve, reject) => {
    pool.connect(function (err, db, done) {
      if (err) {
        log.info("error fetching client from pool" + err);
      } else {
        db.query("SELECT id, no, vid, state_code, state, created_at FROM orders WHERE state_code = 1 AND type = 0", [], (err, result) => {
          if (err) {
            reject(err);
            log.info("SELECT orders err" + err);
          } else if (result === null || result === "") {
            reject("404 not found");
            log.info("not found prepare effective order");
          } else {
            const orders: Object[] = [];
            for (const row of result.rows) {
              const order = {
                id: row.id,
                no: row.no,
                uid: null,
                vid: row.vid,
                state_code: row.state_code,
                created_at: row.created_at,
              };
              orders.push(order);
            }
            let invalidOrders = orders.filter(o => checkInvalidTime(o["created_at"]) === true).map(o => o);
            for (let invalidOrder of invalidOrders) {
              invalidOrder["state"] = "已失效";
              invalidOrder["state_code"] = 5;
            }
            let nowdate = new Date();
            get_order_uid_recursive(db, done, nowdate, invalidOrders.map(invalidOrder => invalidOrder), [], (delorders) => {
              let multi = redis.multi();
              for (let delorder of delorders) {
                multi.hdel("order-entities", delorder["id"]);
                multi.hdel("orderid-vid", delorder["id"]);
                multi.zrem("orders", delorder["id"]);
                multi.zrem(`orders-${delorder["uid"]}`, delorder["id"]);
                multi.zrem("plan-orders", delorder["id"]);
              }
              multi.exec((err, result) => {
                if (err) {
                  log.info(err);
                  reject(err);
                  done();
                } else {
                  log.info("all exec success  and  done");
                  resolve();
                  done();
                }
              });
            });
          };
        });
      }
    });
  });
}

// function updateGroupScale() {
//   return new Promise<void>((resolve, reject) => {
//     pool.connect(function (err, db, done) {
//       if (err) {
//         log.info("error fetching client from pool" + err);
//       } else {
//         db.query("SELECT g.id AS g_id, g.name AS g_name, g.founder AS g_founder , g.apportion AS g_apportion, gv.id AS gv_id, gv.gid AS gv_gid, gv.vid AS gv_vid, gv.type AS gv_type FROM groups AS g LEFT JOIN group_vehicles AS gv ON g.id = gv.gid WHERE g.deleted = false AND gv.type = 1", [], (err, result) => {
//           if (err) {
//             reject(err);
//             log.info("SELECT group error");
//           } else {
//             const groups: Object = {};
//             for (const row of result.rows) {
//               if (groups.hasOwnProperty(row.g_id)) {
//                 groups[row.g_id]["vids"].push(row.gv_vid);
//               } else {
//                 const group = {
//                   id: row.g_id,
//                   name: row.g_name,
//                   founder: row.g_founder,
//                   apportion: row.g_apportion,
//                   vids: []
//                 };
//                 groups[row.g_id] = group;
//               }
//             }
//             const gids = Object.keys(groups);
//             for (let gid of gids) {
//               let len = groups["gid"];
//               groups["gid"]["apportion"] = proportion(len);
//             }

//           }
//         });
//       }
//     });
//   });
// }



log.info("Start timing-cron");
