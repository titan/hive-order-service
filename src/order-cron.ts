import { Processor, Config, ModuleFunction, DoneFunction, rpc, async_serial, async_serial_ignore } from "hive-processor";
import * as nanomsg from "nanomsg";
import * as msgpack from "msgpack-lite";
import * as bunyan from "bunyan";
import * as uuid from "node-uuid";
import { servermap, triggermap } from "hive-hostmap";
import * as schedule from "node-schedule";
import { verify, uuidVerifier, stringVerifier, arrayVerifier, objectVerifier, booleanVerifier } from "hive-verify";
const Redis = require('redis');
const createClient = require('redis');
const RedisClient = require('redis');
const pg = require('pg');
const ResultSet = require('pg');



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
let redis = Redis.createClient(6379, process.env["CACHE_HOST"]);
let config = {
  user: process.env["DB_USER"], //env var: PGUSER
  host: process.env["DB_HOST"],
  database: process.env["DB_NAME"], //env var: PGDATABASE
  password: process.env["DB_PASSWORD"], //env var: PGPASSWORD
  port: process.env["DB_PORT"], //env var: PGPORT
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



function update_group_vehicles_recursive(db, done, redis, vids, acc, cb) {
  if (vids.length === 0) {
    done();
    cb(acc);
  }
  else {
    let vid = vids.shift();
    db.query("UPDATE group_vehicles SET type = $1 WHERE id = $2", [1, vid], (err, result) => {
      if (err) {
        log.info(err);
        done();
      }
      else {
        redis.hget("vid-gid", vid, function (err, result) {
          if (err) {
            log.info(`this ${vid} err to get gid`);
          } else if (result == "") {
            log.info(`not found gid ,this vid is ${vid}`);
          } else {
            redis.hget("group-entities", result, function (err1, result1) {
              if (err || result1 == "") {
                log.info(`use this gid is ${result} to get group-entities err`);
              } else {
                let group_entities = JSON.parse(result1);
                let waiting_vehicles = group_entities["waiting_vehicles"];
                let new_waiting_vehicles = waiting_vehicles.filter(v => v["id"] !== vid);
                let new_join_vehicle = waiting_vehicles.filter(v1 => v1["id"] === vid);
                let new_joined_vehicles = group_entities["joined_vehicles"].push(new_join_vehicle);
                group_entities["waiting_vehicles"] = new_waiting_vehicles;
                group_entities["joined_vehicles"] = new_joined_vehicles;
                // cache.hset("group-entities", result, JSON.parse(group_entities));
                let group = { gid: result, group: group_entities };
                acc.push(group);
                update_group_vehicles_recursive(db, done, redis, vids, acc, cb);
              }
            });
          }
        });
      }
    });
  }
}


function update_order_recursive(db, done, redis, oids, acc, cb) {
  if (oids.length === 0) {
    cb(acc);
    done();
  }
  else {
    let oid = oids.shift();
    db.query("UPDATE orders SET state_code = $1,state = $2 WHERE id = $3", [4, "已生效", oid], (err, result) => {
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
          } else if (result == "") {
            log.info(`not found order_entities to this ${oid}`);
            done();
          } else {
            order_entities = JSON.parse(result);
            order_entities["state"] = "已生效";
            order_entities["state_code"] = 4;
            let order = { oid: oid, order: order_entities };
            acc.push(order);
            update_order_recursive(db, done, redis, oids, acc, cb);
          }
        });
      }
    });
  }
}
function get_order_uid_recursive(db, done, redis, orders, acc, cb) {
  if (orders.length === 0) {
    cb(acc);
    done();
  } else {
    let order = orders.shift();
    let p = rpc<Object>("mobile", servermap["vehicle"], null, "getVehicle", order["vid"]);
    p.then((v) => {
      if (v["code"] === 200) {
        let vehicle = v["data"];
        order["uid"] = vehicle["user_id"];
        acc.push(order);
      }
    });
  }
}


let rule = new schedule.RecurrenceRule();
rule.hour = 0;
rule.minute = 1;
rule.second = 0;
let timing = schedule.scheduleJob(rule, function () {
  const pdo = orderEffective();
  let ps = [pdo];
  async_serial_ignore<void>(ps, [], () => {
    log.info("refresh done!");
  });
});
let rule1 = new schedule.RecurrenceRule();
rule1.hour = 0;
rule1.minute = 11;
rule1.second = 0;
let timing1 = schedule.scheduleJob(rule1, function () {
  const pdo = orderInvalid();
  let ps = [pdo];
  async_serial_ignore<void>(ps, [], () => {
    log.info("refresh done!");
  });
});




function orderEffective() {
  return new Promise<void>((resolve, reject) => {
    pool.connect(function (err, db, done) {
      if (err) {
        log.info("error fetching client from pool" + err);
      } else {
        db.query("SELECT id, no, vid, type, state_code, state, summary, payment, start_at FROM orders WHERE id = '19f30e30-9825-11e6-8194-5f70de362af2' ", [], (err, result) => {
          if (err) {
            reject(err);
            log.info("SELECT orders err" + err);
          } else if (result === null) {
            reject("404 not found");
            log.info("not found prepare effective order");
          } else {
            console.log("================1" + result);
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
              }
              orders.push(order);
            }
            const effectiveOrders = [];
            let efos = orders.filter(o => checkEffectiveTime(o["start_at"]) === true).map(o => o)
            console.log("===============2" + efos);
            let oids = [];
            let vids = [];
            for (let efo of efos) {
              efo["state"] = "已生效";
              efo["state_code"] = 4;
              oids.push(efo["id"]);
              vids.push(efo["vid"]);
            }
            update_order_recursive(db, done, redis, oids.map(oid => oid), [], (order_entities) => {
              update_group_vehicles_recursive(db, done, redis, vids.map(vid => vid), [], (group_entities) => {
                let multi = redis.mulit;
                for (let order_entitie of order_entities) {
                  for (let group_entitie of group_entities) {
                    multi.hset("order-entities", order_entitie["oid"], order_entitie["order"]);
                    multi.hset("group-entities", group_entitie["gid"], group_entitie["group"]);
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
                  }
                }
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
        db.query("SELECT id, no, vid, state_code, state, created_at FROM orders WHERE state_code = 0", [], (err, result) => {
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
                uid: null,
                vid: row.vid,
                state_code: row.state_code,
                created_at: row.created_at,
              }
              orders.push(order);
            }
            let invalidOrders = orders.filter(o => checkInvalidTime(o["created_at"]) === true).map(o => o);
            for (let invalidOrder of invalidOrders) {
              invalidOrder["state"] = "已失效";
              invalidOrder["state_code"] = 5;
            }
            get_order_uid_recursive(db, done, redis, invalidOrders.map(order => order), {}, (orders) => {
              let mulit = redis.mulit;
              for (let order of orders) {
                mulit.hdel("order_entitie", order["id"]);
                mulit.hdel("orderid-vid", order["id"]);
                mulit.zrem("orders", order["id"]);
                mulit.zrem(`orders-${order["uid"]}`, order["id"]);
                mulit.hdel("orderNo-id", order["no"]);
                mulit.zrem("plan-orders", order["id"]);
              }
              mulit.exec((err, result) => {
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
            }
            )
          };
        });
      }
    });
  });
}







log.info("Start timing-cron");
