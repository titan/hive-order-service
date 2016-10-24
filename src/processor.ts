import { Processor, Config, ModuleFunction, DoneFunction, rpc, async_serial, async_serial_ignore } from "hive-processor";
import { Client as PGClient, ResultSet } from "pg";
import { createClient, RedisClient } from "redis";
import { servermap, triggermap } from "hive-hostmap";
import * as bunyan from "bunyan";
import * as uuid from "node-uuid";
import * as msgpack from "msgpack-lite";
import * as nanomsg from "nanomsg";
import * as http from "http";
import * as queryString from "querystring";
let log = bunyan.createLogger({
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

let config: Config = {
  dbhost: process.env["DB_HOST"],
  dbuser: process.env["DB_USER"],
  dbport: process.env["DB_PORT"],
  database: process.env["DB_NAME"],
  dbpasswd: process.env["DB_PASSWORD"],
  cachehost: process.env["CACHE_HOST"],
  addr: "ipc:///tmp/order.ipc"
};
let processor = new Processor(config);
let order_trigger = nanomsg.socket("pub");
order_trigger.bind(triggermap.order);
let underwrite_trigger = nanomsg.socket("pub");
underwrite_trigger.bind(triggermap.underwrite);

let wxhost = process.env["WX_ENV"] === "test" ? "dev.fengchaohuzhu.com" : "m.fengchaohuzhu.com";

function formatNum(Source: string, Length: number): string {
  let strTemp = "";
  for (let i = 1; i <= Length - Source.length; i++) {
    strTemp += "0";
  }
  return strTemp + Source;
}

function getLocalTime(nS) {
  return new Date(parseInt(nS) * 1000).toLocaleString().replace(/:\d{1,2}$/, " ");
}

function insert_sale_order_items_recursive(db, done, order_id, pid, items, piids, acc, cb) {
  if (piids.length === 0) {
    cb(acc);
  } else {
    let item_id = uuid.v1();
    let piid = piids.shift();
    db.query("INSERT INTO order_items(id,piid,pid, price) VALUES($1,$2,$3,$4)", [item_id, piid, pid, items[piid]], (err: Error) => {
      if (err) {
        log.info(err);
        db.query("ROLLBACK", [], (err: Error) => {
          log.error(err, "insert into order_items error");
          done();
        });
      } else {
        acc.push(item_id);
        insert_sale_order_items_recursive(db, done, order_id, pid, items, piids, acc, cb);
      }
    });
  }
}

function update_sale_order_items_recursive(db, done, prices, piids, acc, cb) {
  if (piids.length === 0) {
    cb(acc);
  } else {
    let piid = piids.shift();
    let price = prices.shift();
    db.query("UPDATE order_items SET price = $1 WHERE id = $2", [price, piid], (err: Error, result: ResultSet) => {
      if (err) {
        log.info(err);
        db.query("ROLLBACK", [], (err: Error) => {
          log.error(err, "insert into order_items error");
          done();
        });
      } else {
        update_sale_order_items_recursive(db, done, prices, piids, acc, cb);
      }
    });
  }
}

function insert_order_item_recursive(db, done, plans, pid, args1, piids, acc, cb) {
  if (piids.length === 0) {
    cb(acc);
  } else {
    let item_id = uuid.v1();
    let piid = piids.shift();
    db.query("INSERT INTO order_items(id, pid, piid, price) VALUES($1,$2,$3,$4)", [item_id, pid, piid, plans[pid][piid]], (err: Error) => {
      if (err) {
        log.info(err);
        db.query("ROLLBACK", [], (err: Error) => {
          log.error(err, "insert into order_items error");
          done();
        });
      } else {
        acc.push(item_id);
        insert_order_item_recursive(db, done, plans, pid, args1, piids, acc, cb);
      }
    });
  }
}

function insert_plan_order_recursive(db, done, order_id, args1, pids, acc/* plan-id: [order-item-id] */, cb) {
  if (pids.length === 0) {
    db.query("COMMIT", [], (err: Error) => {
      if (err) {
        log.info(err);
        log.error(err, "insert plan order commit error");
        done();
      } else {
        cb(acc);
      }
    });
  } else {
    let ext_id = uuid.v1();
    let pid = pids.shift();
    db.query("INSERT INTO plan_order_ext(oid, pmid, promotion, pid, qid, service_ratio, expect_at) VALUES($1,$2,$3,$4,$5,$6,$7)", [order_id, args1.pmid, args1.promotion, pid, args1.qid, args1.service_ratio, args1.expect_at], (err: Error, result: ResultSet) => {
      if (err) {
        log.info(err);
        db.query("ROLLBACK", [], (err: Error) => {
          log.error(err, "insert into plan_order_ext error");
          done();
        });
      } else {
        let piids = [];
        for (let piid in args1.plans[pid]) {
          piids.push(piid);
        }
        insert_order_item_recursive(db, done, args1.plans, pid, args1, piids, [], (oiids) => {
          acc[pid] = oiids;
          insert_plan_order_recursive(db, done, order_id, args1, pids, acc, cb);
        });
      }
    });
  }
}

function insert_driver_order_recursive(db, done, order_id, args2, dids, acc/* plan-id: [order-item-id] */, cb) {
  if (dids.length === 0) {
    db.query("COMMIT", [], (err: Error) => {
      if (err) {
        log.info(err);
        log.error(err, "insert plan order commit error");
        done();
      } else {
        cb(acc);
      }
    });
  } else {
    let did = dids.shift();
    db.query("INSERT INTO driver_order_ext(oid,pid) VALUES($1,$2)", [order_id, did], (err: Error, result: ResultSet) => {
      if (err) {
        log.info(err);
        db.query("ROLLBACK", [], (err: Error) => {
          log.error(err, "insert into driver_order_ext error");
          done();
        });
      } else {
        insert_driver_order_recursive(db, done, order_id, args2, dids, acc, cb);
      }
    });
  }
}

function async_serial_driver(ps: Promise<any>[], acc: any[], cb: (vals: any[]) => void) {
  if (ps.length === 0) {
    cb(acc);
  } else {
    let p = ps.shift();
    p.then(v => {
      let val = v["data"];
      acc.push(val);
      async_serial_driver(ps, acc, cb);
    })
      .catch(e => {
        async_serial_driver(ps, acc, cb);
      });
  }
}

function increase_order_no(cache): Promise<number> {
  return new Promise<number>((resolve, reject) => {
    cache.incr("order-no", (err: Error, no: number) => {
      if (err) {
        reject(err);
      } else {
        resolve(no);
      }
    });
  });
}

processor.call("placeAnPlanOrder", (db: PGClient, cache: RedisClient, done: DoneFunction, domain: string, uid: string, order_id: string, vid: string, plans: Object[], qid: string, pmid: string, promotion: number, service_ratio: number, summary: number, payment: number, v_value: number, expect_at: any, cbflag: string) => {
  log.info("placeOrder");
  let event_id = uuid.v1();
  let state_code = 1;
  let state = "已创建订单";
  let type = 0;
  let plan_data1 = "新增plan计划";
  let plan_data = JSON.stringify(plan_data1);
  increase_order_no(cache).then((no: number) => {
    let strno: string = formatNum(String(no), 7);
    let date = new Date();
    let year = date.getFullYear();
    let pids = [];
    for (let plan in plans) {
      pids.push(plan);
    }
    // log.info(pids);
    let sum = 0;
    for (let i of pids) {
      let str = i.substring(24);
      let num = parseInt(str);
      sum += num;
    }
    let sum2 = sum + "";
    let sum1 = formatNum(sum2, 3);
    let order_no = "1" + "110" + "001" + sum1 + year + no;

    const pbegin = new Promise<void>((resolve, reject) => {
      db.query("BEGIN", [], (err: Error) => {
        if (err) {
          reject(err);
        } else {
          resolve();
        }
      });
    });

    const porder = new Promise<void>((resolve, reject) => {
      db.query("INSERT INTO orders(id, vid, type, state_code, state, summary, payment, no) VALUES($1,$2,$3,$4,$5,$6,$7,$8)", [order_id, vid, type, state_code, state, summary, payment, order_no], (err: Error) => {
        if (err) {
          reject(err);
        } else {
          resolve();
        }
      });
    });

    const pcommit = new Promise<void>((resolve, reject) => {
      db.query("COMMIT", [], (err: Error) => {
        if (err) {
          reject(err);
        } else {
          resolve();
        }
      });
    });

    let ps = [pbegin, porder]

    for (const pid of Object.keys(plans)) {
      const porderex = new Promise<void>((resolve, reject) => {
        db.query("INSERT INTO plan_order_ext(oid, pmid, promotion, pid, qid, service_ratio, expect_at) VALUES($1,$2,$3,$4,$5,$6,$7)", [order_id, pmid, promotion, pid, qid, service_ratio, expect_at], (err: Error, result: ResultSet) => {
          if (err) {
            reject(err);
          } else {
            resolve();
          }
        });
      });
      ps.push(porderex);
      for (let piid in plans[pid]) {
        const item_id = uuid.v1();
        const p = new Promise<void>((resolve, reject) => {
          db.query("INSERT INTO order_items(id, pid, piid, price) VALUES($1,$2,$3,$4)", [item_id, pid, piid, plans[pid][piid]], (err: Error) => {
            if (err) {
              reject(err);
            } else {
              resolve();
            }
          });
        });
        ps.push(p);
      }
    }

    ps.push(pcommit);

    async_serial<void>(ps, [], () => {
      cache.setex(cbflag, 30, JSON.stringify({
        code: 200,
        data: { id: order_id, no: order_no }
      }));
      sync_plan_orders(db, cache, domain, uid, order_id).then(() => {
        log.info("Place an plan order %s", order_id);
        done();
      });
    }, (e: Error) => {
      db.query("ROLLBACK", [], (err: Error) => {
        cache.setex(cbflag, 30, JSON.stringify({
          code: 500,
          msg: e.message
        }));
        done();
      });
    });
  }).catch((e: Error) => {
    cache.setex(cbflag, 30, JSON.stringify({
      code: 500,
      msg: e.message
    }));
    done();
  });
});


processor.call("updateOrderNo", (db: PGClient, cache: RedisClient, done: DoneFunction, order_no: string, new_order_no: string, cbflag) => {
  log.info("updateOrderNo" + order_no);
  cache.hget("orderNo-id", order_no, function (err, result) {
    if (err) {
      log.info(err + "get order_id err or order_id not found");
      cache.setex(cbflag, 30, JSON.stringify({
        code: 500,
        msg: err.message
      }));
      done();
    } else if (result === null) {
      cache.setex(cbflag, 30, JSON.stringify({
        code: 404,
        msg: "Order id not found"
      }));
      done();
    } else {
      let order_id = result;
      cache.hget("order-entities", order_id, function (err1, result1) {
        if (err1) {
          log.info(err1 + "get order_entities err or order_entities not found");
          cache.setex(cbflag, 30, JSON.stringify({
            code: 500,
            msg: err1.message
          }));
          done();
        } else if (result1 == null) {
          cache.setex(cbflag, 30, JSON.stringify({
            code: 404,
            msg: "Order not found"
          }));
          done();
        } else {
          let order_entities = JSON.parse(result1);
          order_entities["id"] = new_order_no;
          let multi = cache.multi();
          multi.hdel("orderNo-id", order_no);
          multi.hset("orderNo-id", new_order_no, order_id);
          multi.hset("order-entities", order_id, JSON.stringify(order_entities));
          multi.exec((err2, result2) => {
            if (err2) {
              log.error(err2, "query redis error");
              cache.setex(cbflag, 30, JSON.stringify({
                code: 500,
                msg: err2.message
              }));
            } else {
              log.info("updateOrderNo done");
              cache.setex(cbflag, 30, JSON.stringify({
                code: 200,
                data: new_order_no
              }));
            }
            done();
          });
        }
      });
    }
  });
});
// let args = [domain, uid, vid, dids, summary, payment];
processor.call("placeAnDriverOrder", (db: PGClient, cache: RedisClient, done: DoneFunction, domain: any, uid: string, vid: string, dids: any, summary: number, payment: number, cbflag: string) => {
  log.info("placeAnOrder");
  let order_id = uuid.v1();
  let item_id = uuid.v1();
  let event_id = uuid.v1();
  let state_code = 1;
  let state = "已支付";
  let type = 1;
  let driver_id = uuid.v1();
  let driver_data1 = "添加驾驶人";
  let driver_data = JSON.stringify(driver_data1);
  let created_at = new Date().getTime();
  let created_at1 = getLocalTime(created_at / 1000);
  db.query("BEGIN", (err: Error) => {
    if (err) {
      log.error(err, "query error");
      cache.setex(cbflag, 30, JSON.stringify({
        code: 500,
        msg: err.message
      }));
      done();
    } else {
      // vid, dids, summary, payment
      db.query("INSERT INTO orders(id, vid, type, state_code, state, summary, payment) VALUES($1,$2,$3,$4,$5,$6,$7)", [order_id, vid, type, state_code, state, summary, payment], (err: Error, result: ResultSet) => {
        if (err) {
          log.error(err, " insert orders  error in placeAnDriverOrder");
          cache.setex(cbflag, 30, JSON.stringify({
            code: 500,
            msg: err.message
          }));
          done();
          return;
        } else {
          db.query("INSERT INTO order_events(id, oid, uid, data) VALUES($1,$2,$3,$4)", [event_id, order_id, uid, driver_data], (err: Error, result: ResultSet) => {
            if (err) {
              log.error(err, "insert info order_events error in placeAnDriverOrder");
              cache.setex(cbflag, 30, JSON.stringify({
                code: 500,
                msg: err.message
              }));
              done();
              return;
            } else {
              let args2 = {
                domain: domain,
                uid: uid,
                vid: vid,
                dids: dids,
                summary: summary,
                payment: payment
              };
              insert_driver_order_recursive(db, done, order_id, args2, dids.map(did => did), {}, () => {
                let p = rpc(domain, servermap["vehicle"], null, "getVehicle", vid);
                let driver_promises = [];
                for (let did of dids) {
                  let p1 = rpc(domain, servermap["vehicle"], null, "getDrivers", vid, did);
                  driver_promises.push(p1);
                }
                p.then((v) => {
                  if (err) {
                    log.info("call vehicle error");
                    cache.setex(cbflag, 30, JSON.stringify({
                      code: 500,
                      msg: err.message
                    }));
                    done();
                  } else {
                    let vehicle = v["data"];
                    async_serial_driver(driver_promises, [], drivers => {
                      log.info("=============================555" + drivers);
                      let vid_doid = [];
                      cache.hget("vid-doid", vid, function (err, result1) {
                        if (err) {
                          log.info("get vid-doid error");
                        } else if (result1) {
                          vid_doid = JSON.parse(result1);
                        }
                        vid_doid.push(order_id);
                        let order = { summary: summary, state: state, payment: payment, drivers: drivers, created_at: created_at1, state_code: state_code, order_id: order_id, type: type, vehicle: vehicle };
                        let order_drivers = { drivers: drivers, vehicle: vehicle };
                        let multi = cache.multi();
                        multi.zadd("driver_orders", created_at, order_id);
                        multi.zadd("orders", created_at, order_id);
                        multi.zadd("orders-" + uid, created_at, order_id);
                        multi.zadd("newOrders", created_at, order_id);
                        multi.hset("vid-doid", vid, vid_doid);
                        multi.hset("driver-entities-", vid, JSON.stringify(order_drivers));
                        multi.hset("order-entities", order_id, JSON.stringify(order));
                        multi.exec((err3, replies) => {
                          if (err3) {
                            log.error(err3, "query redis error");
                            cache.setex(cbflag, 30, JSON.stringify({
                              code: 500,
                              msg: err3.message
                            }));
                            done();
                          } else {
                            log.info("placeAnDriverOrder: done");
                            cache.setex(cbflag, 30, JSON.stringify({
                              code: 200,
                              data: order_id
                            }));
                            done(); // close db and cache connection
                          }
                        });
                      });
                    });
                  }
                });
              });
            }
          });
        }
      });
    }
  });
});

function createGroup(domain: string, order: Object, uid: string) {
  return new Promise<any>((resolve, reject) => {
    let vehicle = order["vehicle"];
    let name: string = vehicle["owner"].name;
    let identity_no: string = vehicle["owner"].identity_no;
    let g_name: string;
    let apportion: number = 0.20;
    if (parseInt(identity_no.substr(16, 1)) % 2 === 1) {
      g_name = name + "先生";
    } else {
      g_name = name + "女士";
    }
    let p = rpc(domain, servermap["group"], null, "createGroup", g_name, vehicle["id"], apportion, uid);
    p.then(r => {
      resolve(r);
    }).catch((e: Error) => {
      reject(e);
    });
  });
}

function createAccount(domain: string, order: Object, uid: string) {
  return new Promise<any>((resolve, reject) => {
    const vid = order["vehicle"]["id"];
    const balance = order["summary"];
    const balance0 = balance * 0.2;
    const balance1 = balance * 0.8;
    let p = rpc(domain, servermap["wallet"], null, "createAccount", uid, 1, vid, balance0, balance1);
    p.then(r => {
      resolve(r);
    }).catch((e: Error) => {
      reject(e);
    });
  });
}

processor.call("updateOrderState", (db: PGClient, cache: RedisClient, done: DoneFunction, domain: string, uid: string, vid: string, order_id: string, state_code: number, state: string, cbflag: string) => {
  log.info("updateOrderState");
  let code = state_code;
  let type1 = 1;
  let balance: number = null;
  let start_at = null;
  db.query("UPDATE orders SET state_code = $1, state = $2 WHERE id = $3", [state_code, state, order_id], (err: Error, result: ResultSet) => {
    if (err) {
      log.info(err);
      cache.setex(cbflag, 30, JSON.stringify({
        code: 500,
        msg: err.message
      }));
      done();
    } else {
      cache.hget("order-entities", order_id, function (err, reply) {
        if (err) {
          log.info("err,get redis error");
          cache.setex(cbflag, 30, JSON.stringify({
            code: 500,
            msg: err.message
          }));
          done();
        } else if (!reply) {
          cache.setex(cbflag, 30, JSON.stringify({
            code: 404,
            msg: "Order not found!"
          }));
          done();
        } else {
          let order = JSON.parse(reply);
          let ps = [];
          if (state_code === 3) {
            ps.push(createGroup(domain, order, uid));
          } else if (state_code === 2) {
            ps.push(createAccount(domain, order, uid));
          }

          async_serial_ignore(ps, [], (_) => {
            order["state_code"] = state_code;
            order["state"] = state;
            let updated_at = new Date().getTime();
            let multi = cache.multi();
            multi.hset("order-entities", order_id, JSON.stringify(order));
            multi.zrem("newOrders", order_id);
            multi.zadd("newPays", updated_at, order_id);
            multi.setex(cbflag, 30, JSON.stringify({
              code: 200,
              data: "Success"
            }));
            multi.exec((err, result1) => {
              if (err) {
                log.info("err:hset order_entities error");
                cache.setex(cbflag, 30, JSON.stringify({
                  code: 500,
                  msg: err.message
                }));
                done();
              } else {
                log.info("db end in updateOrderState");
                done();
              }
            });
          });
        }
      });
    }
  });
});

// let args = {uid,vid,items,summary,payment};{piid: price}
processor.call("placeAnSaleOrder", (db: PGClient, cache: RedisClient, done: DoneFunction, uid: string, domain: any, order_id: string, vid: string, pid: string, qid: string, items: any, summary: string, payment: string, cbflag: string) => {
  log.info("placeAnOrder");
  let item_id = uuid.v1();
  let event_id = uuid.v1();
  let state_code = 1;
  let state = "已创建订单";
  let type = 2;
  let sale_id = uuid.v1();
  let sale_data = "新增第三方代售订单";
  let piids = [];
  for (let item in items) {
    piids.push(item);
  }
  db.query("BEGIN", (err: Error) => {
    if (err) {
      log.error(err, "query error");
      cache.setex(cbflag, 30, JSON.stringify({
        code: 500,
        msg: err.message
      }));
      done();
    } else {
      db.query("INSERT INTO orders(id, vid, type, state_code, state, summary, payment) VALUES($1, $2, $3, $4, $5, $6, $7)", [order_id, vid, type, state_code, state, summary, payment], (err: Error) => {
        if (err) {
          db.query("ROLLBACK", [], (err1: Error) => {
            log.error(err, "insert into orders error in placeAnSaleOrder");
            cache.setex(cbflag, 30, JSON.stringify({
              code: 500,
              msg: err.message
            }));
            done();
          });
        } else {
          db.query("INSERT INTO order_events(id, oid, uid, data) VALUES($1, $2, $3, $4)", [event_id, order_id, uid, sale_data], (err: Error, result: ResultSet) => {
            if (err) {
              db.query("ROLLBACK", [], (err1: Error) => {
                log.error(err, "insert into order_events error");
                cache.setex(cbflag, 30, JSON.stringify({
                  code: 500,
                  msg: err.message
                }));
                done();
              });
            } else {
              db.query("INSERT INTO sale_order_ext(id, oid,pid,qid) VALUES($1, $2,$3,$4)", [event_id, order_id, pid, qid], (err: Error, result: ResultSet) => {
                if (err) {
                  db.query("ROLLBACK", [], (err1: Error) => {
                    log.error(err, "insert into order_events error");
                    cache.setex(cbflag, 30, JSON.stringify({
                      code: 500,
                      msg: err.message
                    }));
                    done();
                  });
                } else {
                  insert_sale_order_items_recursive(db, done, order_id, pid, items, piids.map(piid => piid), {}, () => {
                    let p = rpc(domain, servermap["vehicle"], null, "getVehicle", vid);
                    let p1 = rpc(domain, servermap["plan"], null, "getPlan", pid);
                    async_serial<Object>([p, p1], [], (obj: Object[]) => {
                      if (obj[0]["code"] !== 200 || obj[1]["code"] !== 200) {
                        cache.setex(cbflag, 30, JSON.stringify({
                          code: 404,
                          msg: "Vehicle or plan not found"
                        }));
                        done();
                      } else {
                        let vehicle = obj[0]["data"];
                        let plan = obj[1]["data"];
                        let plan_items = plan["items"];
                        let piids = [];
                        for (let item of plan_items) {
                          piids.push(item.id);
                        }
                        let prices = [];
                        for (let item in items) {
                          prices.push(items[item]);
                        }
                        let items1 = [];
                        log.info("piids=============" + piids + "prices=========" + prices + "plan_items========================" + plan_items);
                        let len = Math.min(piids.length, prices.length, plan_items.length);
                        log.info("=====length" + len);
                        for (let i = 0; i < len; i++) {
                          let item_id = piids.shift();
                          let price = prices.shift();
                          let plan_item = plan_items.shift();
                          items.push({ item_id, price, plan_item });
                        }
                        let created_at = new Date().getTime();
                        let created_at1 = getLocalTime(created_at / 1000);
                        let start_at = null;
                        let stop_at = null;
                        let order = {
                          summary: summary, state: state, payment: payment, plan: plan, items: items1, state_code: state_code, id: order_id, type: type, vehicle: vehicle, created_at: created_at1, start_at: start_at, stop_at: stop_at
                        };
                        let multi = cache.multi();
                        multi.zadd("sale-orders", created_at, order_id);
                        multi.zadd("orders", created_at, order_id);
                        multi.zadd("orders-" + uid, created_at, order_id);
                        multi.hset("vehicle-order", vid, order_id);
                        multi.hset("orderid-vid", order_id, vid);
                        multi.hset("order-entities", order_id, JSON.stringify(order));
                        multi.exec((err3, replies) => {
                          if (err3) {
                            log.error(err3, "query error");
                            cache.setex(cbflag, 30, JSON.stringify({
                              code: 500,
                              msg: err3.message
                            }));
                          } else {
                            cache.setex(cbflag, 30, JSON.stringify({
                              code: 200,
                              data: order_id
                            }));
                          }
                          done(); // close db and cache connection
                        });
                      }
                    }, (e: Error) => {
                      cache.setex(cbflag, 30, JSON.stringify({
                        code: 500,
                        msg: e.message
                      }));
                      done();
                    });
                  });
                }
              });
            }
          });
        }
      });
    }
  });
});

processor.call("updateSaleOrder", (db: PGClient, cache: RedisClient, done: DoneFunction, domain: any, order_id: string, items: any, summary: number, payment: number, cbflag: string) => {
  log.info("UpdateSaleOrder");
  // orderNo-id
  let update_at = new Date;
  let piids: Object[] = [];
  let prices: Object[] = [];
  for (let item in items) {
    piids.push(item);
    prices.push(items[item]);
  }
  db.query("BEGIN", (err: Error) => {
    if (err) {
      log.error(err, "query error");
      cache.setex(cbflag, 30, JSON.stringify({
        code: 500,
        msg: err.message
      }));
      done();
    } else {
      db.query("UPDATE orders SET summary = $1,payment = $2,updated_at= $3 WHERE id = $4", [summary, payment, update_at, order_id], (err: Error, result: ResultSet) => {
        if (err) {
          log.info(err);
          log.info("err,updateOrderState error");
          cache.setex(cbflag, 30, JSON.stringify({
            code: 500,
            msg: err.message
          }));
          done();
        }
        else {
          update_sale_order_items_recursive(db, done, prices.map(price => price), piids.map(piid => piid), {}, () => {
            cache.hget("order-entities", order_id, function (err, result) {
              if (err) {
                log.info("err,get redis error");
                cache.setex(cbflag, 30, JSON.stringify({
                  code: 500,
                  msg: err.message
                }));
                done();
              } else {
                log.info("================" + result);
                let order_entities = JSON.parse(result);
                order_entities["payment"] = payment;
                order_entities["summary"] = summary;
                for (let piid in items) {
                  if (order_entities["items"]["item_id"] === piid) {
                    order_entities["items"]["price"] = items["piid"];
                  }
                }
                let multi = cache.multi();
                multi.hset("order-entities", order_id, JSON.stringify(order_entities));
                multi.exec((err, result1) => {
                  if (err) {
                    log.info("err:hset order_entities error");
                    cache.setex(cbflag, 30, JSON.stringify({
                      code: 500,
                      msg: err.message
                    }));
                    done();
                  } else {
                    log.info("db end in updateOrderState");
                    cache.setex(cbflag, 30, JSON.stringify({
                      code: 200,
                      data: order_id
                    }));
                    done();
                  }
                });
              }
            });
          });
        }
      });
    }
  });
});
// 生成核保
processor.call("createUnderwrite", (db: PGClient, cache: RedisClient, done: DoneFunction, uwid: string, oid: string, plan_time: any, validate_place: string, validate_update_time: any, callback: string, domain: any) => {
  log.info("createUnderwrite ");
  let pcreate = new Promise<void>((resolve, reject) => {
    db.query("INSERT INTO underwrites (id, oid, plan_time, validate_place, validate_update_time) VALUES ($1, $2, $3, $4, $5)", [uwid, oid, plan_time, validate_place, validate_update_time], (err: Error) => {
      if (err) {
        log.info("err" + err);
        reject(err);
      } else {
        resolve();
      }
    });
  });
  pcreate.then(() => {
    let now = new Date();
    let underwrite = {
      id: uwid,
      order_id: oid,
      plan_time: plan_time,
      validate_place: validate_place,
      validate_update_time: validate_update_time,
      created_at: now,
      updated_at: now
    };
    let multi = cache.multi();
    multi.hset("underwrite-entities", uwid, JSON.stringify(underwrite));
    multi.zadd("underwrite", now.getTime(), uwid);
    multi.setex(callback, 30, JSON.stringify({
      code: 200,
      uwid: uwid
    }));
    multi.exec((err: Error, _) => {
      if (err) {
        log.error(err, "update underwrite cache error");
      }
      done();
    });
    underwrite_trigger.send(msgpack.encode({ uwid, underwrite }));
  }).catch(error => {
    cache.setex(callback, 30, JSON.stringify({
      code: 500,
      msg: error.message
    }));
    log.info("err" + error);
    done();
  });
});

// 修改预约验车地点
processor.call("alterValidatePlace", (db: PGClient, cache: RedisClient, done: DoneFunction, uwid: string, plan_time: any, validate_place: string, validate_update_time: any, callback: string) => {
  log.info("alterValidatePlace ");
  modifyUnderwrite(db, cache, done, uwid, callback, "UPDATE underwrites SET plan_time = $1, validate_place = $2, validate_update_time = $3, updated_at = $4 WHERE id = $5", [plan_time, validate_place, validate_update_time, validate_update_time, uwid], (underwrite) => {
    underwrite["plan_time"] = plan_time;
    underwrite["validate_place"] = validate_place;
    underwrite["validate_update_time"] = validate_update_time;
    underwrite["updated_at"] = validate_update_time;
    return underwrite;
  });
});

processor.call("fillUnderwrite", (db: PGClient, cache: RedisClient, done: DoneFunction, domain: any, uwid: string, real_place: string, update_time: any, opid: string, certificate_state: number, problem_type: any, problem_description: string, note: string, photos: any, callback: string) => {
  log.info("fillUnderwrite");
  let pbegin = new Promise<void>((resolve, reject) => {
    db.query("BEGIN", [], (err: Error) => {
      if (err) {
        log.info(err);
        reject(err);
      } else {
        resolve();
      }
    });
  });
  let pfill = new Promise<void>((resolve, reject) => {
    db.query("UPDATE underwrites SET real_place = $1, real_update_time = $2, opid = $3, certificate_state = $4, problem_type = $5, problem_description = $6, note = $7, note_update_time = $8, updated_at = $9 WHERE id = $10", [real_place, update_time, opid, certificate_state, problem_type, problem_description, note, update_time, update_time, uwid], (err: Error) => {
      if (err) {
        log.info(err);
        reject(err);
      } else {
        resolve();
      }
    });
  });
  let promises = [pbegin, pfill];
  let photo_entities = [];
  for (let photo of photos) {
    let pphoto = new Promise<void>((resolve, reject) => {
      let upid = uuid.v1();
      db.query("INSERT INTO underwrite_photos (id, uwid, photo) VALUES ($1, $2, $3)", [upid, uwid, photo], (err: Error) => {
        if (err) {
          log.info("query error" + err);
          reject(err);
        } else {
          let photo_entry = {
            id: upid,
            photo: photo,
            created_at: update_time,
            updated_at: update_time
          };
          photo_entities.push(photo_entry);
          resolve();
        }
      });
    });
    promises.push(pphoto);
  }

  let pcommit = new Promise<void>((resolve, reject) => {
    db.query("COMMIT", [], (err: Error) => {
      if (err) {
        log.info(err);
        reject(err);
      } else {
        resolve();
      }
    });
  });
  promises.push(pcommit);
  let predis = new Promise<Object>((resolve, reject) => {
    cache.hget("underwrite-entities", uwid, function (err, result) {
      if (result) {
        let underwrite = JSON.parse(result);
        resolve(underwrite);
      } else if (err) {
        log.info(err);
        reject(err);
      } else {
        resolve(null);
      }
    });
  });
  async_serial<void>(promises, [], () => {
    cache.hget("underwrite-entities", uwid, function (err, result) {
      if (result) {
        let op = rpc<Object>(domain, servermap["operator"], null, "getOperator", opid);
        op.then(o => {
          let operator = o["data"];
          let underwrite = JSON.parse(result);
          underwrite.real_place = real_place;
          underwrite.real_update_time = update_time;
          underwrite.operator = operator;
          underwrite.certificate_state = certificate_state;
          underwrite.problem_type = problem_type;
          underwrite.problem_description = problem_description;
          underwrite.note = note;
          underwrite.note_updat_time = update_time;
          underwrite.update_time = update_time;
          underwrite.photos = photo_entities;
          log.info("underwrite" + underwrite);
          let multi = cache.multi();
          multi.hset("underwrite-entities", uwid, JSON.stringify(underwrite));
          multi.setex(callback, 30, JSON.stringify({
            code: 200,
            uwid: uwid
          }));
          multi.exec((err: Error, _) => {
            log.info("args.callback" + callback);
            if (err) {
              log.info(err);
            }
            underwrite_trigger.send(msgpack.encode({ uwid, underwrite }));
            done();
          });
        });
      } else if (err) {
        log.error(err);
      }
    });
  }, (e: Error) => {
    db.query("ROLLBACK", [], (err: Error) => {
      if (err) {
        log.error(err);
      }
      cache.setex(callback, 30, JSON.stringify({
        code: 500,
        msg: err.message
      }));
      done();
    });
    log.info("err" + e);
  });
});

// 提交审核结果
processor.call("submitUnderwriteResult", (db: PGClient, cache: RedisClient, done: DoneFunction, domain: any, uwid: string, underwrite_result: string, update_time: any, callback: string) => {
  log.info("submitUnderwriteResult ");
  new Promise<void>((resolve, reject) => {
    db.query("UPDATE underwrites SET underwrite_result = $1, result_update_time = $2, updated_at = $3 WHERE id = $4", [underwrite_result, update_time, update_time, uwid], (err: Error) => {
      if (err) {
        log.info(err);
        reject(err);
      } else {
        resolve(null);
      }
    });
  })
    .then(() => {
      return new Promise<Object>((resolve, reject) => {
        log.info("redis " + uwid);
        cache.hget("underwrite-entities", uwid, function (err, result) {
          if (result) {
            resolve(JSON.parse(result));
          } else if (err) {
            log.info(err);
            reject(err);
          } else {
            resolve(null);
          }
        });
      });
    })
    .then((underwrite: Object) => {
      return new Promise<Object>((resolve, reject) => {
        if (underwrite != null) {
          underwrite["underwrite_result"] = underwrite_result;
          underwrite["result_update_time"] = update_time;
          underwrite["updated_at"] = update_time;
          let multi = cache.multi();
          multi.hset("underwrite-entities", uwid, JSON.stringify(underwrite));
          multi.exec((err: Error, _) => {
            if (err) {
              log.info(err);
              reject(err);
            } else {
              resolve(underwrite);
            }
          });
        } else {
          reject("underwrite is null");
        }
      });
    })
    .then((underwrite: Object) => {
      return new Promise<Object>((resolve, reject) => {
        let orderid = underwrite["order_id"];
        console.log("orderid------------" + orderid);
        cache.hget("order-entities", orderid, function (err, result) {
          if (result) {
            let order = JSON.parse(result);
            let expect_at = new Date(order["expect_at"]);
            log.info(expect_at + "expect_at" + order["expect_at"]);
            let start_at = expect_at;
            if (expect_at.getTime() <= update_time.getTime()) {
              let date = expect_at.getFullYear() + "-" + (expect_at.getMonth() + 1) + "-" + (expect_at.getDate() + 1);
              start_at = new Date(date);
            }
            let stop_at = new Date(start_at.getTime() + 31536000000);
            order["start_at"] = start_at;
            order["stop_at"] = stop_at;
            order["state_code"] = 3;
            order["state"] = "已核保";
            db.query("UPDATE orders SET start_at = $1, stop_at = $2, state_code = 3, state = '已核保' WHERE id = $5", [start_at, stop_at, orderid], (err: Error) => {
              if (err) {
                log.info(err);
                reject(err);
              } else {
                resolve(order);
              }
            });
          } else {
            reject(err);
          }
        });
      });
    })
    .then((order: Object) => {
      let orderid = order["order_id"];
      let multi = cache.multi();
      multi.hset("order-entities", orderid, JSON.stringify(order));
      multi.setex(callback, 30, JSON.stringify({
        code: 200,
        uwid: uwid
      }));
      multi.exec((err2, result2) => {
        if (result2) {
          if (underwrite_result.trim() === "通过") {
            log.info("userid------------" + order["vehicle"]["vehicle"]["user_id"]);
            cache.hget("wxuser", order["vehicle"]["vehicle"]["user_id"], function (err, result3) {
              if (err) {
                log.info("get wxuser err");
              } else {
                let openid = result3;
                log.info("openid------------" + openid);
                let No = order["vehicle"]["vehicle"]["license_no"];
                let CarNo = order["vehicle"]["vehicle_model"]["familyName"];
                let name = order["vehicle"]["vehicle"]["owner"]["name"];
                let No1 = String(No);
                let CarNo1 = String(CarNo);
                let Name = String(name);
                var postData = queryString.stringify({
                  "user": openid,
                  "No": No1,
                  "CarNo": CarNo1,
                  "Name": Name,
                  "orderId": orderid
                });

                var options = {
                  hostname: wxhost,
                  port: 80,
                  path: "/wx/wxpay/tmsgUnderwriting",
                  method: "GET",
                  headers: {
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Content-Length": Buffer.byteLength(postData)
                  }
                };

                var req = http.request(options, (res) => {
                  log.info(`STATUS: ${res.statusCode}`);
                  log.info(`HEADERS: ${JSON.stringify(res.headers)}`);
                  res.setEncoding("utf8");
                  res.on("data", (chunk) => {
                    log.info(`BODY: ${chunk}`);
                  });
                  res.on("end", () => {
                    log.info("No more data in response.");
                  });
                });
                req.on("error", (e) => {
                  log.info(`problem with request: ${e.message}`);
                });

                // write data to request body
                req.write(postData);
                req.end();
              }
            });
          }
          underwrite_trigger.send(msgpack.encode({ orderid, order }));
          done();
        } else {
          cache.setex(callback, 30, JSON.stringify({
            code: 500,
            msg: "update order cache error"
          }));
          done();
        }
      });
    })
    .catch(error => {
      cache.setex(callback, 30, JSON.stringify({
        code: 500,
        msg: error.message
      }));
      log.info("err" + error);
      done();
    });
});

// 修改实际验车地点
processor.call("alterRealPlace", (db: PGClient, cache: RedisClient, done: DoneFunction, uwid: string, real_place: string, update_time: any, callback: string) => {
  log.info("alterRealPlace ");
  modifyUnderwrite(db, cache, done, uwid, callback, "UPDATE underwrites SET real_place = $1, real_update_time = $2, updated_at = $3 WHERE id = $4", [real_place, update_time, update_time, uwid], (underwrite) => {
    underwrite["real_place"] = real_place;
    underwrite["real_update_time"] = update_time;
    underwrite["updated_at"] = update_time;
    return underwrite;
  });
});

// 修改备注
processor.call("alterNote", (db: PGClient, cache: RedisClient, done: DoneFunction, uwid: string, note: string, update_time: any, callback: string) => {
  log.info("alterNote ");
  modifyUnderwrite(db, cache, done, uwid, callback, "UPDATE underwrites SET note = $1, note_update_time = $2, updated_at = $3 WHERE id = $4", [note, update_time, update_time, uwid], (underwrite) => {
    underwrite["note"] = note;
    underwrite["note_update_time"] = update_time;
    underwrite["updated_at"] = update_time;
    return underwrite;
  });
});

// 上传现场图片
processor.call("uploadPhotos", (db: PGClient, cache: RedisClient, done: DoneFunction, uwid: string, photo: string, update_time: any, callback: string) => {
  log.info("uploadPhotos ");
  let upid = uuid.v1();
  modifyUnderwrite(db, cache, done, uwid, callback, "INSERT INTO underwrite_photos (id, uwid, photo) VALUES ($1, $2, $3)", [upid, uwid, photo], (underwrite) => {
    let photo_entry = {
      id: upid,
      photo: photo,
      created_at: update_time,
      updated_at: update_time
    };
    underwrite["photos"].push(photo_entry);
    return underwrite;
  });
});

function modifyUnderwrite(db: PGClient, cache: RedisClient, done: DoneFunction, uwid: string, cbflag: string, sql: string, args: any[], cb: ((underwrite: Object) => Object)): void {
  new Promise<void>((resolve, reject) => {
    db.query(sql, args, (err: Error) => {
      if (err) {
        log.info(err);
        reject(err);
      } else {
        resolve(null);
      }
    });
  })
    .then(() => {
      return new Promise<Object>((resolve, reject) => {
        log.info("redis " + uwid);
        cache.hget("underwrite-entities", uwid, function (err, result) {
          if (result) {
            resolve(JSON.parse(result));
          } else if (err) {
            log.info(err);
            reject(err);
          } else {
            resolve(null);
          }
        });
      });
    })
    .then((underwrite: Object) => {
      if (underwrite != null) {
        let uw = cb(underwrite);
        let multi = cache.multi();
        multi.hset("underwrite-entities", uwid, JSON.stringify(uw));
        multi.setex(cbflag, 30, JSON.stringify({
          code: 200,
          uwid: uwid
        }));
        multi.exec((err: Error, _) => {
          if (err) {
            log.error(err, "update underwrite cache error");
            cache.setex(cbflag, 30, JSON.stringify({
              code: 500,
              msg: "update underwrite cache error"
            }));
          }
          underwrite_trigger.send(msgpack.encode({ uwid, underwrite }));
          done();
        });
      } else {
        cache.setex(cbflag, 30, JSON.stringify({
          code: 404,
          msg: "Not found underwrite"
        }));
        log.info("Not found underwrite");
        done();
      }
    })
    .catch(error => {
      cache.setex(cbflag, 30, JSON.stringify({
        code: 500,
        msg: error.message
      }));
      log.info("err" + error);
      done();
    });
}

function select_order_item_recursive(db, done, piids, acc, cb) {
  if (piids.length === 0) {
    cb(acc);
  } else {
    let piid = piids.shift();
    db.query("SELECT id,price,pid FROM order_items WHERE id = $1 AND deleted = FALSE", [piid], (err: Error, result: ResultSet) => {
      if (err) {
        log.info(err);
        db.query("ROLLBACK", [], (err: Error) => {
          log.error(err, "insert into order_items error");
          done();
        });
      } else {
        let item = { id: piid, price: result["price"], pid: result["pid"], plan_item: null };
        acc.push(item);
        select_order_item_recursive(db, done, piids, acc, cb);
      }
    });
  }
}

function refresh_driver_orders(db: PGClient, cache: RedisClient, domain: string): Promise<void> {
  return sync_driver_orders(db, cache, domain, null);
}

function sync_driver_orders(db: PGClient, cache: RedisClient, domain: string, uid: string, oid?: string): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    db.query("SELECT o.id AS o_id, o.no AS o_no, o.vid AS o_vid, o.type AS o_type, o.state_code AS o_state_code, o.state AS o_state, o.summary AS o_summary, o.payment AS o_payment, o.start_at AS o_start_at, o.stop_at AS o_stop_at, o.created_at AS o_created_at, o.updated_at AS o_updated_at, e.pid AS e_pid FROM driver_order_ext AS e LEFT JOIN orders AS o ON o.id = e.oid WHERE o.deleted = FALSE AND e.deleted = FALSE" + (oid ? " AND o.id = $1" : ""), oid ? [oid] : [], (e: Error, result: ResultSet) => {
      if (e) {
        reject(e);
      } else {
        const orders = {};
        for (const row of result.rows) {
          if (orders.hasOwnProperty(row.o_id)) {
            orders[row.o_id]["dids"].push(row.e_pid);
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
              drivers: [],
              dids: [row.e_pid],
              created_at: row.o_created_at,
              updated_at: row.o_created_at
            }
            orders[row.o_id] = order;
          }
        }
        const oids = Object.keys(orders);
        let pvs = oids.map(oid => {
          const order = orders[oid];
          let p = rpc<Object>(domain, servermap["vehicle"], null, "getVehicle", order.vid);
          return p;
        });
        async_serial_ignore<Object>(pvs, [], (vreps) => {
          const vehicles = vreps.filter(v => v["code"] === 200).map(v => v["data"]);
          for (const vehicle of vehicles) {
            for (const oid of oids) {
              const order = orders[oid];
              if (vehicle["id"] === order.vid) {
                order.vehicle = vehicle;
              }
            }
          }
          let pds = oids.reduce((acc, oid) => {
            const order = orders[oid];
            for (const did of order.dids) {
              let p = rpc<Object>(domain, servermap["vehicle"], null, "getDrivers", order.vid, did);
              acc.push(p);
            }
            return acc;
          }, []);
          async_serial_ignore<Object>(pds, [], (drvreps) => {
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
            multi.exec((err: Error, _: any[]) => {
              if (err) {
                reject(err);
              } else {
                resolve();
              }
            });
          });
        });
      }
    });
  });
}

function refresh_plan_orders(db: PGClient, cache: RedisClient, domain: string): Promise<void> {
  return sync_plan_orders(db, cache, domain, null);
}

function sync_plan_orders(db: PGClient, cache: RedisClient, domain: string, uid: string, oid?: string): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    db.query("SELECT o.id AS o_id, o.no AS o_no, o.vid AS o_vid, o.type AS o_type, o.state_code AS o_state_code, o.state AS o_state, o.summary AS o_summary, o.payment AS o_payment, o.start_at AS o_start_at, o.stop_at AS o_stop_at, o.created_at AS o_created_at, o.updated_at AS o_updated_at, e.qid AS e_qid, e.pid AS e_pid, e.service_ratio AS e_service_ratio, e.expect_at AS e_expect_at, oi.id AS oi_id, oi.pid AS oi_pid, oi.price AS oi_price, oi.piid AS oi_piid FROM plan_order_ext AS e INNER JOIN orders AS o ON o.id = e.oid INNER JOIN plans AS p ON e.pid = p.id INNER JOIN plan_items AS pi ON p.id = pi.pid INNER JOIN order_items AS oi ON oi.piid = pi.id AND oi.pid = p.id WHERE o.deleted = FALSE and e.deleted = FALSE AND oi.deleted = FALSE AND p.deleted = FALSE AND pi.deleted = FALSE" + (oid ? " AND o.id = $1" : ""), oid ? [oid] : [], (err: Error, result: ResultSet) => {
      if (err) {
        reject(err);
      } else {
        const orders = {};
        for (const row of result.rows) {
          if (orders.hasOwnProperty(row.o_id)) {
            orders[row.o_id]["items"].push({
              id: row.oi_id,
              pid: row.oi_pid,
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
              start_at: row.o_start_at,
              stop_at: row.o_stop_at,
              vid: row.o_vid,
              vehicle: null,
              pids: pids,
              plans: [],
              qid: row.e_qid,
              quotation: null,
              service_ratio: row.e_service_ratio,
              expact_at: row.e_expact_at,
              items: [{
                id: row.oi_id,
                pid: row.oi_pid,
                piid: row.oi_piid,
                plan_item: null,
                price: row.oi_price
              }],
              created_at: row.o_created_at,
              updated_at: row.o_updated_at
            }
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
          pidstmp.push(Object.keys(orders[oid]["pid"]));
          for (const item of orders[oid]["items"]) {
            piidstmp.push(item["plan_item"]);
          }
        }
        const vids = [... new Set(vidstmp)];
        const qids = [... new Set(qidstmp)];
        const pids = [... new Set(pidstmp)];

        let pvs = vids.map(vid => rpc<Object>(domain, servermap["vehicle"], uid, "getVehicle", vid));
        async_serial_ignore<Object>(pvs, [], (vreps) => {
          const vehicles = vreps.filter(v => v["code"] === 200).map(v => v["data"]);
          for (const vehicle of vehicles) {
            for (const oid of oids) {
              const order = orders[oid];
              if (vehicle["id"] === order["vid"]) {
                order["vehicle"] = vehicle; // a vehicle may belong to many orders
              }
            }
          }
          let pqs = qids.map(qid => rpc<Object>(domain, servermap["quotation"], uid, "getQuotation", qid));
          async_serial_ignore<Object>(pqs, [], (qreps) => {
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
            let pps = pids.map(pid => rpc<Object>(domain, servermap["plan"], uid, "getPlan", pid));
            async_serial_ignore<Object>(pps, [], (preps) => {
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
                multi.zadd("plan-orders", updated_at, oid);
                multi.zadd("orders", updated_at, oid);
                multi.zadd("newOrders", updated_at, oid);
                multi.hset("orderNo-id", order_no, oid);
                multi.hset("order-vid-" + vid, qid, oid);
                multi.hset("orderid-vid", oid, vid);
                multi.hset("order-entities", oid, JSON.stringify(order));
                multi.zadd("plan-orders", updated_at, oid);
              }
              multi.exec((err: Error, _: any[]) => {
                if (err) {
                  reject(err);
                } else {
                  resolve();
                }
              });
            });
          });
        });
      }
    });
  });
}

function refresh_sale_orders(db: PGClient, cache: RedisClient, domain: string): Promise<void> {
  return sync_sale_orders(db, cache, domain, null);
}

function sync_sale_orders(db: PGClient, cache: RedisClient, domain: string, uid: string, oid?: string): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    db.query("SELECT o.id AS o_id, o.no AS o_no, o.vid AS o_vid, o.type AS o_type, o.state_code AS o_state_code, o.state AS o_state, o.summary AS o_summary, o.payment AS o_payment, o.start_at AS o_start_at, o.stop_at AS o_stop_at, o.created_at AS o_created_at, o.updated_at AS o_updated_at, e.qid AS e_qid, oi.id AS oi_id, oi.pid AS oi_pid, oi.price AS oi_price, oi.piid AS oi_piid FROM sale_order_ext AS e INNER JOIN orders AS o ON o.id = e.oid INNER JOIN plans AS p ON e.pid = p.id INNER JOIN plan_items AS pi ON p.id = pi.pid INNER JOIN order_items AS oi ON oi.piid = pi.id AND oi.pid = o.id WHERE o.deleted = FALSE AND e.deleted = FALSE AND p.deleted = FALSE AND pi.deleted = FALSE AND oi.deleted = FALSE" + (oid ? " AND o.id = $1" : ""), oid ? [oid] : [], (err: Error, result: ResultSet) => {
      if (err) {
        reject(err);
      } else {
        const orders = {};
        for (const row of result.rows) {
          if (orders.hasOwnProperty(row.o_id)) {
            orders[row.o_id]["items"].push({
              id: row.oi_id,
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
              plan: null,
              qid: row.e_qid,
              quotation: null,
              items: [{
                id: row.oi_id,
                piid: row.oi_piid,
                plan_item: null,
                price: row.oi_price
              }],
              created_at: row.o_created_at,
              updated_at: row.o_updated_at
            }
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

        let pvs = vids.map(vid => rpc<Object>(domain, servermap["vehicle"], null, "getVehicle", vid));
        async_serial_ignore<Object>(pvs, [], (vreps) => {
          const vehicles = vreps.filter(v => v["code"] === 200).map(v => v["data"]);
          for (const vehicle of vehicles) {
            for (const oid of oids) {
              const order = orders[oid];
              if (vehicle["id"] === order["vid"]) {
                order["vehicle"] = vehicle; // a vehicle may belong to many orders
              }
            }
          }
          let pqs = qids.map(qid => rpc<Object>(domain, servermap["quotation"], null, "getQuotation", qid));
          async_serial_ignore<Object>(pqs, [], (qreps) => {
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
            let pps = pids.map(pid => rpc<Object>(domain, servermap["plan"], null, "getPlan", pid));
            async_serial_ignore<Object>(pps, [], (preps) => {
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
                multi.hset("vehicle-order", vid, oid);
                multi.hset("orderid-vid", oid, vid);
                multi.hset("order-entities", oid, JSON.stringify(order));
                multi.zadd("sale-orders", updated_at, oid);
              }
              multi.exec((err: Error, _: any[]) => {
                if (err) {
                  reject(err);
                } else {
                  resolve();
                }
              });
            });
          });
        });
      }
    });
  });
}

function refresh_underwrite(db: PGClient, cache: RedisClient, domain: string) {
  return new Promise<void>((resolve, reject) => {
    db.query("SELECT u.id AS u_id, u.oid AS u_oid, u.opid AS u_opid, u.plan_time AS u_plan_time, u.real_time AS u_real_time, u.validate_place AS u_validate_place, u.validate_update_time AS u_validate_update_time, u.real_place AS u_real_place, u.real_update_time AS u_real_update_time, u.certificate_state AS u_certificate_state, u.problem_type AS u_problem_type, u.problem_description AS u_problem_description, u.note AS u_note, u.note_update_time AS u_note_update_time, u.underwrite_result AS u_underwrite_result, u.result_update_time AS u_result_update_time, u.created_at AS u_created_at, u.updated_at AS u_updated_at, up.photo AS up_photo FROM underwrites AS u INNER JOIN underwrite_photos AS up ON u.id= up.uwid WHERE u.deleted= FALSE AND up.deleted= FALSE", [], (err: Error, result: ResultSet) => {
      if (err) {
        reject(err);
      } else {
        const underwrites = {};
        for (const row of result.rows) {
          if (underwrites.hasOwnProperty(row.u_id)) {
            underwrites[row.u_id]["photos"].push(trim(row.up_photo));
          } else {
            underwrites[row.u_id] = {
              id: row.u_id,
              oid: row.u_oid,
              order: null,
              opid: row.u_opid,
              operator: null,
              plan_time: row.u_plan_time,
              real_time: row.u_real_time,
              validate_place: trim(row.u_validate_place),
              validate_update_time: row.u_validate_update_time,
              real_place: trim(row.u_real_place),
              real_update_time: row.u_real_update_time,
              certificate_state: row.u_certificate_state,
              problem_type: row.u_problem_type,
              problem_description: trim(row.u_problem_description),
              note: trim(row.u_note),
              note_update_time: row.u_note_update_time,
              underwrite_result: trim(row.u_underwrite_result),
              result_update_time: row.u_result_update_time,
              created_at: row.u_created_at,
              updated_at: row.u_updated_at,
              photos: [trim(row.up_photo)]
            };
          }
        }
        const uwids = Object.keys(underwrites);

        const oridstmp = [];
        const opidstmp = [];

        for (const uwid of uwids) {
          const underwrite = underwrites[uwid];
          if (underwrite.opid) {
            opidstmp.push(underwrite.opid);
          }
          oridstmp.push(underwrite.oid);
        }

        const orids = [... new Set(oridstmp)];
        const opids = [... new Set(opidstmp)];

        const multi = cache.multi();
        for (const oid of orids) {
          multi.hget("order-entities", oid);
        }
        multi.exec((e: Error, results: string[]) => {
          if (e) {
            reject(e);
          } else {
            const orders = results.filter(r => r !== null).map(r => JSON.parse(r));
            for (const order of orders) {
              for (const uwid of uwids) {
                const underwrite = underwrites[uwid];
                if (underwrite.oid === order.id) {
                  underwrite["order"] = order;
                }
              }
            }
            let pops = opids.map(opid => rpc<Object>(domain, servermap["operator"], null, "getOperator", opid));
            async_serial_ignore<Object>(pops, [], (opreps) => {
              const operators = opreps.filter(o => o["code"] === 200).map(o => o["data"]);
              for (const operator of operators) {
                for (const uwid of uwids) {
                  const underwrite = underwrites[uwid];
                  if (underwrite.opid === operator.id) {
                    underwrite["operator"] = operator;
                  }
                }
              }
              const multi1 = cache.multi();
              for (const uwid of uwids) {
                const underwrite = underwrites[uwid];
                multi1.hset("underwrite-entities", uwid, JSON.stringify(underwrite));
              }
            });
          }
        });
      }
    });
  });
}

processor.call("refresh", (db: PGClient, cache: RedisClient, done: DoneFunction, domain: string) => {
  log.info("refresh");
  const pdo = refresh_driver_orders(db, cache, domain);
  const ppo = refresh_plan_orders(db, cache, domain);
  const pso = refresh_sale_orders(db, cache, domain);
  const puw = refresh_underwrite(db, cache, domain);
  let ps = [ppo, pdo, pso, puw];
  async_serial_ignore<void>(ps, [], () => {
    log.info("refresh done!");
    done();
  });
});

function trim(str: string) {
  if (str) {
    return str.trim();
  } else {
    return null;
  }
}

processor.run();

log.info("Start processor at " + config.addr);
