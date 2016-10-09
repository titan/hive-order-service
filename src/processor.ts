import { Processor, Config, ModuleFunction, DoneFunction, rpc, async_serial, async_serial_ignore } from "hive-processor";
import { Client as PGClient, ResultSet } from "pg";
import { createClient, RedisClient } from "redis";
import { servermap, triggermap } from "hive-hostmap";
import * as bunyan from "bunyan";
import * as uuid from "node-uuid";
import * as msgpack from "msgpack-lite";
import * as nanomsg from "nanomsg";
import * as http from "http";
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
  if (piids.length == 0) {
    cb(acc);
  } else {
    let item_id = uuid.v1();
    let piid = piids.shift();
    db.query('INSERT INTO order_items(id,piid,pid, price) VALUES($1,$2,$3,$4)', [item_id, piid, pid, items[piid]], (err: Error) => {
      if (err) {
        log.info(err);
        db.query('ROLLBACK', [], (err: Error) => {
          log.error(err, 'insert into order_items error');
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
  if (piids.length == 0) {
    cb(acc);
  } else {
    let piid = piids.shift();
    let price = prices.shift();
    db.query('UPDATE order_items SET price = $1 WHERE id = $2', [price, piid], (err: Error, result: ResultSet) => {
      if (err) {
        log.info(err);
        db.query('ROLLBACK', [], (err: Error) => {
          log.error(err, 'insert into order_items error');
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
    p.then(val => {
      acc.push(val);
      async_serial_driver(ps, acc, cb);
    })
      .catch(e => {
        async_serial_driver(ps, acc, cb);
      });
  }
}
processor.call("placeAnPlanOrder", (db: PGClient, cache: RedisClient, done: DoneFunction, domain: any, uid: string, order_id: string, vid: string, plans: any, qid: string, pmid: string, promotion: number, service_ratio: number, summary: number, payment: number, v_value: number, expect_at: any, callback: string) => {
  log.info("placeOrder");
  let event_id = uuid.v1();
  let state_code = 1;
  let state = "已创建订单";
  let type = 0;
  let plan_data1 = "新增plan计划";
  let plan_data = JSON.stringify(plan_data1);
  cache.incr("order-no", (err, strNo) => {
    if (err || !strNo) {
      log.info(err + "cache incr err");
    } else {
      let no: string = formatNum(<string>strNo, 7);
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
        let num = +str;
        sum += num;
      }
      let sum2 = sum + "";
      let sum1 = formatNum(sum2, 3);
      let order_no = "1" + "110" + "001" + sum1 + year + no;
      db.query("BEGIN", (err: Error) => {
        if (err) {
          log.error(err, "query error");
          done();
        } else {
          db.query("INSERT INTO orders(id, vid, type, state_code, state, summary, payment) VALUES($1,$2,$3,$4,$5,$6,$7)", [order_id, vid, type, state_code, state, summary, payment], (err: Error) => {
            if (err) {
              db.query("ROLLBACK", [], (err: Error) => {
                log.error(err, "insert into orders error");
                done();
              });
            } else {
              db.query("INSERT INTO order_events(id, oid, uid, data) VALUES($1, $2, $3, $4)", [event_id, order_id, uid, plan_data], (err: Error, result: ResultSet) => {
                if (err) {
                  db.query("ROLLBACK", [], (err: Error) => {
                    log.error(err, "insert into order_events error");
                    done();
                  });
                } else {
                  let args1 = {
                    domain: domain,
                    uid: uid,
                    order_id: order_id,
                    vid: vid,
                    plans: plans,
                    qid: qid,
                    pmid: pmid,
                    promotion: promotion,
                    service_ratio: service_ratio,
                    summary: summary,
                    payment: payment,
                    v_value: v_value,
                    expect_at: expect_at,
                    callback: callback
                  };
                  insert_plan_order_recursive(db, done, order_id, args1, pids.map(pid => pid), {}, (pid_oiids_obj) => {
                    let p = rpc<Object>(domain, servermap["vehicle"], null, "getModelAndVehicleInfo", vid);
                    let p2 = rpc<Object>(domain, servermap["quotation"], null, "getQuotation", qid);
                    let plan_promises: Promise<Object>[] = [];
                    for (let pid of pids) {
                      let p1 = rpc<Object>(domain, servermap["plan"], null, "getPlan", pid);
                      plan_promises.push(p1);
                    }
                    p.then((vehicle) => {
                      if (err) {
                        log.info("call vehicle error");
                      } else {
                        p2.then((quotation) => {
                          if (err) {
                            log.info("call quotation error");
                          } else {
                            async_serial<Object>(plan_promises, [], (plans2: Object[]) => {
                              let plan_items = [];
                              for (let plan1 of plans2) {
                                for (let piid in plan1["items"]) {
                                  plan_items.push(plan1["items"][piid]);
                                }
                              }
                              let piids = [];
                              let prices = [];
                              for (let pid in plans) {
                                for (let piid in plans[pid]) {
                                  piids.push(piid);
                                  prices.push(plans[pid][piid]);
                                }
                              }
                              let item_ids = [];
                              for (let pid in pid_oiids_obj) {
                                item_ids.push(pid_oiids_obj[pid]);
                              }
                              let items = [];
                              log.info("piids=============" + piids + "prices=========" + prices + "item_ids====================" + item_ids + "plan_items========================" + plan_items);
                              let len = Math.min(piids.length, prices.length, plan_items.length);
                              for (let i = 0; i < len; i++) {
                                let item_id = piids.shift();
                                let price = prices.shift();
                                // let pid = item_ids.shift();
                                let plan_item = plan_items.shift();
                                items.push({ item_id, price, plan_item });
                              }
                              let created_at = new Date().getTime();
                              let created_at1 = getLocalTime(created_at / 1000);
                              let start_at = null;
                              let stop_at = null;
                              let orders = [order_id, expect_at];
                              let order = {
                                summary: summary, state: state, payment: payment, v_value: v_value, p_price: promotion, stop_at: stop_at, service_ratio: service_ratio, plan: plans2, items: items, expect_at: expect_at, state_code: state_code, id: order_no, order_id: order_id, type: type, vehicle: vehicle, created_at: created_at1, start_at: start_at
                              };
                              let multi = cache.multi();
                              multi.zadd("plan-orders", created_at, order_id);
                              multi.zadd("orders", created_at, order_id);
                              multi.zadd("orders-" + uid, created_at, order_id);
                              multi.hset("orderNo-id", order_no, order_id);
                              multi.hset("order-vid-" + vid, qid, order_id);
                              multi.hset("orderid-vid", order_id, vid);
                              multi.hset("order-entities", order_id, JSON.stringify(order));
                              multi.exec((err3, replies) => {
                                if (err3) {
                                  log.error(err3, "query redis error");
                                } else {
                                  log.info("placeAnOrder: done");
                                }
                                done(); // close db and cache connection
                              });
                            }, (e: Error) => {
                              log.error(e);
                              done(); // close db and cache connection
                            });
                          }
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
    }
  });
});
// let args = [domain, uid, vid, dids, summary, payment];
processor.call("placeAnDriverOrder", (db: PGClient, cache: RedisClient, done: DoneFunction, domain: any, uid: string, vid: string, dids: any, summary: number, payment: number) => {
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
      done();
    } else {
      // vid, dids, summary, payment
      db.query("INSERT INTO orders(id, vid, type, state_code, state, summary, payment) VALUES($1,$2,$3,$4,$5,$6,$7)", [order_id, vid, type, state_code, state, summary, payment], (err: Error, result: ResultSet) => {
        if (err) {
          log.error(err, " insert orders  error in placeAnDriverOrder");
          done();
          return;
        }
        else {
          db.query("INSERT INTO order_events(id, oid, uid, data) VALUES($1,$2,$3,$4)", [event_id, order_id, uid, driver_data], (err: Error, result: ResultSet) => {
            if (err) {
              log.error(err, "insert info order_events error in placeAnDriverOrder");
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
                let p = rpc(domain, servermap["vehicle"], null, "getModelAndVehicleInfo", vid);
                let driver_promises = [];
                for (let did of dids) {
                  let p1 = rpc(domain, servermap["vehicle"], null, "getDriverInfos", vid, did);
                  driver_promises.push(p1);
                }
                p.then((vehicle) => {
                  if (err) {
                    log.info("call vehicle error");
                  } else {
                    async_serial_driver(driver_promises, [], drivers => {
                      log.info("=============================555" + drivers);
                      let vid_doid = [];
                      cache.hget("vid-doid", vid, function (err, result1) {
                        if (err) {
                          log.info("get vid-doid error");
                        } else if (result1 == null) {
                          cache.hset("vid-doid", vid, order_id, function (err, result2) {
                            if (err) {
                              log.info("hset vid-doid in placeAnDriverOrder");
                            } else {
                              log.info("success set vid-doid");
                            }
                          });
                        } else {
                          vid_doid = result1;
                        }
                      });
                      vid_doid.push(order_id);
                      let order = { summary: summary, state: state, payment: payment, drivers: drivers, created_at: created_at1, state_code: state_code, order_id: order_id, type: type, vehicle: vehicle };
                      let order_drivers = { drivers: drivers, vehicle: vehicle };
                      let multi = cache.multi();
                      multi.zadd("driver_orders", created_at, order_id);
                      multi.zadd("orders", created_at, order_id);
                      multi.zadd("orders-" + uid, created_at, order_id);
                      multi.hset("vid-doid", vid, vid_doid);
                      multi.hset("driver-entities-", vid, JSON.stringify(order_drivers));
                      multi.hset("order-entities", order_id, JSON.stringify(order));
                      multi.exec((err3, replies) => {
                        if (err3) {
                          log.error(err3, "query redis error");
                          done();
                        } else {
                          log.info("placeAnDriverOrder: done");
                          done(); // close db and cache connection
                        }
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
processor.call('updateOrderState', (db: PGClient, cache: RedisClient, done: DoneFunction, domain: any, uid: string, vid: string, order_id: string, state_code: string, state: string) => {
  log.info('updateOrderState');
  // orderNo-id  
  let code = parseInt(state_code, 10);
  let type1 = 1;//钱包帐号type  
  let balance: number = null;
  let start_at = null;
  db.query('UPDATE orders SET state_code = $1,state = $2 WHERE id = $3', [code, state, order_id], (err: Error, result: ResultSet) => {
    if (err) {
      log.info(err);
      log.info('err,updateOrderState error');
      done();
    } else {
      let p = rpc(domain, servermap["vehicle"], null, "getVehicleInfo", vid);
      p.then((vehicle) => {
        if (err) {
          log.info("call vehicle error");
        } else {
          let name: string = vehicle["owner"].name;
          let identity_no: string = vehicle["owner"].identity_no;
          let g_name: any;
          let apportion: number = 0.20;
          if (parseInt(identity_no.substr(16, 1)) % 2 == 1) {
            g_name = name + "先生";
          } else {
            g_name = name + "女士";
          }
          let p1 = rpc(domain, servermap["group"], null, "createGroup", g_name, vid, apportion, uid);
          p1.then((result2) => {
            if (err) {
              log.info("call group error");
            } else {
              log.info('call group success for createGroup');
            }
          });
          cache.hget("order-entities", order_id, function (err, replise) {
            if (err) {
              log.info('err,get redis error');
              done();
            } else {
              let order_entities = JSON.parse(replise);
              balance = order_entities["summary"];
              let balance0 = balance * 0.2;
              let balance1 = balance * 0.8;
              let p2 = rpc(domain, servermap["wallet"], null, "createAccount", uid, type1, vid, balance0, balance1);
              p2.then((vehicle) => {
                if (err) {
                  log.info("call vehicle error");
                } else {
                  order_entities["state_code"] = state_code;
                  order_entities["state"] = state;
                  if (start_at !== null) {
                    order_entities["start_at"] = start_at;
                  } else {
                    let multi = cache.multi();
                    multi.hset("vid-poid", vid, order_id);
                    multi.hset("order-entities", order_id, JSON.stringify(order_entities));
                    multi.exec((err, result1) => {
                      if (err) {
                        log.info('err:hset order_entities error');
                        done();
                      } else {
                        log.info('db end in updateOrderState');
                        done();
                      }
                    });
                  }
                }
              });
            }
          });
        }
      });
    }
  });
});

// let args = {uid,vid,items,summary,payment};{piid: price}
processor.call('placeAnSaleOrder', (db: PGClient, cache: RedisClient, done: DoneFunction, uid: string, domain: any, order_id: string, vid: string, pid: string, qid: string, items: any, summary: string, payment: string) => {
  log.info('placeAnOrder');
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
  db.query('BEGIN', (err: Error) => {
    if (err) {
      log.error(err, 'query error');
      done();
    } else {
      db.query('INSERT INTO orders(id, vid, type, state_code, state, summary, payment) VALUES($1, $2, $3, $4, $5, $6, $7)', [order_id, vid, type, state_code, state, summary, payment], (err: Error) => {
        if (err) {
          db.query('ROLLBACK', [], (err: Error) => {
            log.error(err, 'insert into orders error in placeAnSaleOrder');
            done();
          });
        } else {
          db.query('INSERT INTO order_events(id, oid, uid, data) VALUES($1, $2, $3, $4)', [event_id, order_id, uid, sale_data], (err: Error, result: ResultSet) => {
            if (err) {
              db.query('ROLLBACK', [], (err: Error) => {
                log.error(err, 'insert into order_events error');
                done();
              });
            } else {
              db.query('INSERT INTO sale_order_ext(id, oid,pid,qid) VALUES($1, $2,$3,$4)', [event_id, order_id, pid, qid], (err: Error, result: ResultSet) => {
                if (err) {
                  db.query('ROLLBACK', [], (err: Error) => {
                    log.error(err, 'insert into order_events error');
                    done();
                  });
                } else {
                  insert_sale_order_items_recursive(db, done, order_id, pid, items, piids.map(piid => piid), {}, () => {
                    let p = rpc(domain, servermap["vehicle"], null, "getModelAndVehicleInfo", vid);
                    let p1 = rpc(domain, servermap["plan"], null, "getPlan", pid);
                    p.then((vehicle) => {
                      if (err) {
                        log.info("call vehicle error");
                      } else {
                        p1.then((plan) => {
                          if (err) {
                            log.info("call quotation error");
                          }
                          else {
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
                            log.info('piids=============' + piids + 'prices=========' + prices + 'plan_items========================' + plan_items);
                            let len = Math.min(piids.length, prices.length, plan_items.length);
                            log.info('=====length' + len);
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
                                log.error(err3, 'query error');
                              }
                              done(); // close db and cache connection
                            });
                          }
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
    }
  });
});
// let args = [domain, order_id, items, summary, payment];
//修改第三方保险
processor.call('updateAnSaleOrder', (db: PGClient, cache: RedisClient, done: DoneFunction, domain: any, order_id: string, items: any, summary: number, payment: number) => {
  log.info('updateAnSaleOrder');
  // orderNo-id
  let update_at = new Date;
  let piids: Object[] = [];
  let prices: Object[] = [];
  for (let item in items) {
    piids.push(item);
    prices.push(items[item]);
  }
  db.query('BEGIN', (err: Error) => {
    if (err) {
      log.error(err, 'query error');
      done();
    } else {
      db.query('UPDATE orders SET summary = $1,payment = $2,updated_at= $3 WHERE id = $4', [summary, payment, update_at, order_id], (err: Error, result: ResultSet) => {
        if (err) {
          log.info(err);
          log.info('err,updateOrderState error');
          done();
        }
        else {
          update_sale_order_items_recursive(db, done, prices.map(price => price), piids.map(piid => piid), {}, () => {
            cache.hget("order-entities", order_id, function (err, result) {
              if (err) {
                log.info('err,get redis error');
                done();
              } else {
                log.info('================' + result);
                let order_entities = JSON.parse(result);
                order_entities["payment"] = payment;
                order_entities["summary"] = summary;
                for (let piid in items) {
                  if (order_entities["items"]["item_id"] == piid) {
                    order_entities["items"]["price"] = items["piid"];
                  }
                }
                let multi = cache.multi();
                multi.hset("order-entities", order_id, JSON.stringify(order_entities));
                multi.exec((err, result1) => {
                  if (err) {
                    log.info('err:hset order_entities error');
                    done();
                  } else {
                    log.info('db end in updateOrderState');
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
processor.call("createUnderwrite", (db: PGClient, cache: RedisClient, done: DoneFunction, uwid: string, oid: string, plan_time: any, validate_place: string, validate_update_time: any, callback: string) => {
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
        let op = rpc<Object>(domain, servermap["operator"], null, "getOperatorInfo", opid);
        op.then(operator => {
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
            let order = JSON.parse(result)
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
            order["state_code"] = "3";
            order["state"] = "已核保";
            db.query("UPDATE orders SET start_at = $1, stop_at = $2, state_code = '3', state = '已核保' WHERE id = $3", [start_at, stop_at, orderid], (err: Error) => {
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
      })
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
          if (underwrite_result.trim() == "通过") {
            log.info("userid------------" + order["vehicle"]["vehicle"]["user_id"]);
            let openid = rpc<Object>(domain, servermap["profile"], null, "getUserOpenId", order["vehicle"]["vehicle"]["user_id"]);
            log.info("openid------------" + openid);
            let No = order["vehicle"]["vehicle"]["license_no"];
            let CarNo = order["vehicle"]["vehicle"]["familyName"];
            let name = order["vehicle"]["vehicle"]["owner"]["name"];

            http.get(`http://${wxhost}/wx/wxpay/tmsgApplication?user=${openid}&No=${No}&CarNo=${CarNo}&Name=${name}&orderid=${orderid}`, (res) => {
              log.info(`Notify response: ${res.statusCode}`);
              // consume response body
              res.resume();
            }).on("error", (e) => {
              log.error(`Notify error: ${e.message}`);
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






processor.run();

console.log("Start processor at " + config.addr);
