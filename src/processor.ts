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


function insert_sale_order_items_recursive(db, done, order_id, args3, piids, acc, cb) {
  if (piids.length == 0) {
    cb(acc);
  } else {
    let item_id = uuid.v1();
    let piid = piids.shift();
    db.query('INSERT INTO order_items(id,piid,pid, price) VALUES($1,$2,$3,$4)', [item_id, piid, args3.pid, args3.items[piid]], (err: Error) => {
      if (err) {
        log.info(err);
        db.query('ROLLBACK', [], (err: Error) => {
          log.error(err, 'insert into order_items error');
          done();
        });
      } else {
        acc.push(item_id);
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
processor.call("placeAnPlanOrder", (db: PGClient, cache: RedisClient, done: DoneFunction, args1) => {
  log.info("placeOrder");
  let order_id = args1.order_id;
  let event_id = uuid.v1();
  let state_code = "1";
  let state = "已创建订单";
  let type = "0";
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
      for (let plan in args1.plans) {
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
          db.query("INSERT INTO orders(id, vid, type, state_code, state, summary, payment) VALUES($1,$2,$3,$4,$5,$6,$7)", [order_id, args1.vid, type, state_code, state, args1.summary, args1.payment], (err: Error) => {
            if (err) {
              db.query("ROLLBACK", [], (err: Error) => {
                log.error(err, "insert into orders error");
                done();
              });
            } else {
              db.query("INSERT INTO order_events(id, oid, uid, data) VALUES($1, $2, $3, $4)", [event_id, order_id, args1.uid, plan_data], (err: Error, result: ResultSet) => {
                if (err) {
                  db.query("ROLLBACK", [], (err: Error) => {
                    log.error(err, "insert into order_events error");
                    done();
                  });
                } else {
                  insert_plan_order_recursive(db, done, order_id, args1, pids.map(pid => pid), {}, (pid_oiids_obj) => {
                    let p = rpc<Object>(args1.domain, servermap["vehicle"], null, "getModelAndVehicleInfo", args1.vid);
                    let p2 = rpc<Object>(args1.domain, servermap["quotation"], null, "getQuotation", args1.qid);
                    let plan_promises: Promise<Object>[] = [];
                    for (let pid of pids) {
                      let p1 = rpc<Object>(args1.domain, servermap["plan"], null, "getPlan", pid);
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
                            async_serial<Object>(plan_promises, [], (plans: Object[]) => {
                              let plan_items = [];
                              for (let plan1 of plans) {
                                for (let piid in plan1["items"]) {
                                  plan_items.push(plan1["items"][piid]);
                                }
                              }
                              let piids = [];
                              let prices = [];
                              for (let pid in args1.plans) {
                                for (let piid in args1.plans[pid]) {
                                  piids.push(piid);
                                  prices.push(args1.plans[pid][piid]);
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
                              let orders = [order_id, args1.expect_at];
                              let order = {
                                summary: args1.summary, state: state, payment: args1.payment, v_value: args1.v_value, p_price: args1.promotion, stop_at: args1.stop, service_ratio: args1.service_ratio, plan: plans, items: items, expect_at: args1.expect_at, state_code: state_code, id: order_no, order_id: order_id, type: type, vehicle: vehicle, created_at: created_at1, start_at: start_at
                              };
                              let multi = cache.multi();
                              multi.zadd("plan-orders", created_at, order_id);
                              multi.zadd("orders", created_at, order_id);
                              multi.zadd("orders-" + args1.uid, created_at, order_id);
                              multi.hset("orderNo-id", order_no, order_id);
                              multi.hset("order-vid-" + args1.vid, args1.qid, order_id);
                              multi.hset("orderid-vid", order_id, args1.vid);
                              multi.hset("order-entities", order_id, JSON.stringify(order));
                              // multi.setex(args1.callback, 30, JSON.stringify({
                              //   code: 200,
                              //   order_id: order_id
                              // }));
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

processor.call("placeAnDriverOrder", (db: PGClient, cache: RedisClient, done: DoneFunction, args2) => {
  log.info("placeAnOrder");
  let order_id = uuid.v1();
  let item_id = uuid.v1();
  let event_id = uuid.v1();
  let state_code = "1";
  let state = "已支付";
  let type = "1";
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
      db.query("INSERT INTO orders(id, vid, type, state_code, state, summary, payment) VALUES($1,$2,$3,$4,$5,$6,$7)", [order_id, args2.vid, type, state_code, state, args2.summary, args2.payment], (err: Error, result: ResultSet) => {
        if (err) {
          log.error(err, " insert orders  error in placeAnDriverOrder");
          done();
          return;
        }
        else {
          db.query("INSERT INTO order_events(id, oid, uid, data) VALUES($1,$2,$3,$4)", [event_id, order_id, args2.uid, driver_data], (err: Error, result: ResultSet) => {
            if (err) {
              log.error(err, "insert info order_events error in placeAnDriverOrder");
              done();
              return;
            } else {
              insert_driver_order_recursive(db, done, order_id, args2, args2.dids.map(did => did), {}, () => {
                let p = rpc(args2.domain, servermap["vehicle"], null, "getModelAndVehicleInfo", args2.vid);
                let driver_promises = [];
                for (let did of args2.dids) {
                  let p1 = rpc(args2.domain, servermap["vehicle"], null, "getDriverInfos", args2.vid, did);
                  driver_promises.push(p1);
                }
                p.then((vehicle) => {
                  if (err) {
                    log.info("call vehicle error");
                  } else {
                    async_serial_driver(driver_promises, [], drivers => {
                      let order = { summary: args2.summary, state: state, payment: args2.payment, drivers: drivers, created_at: created_at1, state_code: state_code, order_id: order_id, type: type, vehicle: vehicle };
                      let order_drivers = { drivers: drivers, vehicle: vehicle };
                      let multi = cache.multi();
                      multi.zadd("driver_orders", created_at, order_id);
                      multi.zadd("orders", created_at, order_id);
                      multi.zadd("orders-" + args2.uid, created_at, order_id);
                      multi.hset("driver-entities-", args2.vid, JSON.stringify(order_drivers));
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
processor.call('updateOrderState', (db: PGClient, cache: RedisClient, done: DoneFunction, args4) => {
  log.info('updateOrderState');
  // orderNo-id  
  let code = parseInt(args4.state_code, 10);
  let type1 = 1;//钱包帐号type  
  let balance: number = null;
  let start_at = null;
  db.query('UPDATE orders SET state_code = $1,state = $2 WHERE id = $3', [code, args4.state, args4.order_id], (err: Error, result: ResultSet) => {
    if (err) {
      log.info(err);
      log.info('err,updateOrderState error');
      done();
    } else {
      let p = rpc(args4.domain, servermap["vehicle"], null, "getVehicleInfo", args4.vid);
      p.then((vehicle) => {
        if (err) {
          log.info("call vehicle error");
        } else {
          if (code == 2) {
            let name = vehicle.owner.name;
            let identity_no = vehicle.owner.identity_no;
            let g_name: any;
            let apportion: number = 0.20;
            if (parseInt(identity_no.substr(16, 1)) % 2 == 1) {
              g_name = name + "先生";
            } else {
              g_name = name + "女士";
            }
            let p1 = rpc(args4.domain, servermap["group"], null, "createGroup", g_name, args4.vid, apportion, args4.uid);
            p1.then((result2) => {
              if (err) {
                log.info("call group error");
              } else {
                log.info('call group success for createGroup');
              }
            });
          } else if (code == 3) {
            let multi = cache.multi();
            multi.hget("order-entities", args4.order_id);
            multi.exec((err, replise) => {
              if (err) {
                log.info('err,get redis error');
                done();
              }
              else {
                let order_entities = JSON.parse(replise);
                // balance = order_entities["summary"];
                let expect_at = order_entities["expect_at"];
                let date = new Date();
                let date_now = date.getTime();
                let expect_at1 = (new Date(expect_at)).getTime();
                if (date_now - expect_at1 > 0) {
                  start_at = date.getFullYear() + "-" + (date.getMonth() + 1) + "-" + (date.getDate() + 1) + " " + '00:00:00';
                }
                if (date_now - expect_at1 < 0) {
                  log.info('核保完成时间小于期望时间');
                }
              }
            });
          } else {
            log.info('out of 2,3 in order state_code');
          }
          let multi = cache.multi();
          multi.hget("order-entities", args4.order_id);
          multi.exec((err, replise) => {
            if (err) {
              log.info('err,get redis error');
              done();
            } else {
              let order_entities = JSON.parse(replise);
              balance = order_entities["summary"];
              let balance0 = balance * 0.2;
              let balance1 = balance * 0.8;
              let p2 = rpc(args4.domain, servermap["wallet"], null, "createAccount", args4.uid, type1, args4.vid, balance0, balance1);
              p2.then((vehicle) => {
                if (err) {
                  log.info("call vehicle error");
                } else {
                  order_entities["state_code"] = args4.state_code;
                  order_entities["state"] = args4.state;
                  if (start_at !== null) {
                    order_entities["start_at"] = start_at;
                  } else {
                    let multi = cache.multi();
                    multi.hset("order-entities", args4.order_id, JSON.stringify(order_entities));
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
processor.call('placeAnSaleOrder', (db: PGClient, cache: RedisClient, done: DoneFunction, args3) => {
  log.info('placeAnOrder');
  let order_id = args3.order_id;
  let item_id = uuid.v1();
  let event_id = uuid.v1();
  let state_code = "1";
  let state = "已创建订单";
  let type = "2";
  let sale_id = uuid.v1();
  let sale_data = "新增第三方代售订单";
  let piids = [];
  for (var item in args3.items) {
    piids.push(item);
  }
  db.query('BEGIN', (err: Error) => {
    if (err) {
      log.error(err, 'query error');
      done();
    } else {
      db.query('INSERT INTO orders(id, vid, type, state_code, state, summary, payment) VALUES($1, $2, $3, $4, $5, $6, $7)', [order_id, args3.vid, type, state_code, state, args3.summary, args3.payment], (err: Error) => {
        if (err) {
          db.query('ROLLBACK', [], (err: Error) => {
            log.error(err, 'insert into orders error in placeAnSaleOrder');
            done();
          });
        } else {
          db.query('INSERT INTO order_events(id, oid, uid, data) VALUES($1, $2, $3, $4)', [event_id, order_id, args3.uid, sale_data], (err: Error, result: ResultSet) => {
            if (err) {
              db.query('ROLLBACK', [], (err: Error) => {
                log.error(err, 'insert into order_events error');
                done();
              });
            } else {
              db.query('INSERT INTO sale_order_ext(id, oid,pid,qid) VALUES($1, $2)', [event_id, order_id, args3.pid, args3.qid], (err: Error, result: ResultSet) => {
                if (err) {
                  db.query('ROLLBACK', [], (err: Error) => {
                    log.error(err, 'insert into order_events error');
                    done();
                  });
                } else {
                  insert_sale_order_items_recursive(db, done, order_id, args3, piids.map(pid => pid), {}, () => {
                    let p = rpc(args3.domain, servermap["vehicle"], null, "getModelAndVehicleInfo", args3.vid);
                    let p1 = rpc(args3.domain, servermap["plan"], null, "getPlan", args3.pid);
                    p.then((vehicle) => {
                      if (err) {
                        log.info("call vehicle error");
                      } else {
                        p1.then((plan) => {
                          if (err) {
                            log.info("call quotation error");
                          }
                          else {
                            let plan_items = plan.items;
                            let piids = [];
                            for (let item of plan_items) {
                              piids.push(item.id);
                            }
                            let prices = [];
                            for (var item in args3.items) {
                              piids.push(args3.items[item]);
                            }
                            let items = [];
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
                            let orders = [order_id, args3.expect_at];
                            let order = {
                              summary: args3.summary, state: state, payment: args3.payment, plan: plan, items: items, state_code: state_code, id: order_id, type: type, vehicle: vehicle, created_at: created_at1
                            };
                            let multi = cache.multi();
                            multi.zadd("sale-orders", created_at, order_id);
                            multi.zadd("orders", created_at, order_id);
                            multi.zadd("orders-" + args3.uid, created_at, order_id);
                            multi.hset("vehicle-order", args3.vid, order_id);
                            multi.hset("orderid-vid", order_id, args3.vid);
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

//修改第三方保险
processor.call('updateAnSaleOrder', (db: PGClient, cache: RedisClient, done: DoneFunction, args5) => {
  log.info('updateAnSaleOrder');
  // orderNo-id
  let start_at = null;
  db.query('UPDATE orders SET state_code = $1,state = $2 WHERE id = $3', [code, args4.state, args5.order_id], (err: Error, result: ResultSet) => {
    if (err) {
      log.info(err);
      log.info('err,updateOrderState error');
      done();
    }
    else {
      let multi = cache.multi();
      log.info(args5.order_id + '====================================');
      multi.hget("order-entities", args5.order_id);
      multi.exec((err, replise) => {
        if (err) {
          log.info('err,get redis error');
          done();
        } else {
          log.info('================' + replise);
          let order_entities = JSON.parse(replise);
          log.info(order_entities);
          order_entities["state_code"] = args4.state_code;
          log.info("=============" + order_entities["state_code"]);
          order_entities["state"] = args4.state;
          order_entities["start_at"] = start_at;
          log.info('=====================1321' + order_entities["state"]);
          log.info('======================1111' + order_entities);
          let multi = cache.multi();
          multi.hset("order-entities", args5.order_id, JSON.stringify(order_entities));
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
    }
  });
});
// 生成核保
processor.call("createUnderwrite", (db: PGClient, cache: RedisClient, done: DoneFunction, args) => {
  log.info("createUnderwrite " + args);
  let pcreate = new Promise<void>((resolve, reject) => {
    db.query("INSERT INTO underwrites (id, oid, plan_time, validate_place, validate_update_time) VALUES ($1, $2, $3, $4, $5)", [args.uwid, args.oid, args.plan_time, args.validate_place, args.validate_update_time], (err: Error) => {
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
      id: args.uwid,
      order_id: args.oid,
      plan_time: args.plan_time,
      validate_place: args.validate_place,
      validate_update_time: args.validate_update_time,
      created_at: now,
      updated_at: now
    };
    let multi = cache.multi();
    let uwid = args.uwid;
    multi.hset("underwrite-entities", uwid, JSON.stringify(underwrite));
    multi.zadd("underwrite", now.getTime(), uwid);
    multi.setex(args.callback, 30, JSON.stringify({
      code: 200,
      uwid: uwid
    }));
    multi.exec((err: Error, _) => {
      if (err) {
        log.error(err, "update underwrite cache error");
      }
      done();
    });
    order_trigger.send(msgpack.encode({ uwid, underwrite }));
  }).catch(error => {
    cache.setex(args.callback, 30, JSON.stringify({
      code: 500,
      msg: error.message
    }));
    log.info("err" + error);
    done();
  });
});

// 修改预约验车地点
processor.call("alterValidatePlace", (db: PGClient, cache: RedisClient, done: DoneFunction, args) => {
  log.info("alterValidatePlace " + args);
  modifyUnderwrite(db, cache, done, args.uwid, args.callback, "UPDATE underwrites SET plan_time = $1, validate_place = $2, validate_update_time = $3, updated_at = $4 WHERE id = $5", [args.plan_time, args.validate_place, args.validate_update_time, args.validate_update_time, args.uwid], (underwrite) => {
    underwrite["plan_time"] = args.plan_time;
    underwrite["validate_place"] = args.validate_place;
    underwrite["validate_update_time"] = args.validate_update_time;
    underwrite["updated_at"] = args.validate_update_time;
    return underwrite;
  });
});

processor.call("fillUnderwrite", (db: PGClient, cache: RedisClient, done: DoneFunction, args) => {
  log.info("fillUnderwrite args is " + JSON.stringify(args));
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
    db.query("UPDATE underwrites SET real_place = $1, real_update_time = $2, opid = $3, certificate_state = $4, problem_type = $5, problem_description = $6, note = $7, note_update_time = $8, updated_at = $9 WHERE id = $10", [args.real_place, args.update_time, args.opid, args.certificate_state, args.problem_type, args.problem_description, args.note, args.update_time, args.update_time, args.uwid], (err: Error) => {
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
  for (let photo of args.photos) {
    let pphoto = new Promise<void>((resolve, reject) => {
      let upid = uuid.v1();
      db.query("INSERT INTO underwrite_photos (id, uwid, photo) VALUES ($1, $2, $3)", [upid, args.uwid, photo], (err: Error) => {
        if (err) {
          log.info("query error" + err);
          reject(err);
        } else {
          let photo_entry = {
            id: upid,
            photo: photo,
            created_at: args.update_time,
            updated_at: args.update_time
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
    let uwid = args.uwid;
    cache.hget("underwrite-entities", args.uwid, function (err, result) {
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
    let uwid = args.uwid;
    cache.hget("underwrite-entities", args.uwid, function (err, result) {
      if (result) {
        let op = rpc<Object>(args.domain, servermap["operator"], null, "getOperatorInfo", args.opid);
        op.then(operator => {
          let underwrite = JSON.parse(result);
          underwrite.real_place = args.real_place;
          underwrite.real_update_time = args.update_time;
          underwrite.operator = operator;
          underwrite.certificate_state = args.certificate_state;
          underwrite.problem_type = args.problem_type;
          underwrite.problem_description = args.problem_description;
          underwrite.note = args.note;
          underwrite.note_updat_time = args.update_time;
          underwrite.update_time = args.update_time;
          underwrite.photos = photo_entities;
          log.info("underwrite" + underwrite);
          let multi = cache.multi();
          multi.hset("underwrite-entities", uwid, JSON.stringify(underwrite));
          multi.setex(args.callback, 30, JSON.stringify({
            code: 200,
            uwid: uwid
          }));
          multi.exec((err: Error, _) => {
            log.info("args.callback" + args.callback);
            if (err) {
              log.info(err);
            }
            order_trigger.send(msgpack.encode({ uwid, underwrite }));
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
      cache.setex(args.callback, 30, JSON.stringify({
        code: 500,
        msg: err.message
      }));
      done();
    });
    log.info("err" + e);
  });
});

// 提交审核结果
processor.call("submitUnderwriteResult", (db: PGClient, cache: RedisClient, done: DoneFunction, args) => {
  log.info("submitUnderwriteResult " + args);
  modifyUnderwrite(db, cache, done, args.uwid, args.callback, "UPDATE underwrites SET underwrite_result = $1, result_update_time = $2, updated_at = $3 WHERE id = $4", [args.underwrite_result, args.update_time, args.update_time, args.uwid], (underwrite) => {
    underwrite["underwrite_result"] = args.underwrite_result;
    underwrite["result_update_time"] = args.update_time;
    underwrite["updated_at"] = args.update_time;
    return underwrite;
  });
});

// 修改审核结果
processor.call("alterUnderwriteResult", (db: PGClient, cache: RedisClient, done: DoneFunction, args) => {
  log.info("alterUnderwriteResult " + args);
  modifyUnderwrite(db, cache, done, args.uwid, args.callback, "UPDATE underwrites SET underwrite_result = $1, result_update_time = $2, updated_at = $3 WHERE id = $4", [args.underwrite_result, args.update_time, args.update_time, args.uwid], (underwrite) => {
    underwrite["underwrite_result"] = args.underwrite_result;
    underwrite["result_update_time"] = args.update_time;
    underwrite["updated_at"] = args.update_time;
    return underwrite;
  });
});

// 修改实际验车地点
processor.call("alterRealPlace", (db: PGClient, cache: RedisClient, done: DoneFunction, args) => {
  log.info("alterRealPlace " + args);
  modifyUnderwrite(db, cache, done, args.uwid, args.callback, "UPDATE underwrites SET real_place = $1, real_update_time = $2, updated_at = $3 WHERE id = $4", [args.real_place, args.update_time, args.update_time, args.uwid], (underwrite) => {
    underwrite["real_place"] = args.real_place;
    underwrite["real_update_time"] = args.update_time;
    underwrite["updated_at"] = args.update_time;
    return underwrite;
  });
});

// 修改备注
processor.call("alterNote", (db: PGClient, cache: RedisClient, done: DoneFunction, args) => {
  log.info("alterNote " + args);
  modifyUnderwrite(db, cache, done, args.uwid, args.callback, "UPDATE underwrites SET note = $1, note_update_time = $2, updated_at = $3 WHERE id = $4", [args.note, args.update_time, args.update_time, args.uwid], (underwrite) => {
    underwrite["note"] = args.note;
    underwrite["note_update_time"] = args.update_time;
    underwrite["updated_at"] = args.update_time;
    return underwrite;
  });
});

// 上传现场图片
processor.call("uploadPhotos", (db: PGClient, cache: RedisClient, done: DoneFunction, args) => {
  log.info("uploadPhotos " + args);
  let upid = uuid.v1();
  modifyUnderwrite(db, cache, done, args.uwid, args.callback, "INSERT INTO underwrite_photos (id, uwid, photo) VALUES ($1, $2, $3)", [upid, args.uwid, args.photo], (underwrite) => {
    let photo_entry = {
      id: upid,
      photo: args.photo,
      created_at: args.update_time,
      updated_at: args.update_time
    };
    underwrite["photos"].push(photo_entry);
    return underwrite;
  });
});

function modifyUnderwrite(db: PGClient, cache: RedisClient, done: DoneFunction, uwid: string, cbflag: string, sql: string, args: any[], cb: ((underwrite: Object) => Object)): void {
  new Promise<void>((resolve, reject) => {
    db.query(sql, args, (err: Error) => {
      if (err) {
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
