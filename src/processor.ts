import { Processor, Config, ModuleFunction, DoneFunction, rpc, } from 'hive-processor';
import { Client as PGClient, ResultSet } from 'pg';
import { createClient, RedisClient} from 'redis';
import * as bunyan from 'bunyan';
import * as uuid from 'uuid';
import * as hostmap from './hostmap';
let log = bunyan.createLogger({
  name: 'order-processor',
  streams: [
    {
      level: 'info',
      path: '/var/log/order-processor-info.log',  // log ERROR and above to a file
      type: 'rotating-file',
      period: '1d',   // daily rotation
      count: 7        // keep 7 back copies
    },
    {
      level: 'error',
      path: '/var/log/order-processor-error.log',  // log ERROR and above to a file
      type: 'rotating-file',
      period: '1w',   // daily rotation
      count: 3        // keep 7 back copies
    }
  ]
});

let config: Config = {
  dbhost: process.env['DB_HOST'],
  dbuser: process.env['DB_USER'],
  dbport: process.env['DB_PORT'],
  database: process.env['DB_NAME'],
  dbpasswd: process.env['DB_PASSWORD'],
  cachehost: process.env['CACHE_HOST'],
  addr: "ipc:///tmp/order.ipc"
};
let processor = new Processor(config);


function formatNum(Source: string, Length: number): string {
  var strTemp = "";
  for (let i = 1; i <= Length - Source.length; i++) {
    strTemp += "0";
  }
  return strTemp + Source;
}
function getLocalTime(nS) {
  return new Date(parseInt(nS) * 1000).toLocaleString().replace(/:\d{1,2}$/, ' ');
}


function insert_order_item_recursive(db, done, plans, pid, args1, piids, acc, cb) {
  if (piids.length == 0) {
    cb(acc);
  } else {
    let item_id = uuid.v1();
    let piid = piids.shift();
    db.query('INSERT INTO order_items(id, pid, piid, price) VALUES($1,$2,$3,$4)', [item_id, pid, piid, plans[pid][piid]], (err: Error) => {
      if (err) {
        log.info(err);
        db.query('ROLLBACK', [], (err: Error) => {
          log.error(err, 'insert into order_items error');
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
  if (pids.length == 0) {
    db.query('COMMIT', [], (err: Error) => {
      if (err) {
        log.info(err);
        log.error(err, 'insert plan order commit error');
        done();
      } else {
        cb(acc);
      }
    });
  } else {
    let ext_id = uuid.v1();
    let pid = pids.shift();
    db.query('INSERT INTO plan_order_ext(oid, pmid, pid, qid, service_ratio, expect_at) VALUES($1,$2,$3,$4,$5,$6)', [order_id, args1.pmid, pid, args1.qid, args1.service_ratio, args1.expect_at], (err: Error, result: ResultSet) => {
      if (err) {
        log.info(err);
        db.query('ROLLBACK', [], (err: Error) => {
          log.error(err, 'insert into plan_order_ext error');
          done();
        });
      } else {
        let piids = [];
        for (var piid in args1.plans[pid]) {
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
  if (dids.length == 0) {
    db.query('COMMIT', [], (err: Error) => {
      if (err) {
        log.info(err);
        log.error(err, 'insert plan order commit error');
        done();
      } else {
        cb(acc);
      }
    });
  } else {
    let did = dids.shift();
    db.query('INSERT INTO driver_order_ext(oid,pid) VALUES($1,$2)', [order_id, did], (err: Error, result: ResultSet) => {
      if (err) {
        log.info(err);
        db.query('ROLLBACK', [], (err: Error) => {
          log.error(err, 'insert into driver_order_ext error');
          done();
        });
      } else {
        insert_driver_order_recursive(db, done, order_id, args2, dids, acc, cb);
      }
    });
  }
}




function async_serial(ps: Promise<any>[], acc: any[], cb: (vals: any[]) => void) {
  if (ps.length == 0) {
    cb(acc);
  } else {
    let p = ps.shift();
    log.info(p + '2222222222===============================');
    p.then(val => {
      acc.push(val);
      async_serial(ps, acc, cb);
    })
      .catch(e => {
        async_serial(ps, acc, cb);
      });
  }
}
function async_serial_driver(ps: Promise<any>[], acc: any[], cb: (vals: any[]) => void) {
  if (ps.length == 0) {
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
processor.call('placeAnPlanOrder', (db: PGClient, cache: RedisClient, done: DoneFunction, args1) => {
  log.info('placeOrder');
  let order_id = args1.order_id;
  let event_id = uuid.v1();
  let state_code = "1";
  let state = "已创建订单";
  let type = "0";
  let plan_data1 = "新增plan计划";
  let plan_data = JSON.stringify(plan_data1);
  cache.incr('order-no', (err, strNo) => {
    if (err || !strNo) {
      log.info(err + 'cache incr err');
    } else {
      let no: string = formatNum(strNo, 7);
      let date = new Date();
      let year = date.getFullYear();
      let pids = [];
      for (var plan in args1.plans) {
        pids.push(plan);
      }
      // log.info(pids);
      let sum = 0;
      for (let i of pids) {
        let str = i.substring(24);
        let num = +str;
        sum += num;
      }
      let sum2 = sum + '';
      let sum1 = formatNum(sum2, 3);
      let order_no = '1' + '110' + '001' + sum1 + year + no;
      db.query('BEGIN', (err: Error) => {
        if (err) {
          log.error(err, 'query error');
          done();
        } else {
          db.query('INSERT INTO orders(id, vid, type, state_code, state, summary, payment) VALUES($1,$2,$3,$4,$5,$6,$7)', [order_id, args1.vid, type, state_code, state, args1.summary, args1.payment], (err: Error) => {
            if (err) {
              db.query('ROLLBACK', [], (err: Error) => {
                log.error(err, 'insert into orders error');
                done();
              });
            } else {
              db.query('INSERT INTO order_events(id, oid, uid, data) VALUES($1, $2, $3, $4)', [event_id, order_id, args1.uid, plan_data], (err: Error, result: ResultSet) => {
                if (err) {
                  db.query('ROLLBACK', [], (err: Error) => {
                    log.error(err, 'insert into order_events error');
                    done();
                  });
                } else {
                  insert_plan_order_recursive(db, done, order_id, args1, pids.map(pid => pid), {}, (pid_oiids_obj) => {
                    let p = rpc(args1.ctx.domain, hostmap.default["vehicle"], null, "getModelAndVehicleInfo", args1.vid);
                    let p2 = rpc(args1.ctx.domain, hostmap.default["quotation"], null, "getQuotation", args1.qid);
                    let plan_promises = [];
                    for (let pid of pids) {
                      let p1 = rpc(args1.ctx.domain, hostmap.default["plan"], null, "getPlan", pid);
                      plan_promises.push(p1);
                    }
                    p.then((vehicle) => {
                      if (err) {
                        log.info("call vehicle error");
                      } else {
                        p2.then((quotation) => {
                          if (err) {
                            log.info("call quotation error");
                          }
                          else {
                            async_serial(plan_promises, [], plans => {
                              let plan_items = [];
                              for (let plan1 of plans) {
                                for (let plan2 in plan1.items) {
                                  plan_items.push(plan1.items[plan2]);
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
                              log.info('piids=============' + piids + 'prices=========' + prices + 'item_ids====================' + item_ids + 'plan_items========================' + plan_items);
                              let len = Math.min(piids.length, prices.length, plan_items.length);
                              log.info('=====length' + len);
                              for (let i = 0; i < len; i++) {
                                let item_id = piids.shift();
                                let price = prices.shift();
                                // let pid = item_ids.shift();
                                let plan_item = plan_items.shift();
                                items.push({ item_id, price, plan_item });
                              }
                              log.info(plan_items + '111111111111111========================');
                              let created_at = new Date().getTime();
                              let created_at1 = getLocalTime(created_at / 1000);
                              let orders = [order_id, args1.expect_at];
                              let order = {
                                summary: args1.summary, state: state, payment: args1.payment, v_value: args1.v_value, stop_at: args1.stop, service_ratio: args1.service_ratio, plan: plans, items: items, expect_at: args1.expect_at, state_code: state_code, id: order_no, order_id: order_id, type: type, vehicle: vehicle
                              };
                              let order_vid = { state, state_code };
                              let multi = cache.multi();
                              multi.zadd("plan-orders", created_at, order_id);
                              multi.zadd("orders", created_at, order_id);
                              multi.zadd("orders-" + args1.uid, created_at, order_id);
                              multi.hset("order-vid-" + args1.vid, args1.qid, JSON.stringify(order_vid));
                              multi.hset("order-entities", order_id, JSON.stringify(order));
                              multi.exec((err3, replies) => {
                                if (err3) {
                                  log.error(err3, 'query redis error');
                                } else {
                                  log.info("placeAnOrder:===================is done")
                                  done(); // close db and cache connection
                                }
                              });
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

// let args = {uid,vid,dids,summary,payment};
processor.call('placeAnDriverOrder', (db: PGClient, cache: RedisClient, done: DoneFunction, args2) => {
  log.info('placeAnOrder');
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
  db.query('BEGIN', (err: Error) => {
    if (err) {
      log.error(err, 'query error');
      done();
    } else {
      // vid, dids, summary, payment
      db.query('INSERT INTO orders(id, vid, type, state_code, state, summary, payment) VALUES($1,$2,$3,$4,$5,$6,$7)', [order_id, args2.vid, type, state_code, state, args2.summary, args2.payment], (err: Error, result: ResultSet) => {
        if (err) {
          log.error(err, ' insert orders  error in placeAnDriverOrder');
          done();
          return;
        }
        else {
          db.query('INSERT INTO order_events(id, oid, uid, data) VALUES($1,$2,$3,$4)', [event_id, order_id, args2.uid, driver_data], (err: Error, result: ResultSet) => {
            if (err) {
              log.error(err, 'insert info order_events error in placeAnDriverOrder');
              done();
              return;
            } else {
              insert_driver_order_recursive(db, done, order_id, args2, args2.dids.map(did => did), {}, () => {
                let p = rpc(args2.ctx.domain, hostmap.default["vehicle"], null, "getModelAndVehicleInfo", args2.vid);
                let driver_promises = [];
                for (let did of args2.dids) {
                  log.info('=================================did' + did);
                  let p1 = rpc(args2.ctx.domain, hostmap.default["vehicle"], null, "getDriverInfos", args2.vid, did);
                  driver_promises.push(p1);
                }
                p.then((vehicle) => {
                  if (err) {
                    log.info("call vehicle error");
                  } else {
                    async_serial_driver(driver_promises, [], drivers => {
                      let order = { summary: args2.summary, state: state, payment: args2.payment, drivers: drivers, created_at: created_at1, state_code: state_code, order_id: order_id, type: type, vehicle: vehicle };
                      let order_drivers = { drivers: drivers, vehicle: vehicle };
                      log.info("===================drivers" + drivers);
                      log.info('====order_drivers' + JSON.stringify(order_drivers));
                      let multi = cache.multi();
                      multi.zadd("driver_orders", created_at, order_id);
                      multi.zadd("orders", created_at, order_id);
                      multi.zadd("orders-" + args2.uid, created_at, order_id);
                      multi.hset("driver-entities-", args2.vid, JSON.stringify(order_drivers));
                      multi.hset("order-entities", order_id, JSON.stringify(order));
                      multi.exec((err3, replies) => {
                        if (err3) {
                          log.error(err3, 'query redis error');
                          done();
                        } else {
                          log.info('placeAnDriverOrder:==========is done');
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

// 订单状态更新state_code state
processor.call('updateOrderState', (db: PGClient, cache: RedisClient, done: DoneFunction, args4) => {
  log.info('updateOrderState');
  let code = parseInt(args4.state_code, 10);
  db.query('UPDATE orders SET state_code = $1,state = $2 WHERE id = $3', [code, args4.state, args4.order_id], (err: Error, result: ResultSet) => {
    if (err) {
      log.info(err);
      log.info('err,updateOrderState error');
      done();
      return;
    }
    else {
      let multi = cache.multi();
      log.info(args4.order_id + '====================================');
      multi.hget("order-entities", args4.order_id);
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
          log.info('=====================1321' + order_entities["state"]);
          log.info('======================1111' + order_entities);
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
      });
    }
  });
});


// let args = {uid,vid,items,summary,payment};{piid: price}
processor.call('placeAnSaleOrder', (db: PGClient, cache: RedisClient, done: DoneFunction, args3) => {
  log.info('placeAnOrder');
  let order_id = uuid.v1();
  let item_id = uuid.v1();
  let event_id = uuid.v1();
  let state_code = "1";
  let state = "已创建订单";
  let type = "2";
  let sale_id = uuid.v1();
  // let plan_data = "新增plan计划"

  db.query('INSERT INTO orders(id, vid, type, state_code, state, summary, payment) VALUES($1,$2,$3,$4,$5,$6,$7)', [order_id, args3.vid, type, state_code, state, args3.summary, args3.payment], (err: Error, result: ResultSet) => {
    if (err) {
      log.error(err, 'query error');
      done();
      return;
    }
    let piid;
    // for (let item in args3.items) {
    //   let a = args3.items.hasOwnProperty(item);
    //   if (a) {
    //     piid.push(item);
    //   }
    // }
    for (var i = 0; i < args3.items.length; i++) {
      for (var item in args3.items[i]) {
        piid.push(item);
      }
    }
    db.query('INSERT INTO sale_order_ext(id, oid, pid, qid) VALUES($1,$2,$3,$4)', [sale_id, order_id, piid, args3.qid], (err: Error, result: ResultSet) => {
      if (err) {
        log.error(err, 'query error');
        done();
        return;
      }
      db.query('INSERT INTO order_items(item_id, piid, price) VALUES($1,$2,$3,$4)', [item_id, piid, args3.items["piid"]], (err: Error, result: ResultSet) => {
        if (err) {
          log.error(err, 'query error');
          done();
          return;
        }
        db.query('INSERT INTO order_events(id, oid, uid, data) VALUES($1,$2,$3,$4)', [event_id, order_id, args3.uid], (err: Error, result: ResultSet) => {
          if (err) {
            log.error(err, 'query error');
            done();
            return;
          }

          // let args = {uid,vid,plans,pmid,service_ratio,summary,payment,expect_at};
          let items = [item_id, piid, args3.items["piid"]];
          let order = [args3.summary, state, args3.payment, args3.stop_at, args3.items.piid, items, args3.updated_at,
            args3.start_at, args3.created_at, state_code, order_id, type, args3.vid];
          let sale_order = [args3.updata_at, order_id];
          let multi = cache.multi();
          multi.sadd("sale_orders", sale_id);
          multi.sadd("orders", order_id);
          multi.sadd("orders-uid", args3.uid);
          multi.hset("order_entities", order_id, JSON.stringify(order));
          multi.exec((err3, replies) => {
            if (err3) {
              log.error(err3, 'query error');
            }
            done(); // close db and cache connection
          });
        });
      });
    });
  });
});

processor.run();

console.log('Start processor at ' + config.addr);
