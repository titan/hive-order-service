"use strict";
const hive_processor_1 = require('hive-processor');
const bunyan = require('bunyan');
const uuid = require('uuid');
const hostmap = require('./hostmap');
let log = bunyan.createLogger({
    name: 'order-processor',
    streams: [
        {
            level: 'info',
            path: '/var/log/order-processor-info.log',
            type: 'rotating-file',
            period: '1d',
            count: 7
        },
        {
            level: 'error',
            path: '/var/log/order-processor-error.log',
            type: 'rotating-file',
            period: '1w',
            count: 3
        }
    ]
});
let config = {
    dbhost: process.env['DB_HOST'],
    dbuser: process.env['DB_USER'],
    dbport: process.env['DB_PORT'],
    database: process.env['DB_NAME'],
    dbpasswd: process.env['DB_PASSWORD'],
    cachehost: process.env['CACHE_HOST'],
    addr: "ipc:///tmp/order.ipc"
};
let processor = new hive_processor_1.Processor(config);
function formatNum(Source, Length) {
    var strTemp = "";
    for (let i = 1; i <= Length - Source.length; i++) {
        strTemp += "0";
    }
    return strTemp + Source;
}
function insert_order_item_recursive(db, done, plans, pid, args1, piids, acc, cb) {
    if (piids.length == 0) {
        cb(acc);
    }
    else {
        let item_id = uuid.v1();
        let piid = piids.shift();
        log.info(plans[pid][piid] + '================================================');
        db.query('INSERT INTO order_items(id, pid, piid, price) VALUES($1,$2,$3,$4)', [item_id, pid, piid, plans[pid][piid]], (err) => {
            if (err) {
                log.info(err);
                db.query('ROLLBACK', [], (err) => {
                    log.error(err, 'insert into order_items error');
                    done();
                });
            }
            else {
                log.info(item_id + '=========================');
                acc.push(item_id);
                insert_order_item_recursive(db, done, plans, pid, args1, piids, acc, cb);
            }
        });
    }
}
function insert_plan_order_recursive(db, done, order_id, args1, pids, acc, cb) {
    if (pids.length == 0) {
        db.query('COMMIT', [], (err) => {
            if (err) {
                log.info(err);
                log.error(err, 'insert plan order commit error');
                done();
            }
            else {
                cb(acc);
            }
        });
    }
    else {
        let ext_id = uuid.v1();
        let pid = pids.shift();
        db.query('INSERT INTO plan_order_ext(oid, pmid, pid, qid, service_ratio, expect_at) VALUES($1,$2,$3,$4,$5,$6)', [order_id, args1.pmid, pid, args1.qid, args1.service_ratio, args1.expect_at], (err, result) => {
            if (err) {
                log.info(err);
                db.query('ROLLBACK', [], (err) => {
                    log.error(err, 'insert into plan_order_ext error');
                    done();
                });
            }
            else {
                let piids = [];
                for (var piid in args1.plans[pid]) {
                    piids.push(piid);
                }
                log.info(" ================================= piids: " + piids);
                insert_order_item_recursive(db, done, args1.plans, pid, args1, piids, [], (oiids) => {
                    acc[pid] = oiids;
                    insert_plan_order_recursive(db, done, order_id, args1, pids, acc, cb);
                });
            }
        });
    }
}
function async_serial(ps, acc, cb) {
    if (ps.length == 0) {
        cb(acc);
    }
    else {
        let p = ps.shift();
        p.then(val => {
            acc.push(val);
            async_serial(ps, acc, cb);
        })
            .catch(e => {
            async_serial(ps, acc, cb);
        });
    }
}
processor.call('placeAnPlanOrder', (db, cache, done, args1) => {
    log.info('placeOrder');
    let order_id = uuid.v1();
    let event_id = uuid.v1();
    let state_code = "1";
    let state = "已创建订单";
    let type = "0";
    let plan_data1 = "新增plan计划";
    let plan_data = JSON.stringify(plan_data1);
    cache.incr('order-no', (err, strNo) => {
        if (err || !strNo) {
            log.info(err + 'cache incr err');
        }
        else {
            let no = formatNum(strNo, 7);
            let date = new Date();
            let year = date.getFullYear();
            let pids = [];
            for (var plan in args1.plans) {
                pids.push(plan);
            }
            log.info(pids);
            let sum = 0;
            for (let i of pids) {
                let str = i.substring(24);
                let num = +str;
                sum += num;
            }
            let sum2 = sum + '';
            let sum1 = formatNum(sum2, 3);
            let order_no = 'P' + '110' + 'OA' + sum1 + year + no;
            db.query('BEGIN', (err) => {
                if (err) {
                    log.error(err, 'query error');
                    done();
                }
                else {
                    db.query('INSERT INTO orders(id, vid, type, state_code, state, summary, payment) VALUES($1,$2,$3,$4,$5,$6,$7)', [order_id, args1.vid, type, state_code, state, args1.summary, args1.payment], (err) => {
                        if (err) {
                            db.query('ROLLBACK', [], (err) => {
                                log.error(err, 'insert into orders error');
                                done();
                            });
                        }
                        else {
                            db.query('INSERT INTO order_events(id, oid, uid, data) VALUES($1, $2, $3, $4)', [event_id, order_id, args1.uid, plan_data], (err, result) => {
                                if (err) {
                                    db.query('ROLLBACK', [], (err) => {
                                        log.error(err, 'insert into order_events error');
                                        done();
                                    });
                                }
                                else {
                                    insert_plan_order_recursive(db, done, order_id, args1, pids, {}, (pid_oiids_obj) => {
                                        let p = hive_processor_1.rpc(args1.ctx.domain, hostmap.default["vehicle"], null, "getVehicleInfo", args1.vid);
                                        let p2 = hive_processor_1.rpc(args1.ctx.domain, hostmap.default["quotation"], null, "getQuotation", args1.qid);
                                        let plan_promises = [];
                                        for (let pid of pids) {
                                            let p1 = hive_processor_1.rpc(args1.ctx.domain, hostmap.default["plan"], null, "getPlan", pid);
                                            plan_promises.push(p1);
                                        }
                                        p.then((vehicle) => {
                                            if (err) {
                                                log.info("call vehicle error");
                                            }
                                            else {
                                                p2.then((quotation) => {
                                                    if (err) {
                                                        log.info("call quotation error");
                                                    }
                                                    else {
                                                        async_serial(plan_promises, [], plans => {
                                                            let plan_items = [];
                                                            for (let plan1 of plans) {
                                                                let item = plan1.items;
                                                                plan_items.push(item);
                                                            }
                                                            let piids = [];
                                                            for (let pid in pids) {
                                                                for (var item in args1.plans[pid]) {
                                                                    piids.push(item);
                                                                }
                                                            }
                                                            let items = [pid_oiids_obj, args1.plans["pid"]["piid"], pids, plan_items];
                                                            let orders = [order_id, args1.expect_at];
                                                            let order = [args1.summary, state, args1.payment, args1.stop_at, args1.service_ratio, plans, items, args1.expect_at, state_code, order_id, type, vehicle];
                                                            let plan_order = [args1.updata_at, order_id];
                                                            log.info("placeAnOrder:" + JSON.stringify(order));
                                                            let multi = cache.multi();
                                                            let created_at = new Date().getTime();
                                                            multi.zadd("plan-orders", created_at, order_id);
                                                            multi.zadd("orders", created_at, order_id);
                                                            multi.zadd("orders-" + args1.uid, created_at, order_id);
                                                            multi.hset("order-entities", order_id, JSON.stringify(order));
                                                            multi.exec((err3, replies) => {
                                                                if (err3) {
                                                                    log.error(err3, 'query redis error');
                                                                }
                                                                else {
                                                                    log.info("placeAnOrder:===================is done");
                                                                    done();
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
processor.call('placeAnDriverOrder', (db, cache, done, args2) => {
    log.info('placeAnOrder');
    let order_id = uuid.v1();
    let item_id = uuid.v1();
    let event_id = uuid.v1();
    let state_code = "1";
    let state = "已创建订单";
    let type = "1";
    let driver_id = uuid.v1();
    db.query('INSERT INTO orders(id, vid, type,state_code,state, summary, payment) VALUES($1,$2,$3,$4,$5,$6,$7)', [order_id, args2.vid, type, state_code, state, args2.summary, args2.payment], (err, result) => {
        if (err) {
            log.error(err, 'query error');
            done();
            return;
        }
        db.query('INSERT INTO driver_order_ext(id, oid,pid,) VALUES($1,$2,$3)', [driver_id, order_id, args2.dids], (err, result) => {
            if (err) {
                log.error(err, 'query error');
                done();
                return;
            }
            db.query('INSERT INTO order_events(id, oid, uid, data) VALUES($1,$2,$3,$4)', [event_id, order_id, args2.uid], (err, result) => {
                if (err) {
                    log.error(err, 'query error');
                    done();
                    return;
                }
                let order = [args2.summary, state, args2.payment, args2.stop_at, args2.dids, args2.updated_at,
                    args2.start_at, args2.created_at, state_code, order_id, type, args2.vid];
                let driver_order = [args2.updata_at, order_id];
                let multi = cache.multi();
                multi.zadd("driver_orders", driver_id);
                multi.zadd("orders", order_id);
                multi.zadd("orders-uid", args2.uid);
                multi.hset("order_entities", order_id, JSON.stringify(order));
                multi.exec((err3, replies) => {
                    if (err3) {
                        log.error(err3, 'query error');
                    }
                    done();
                });
            });
        });
    });
});
processor.call('placeAnSaleOrder', (db, cache, done, args3) => {
    log.info('placeAnOrder');
    let order_id = uuid.v1();
    let item_id = uuid.v1();
    let event_id = uuid.v1();
    let state_code = "1";
    let state = "已创建订单";
    let type = "2";
    let sale_id = uuid.v1();
    db.query('INSERT INTO orders(id, vid, type,state_code,state, summary, payment) VALUES($1,$2,$3,$4,$5,$6,$7)', [order_id, args3.vid, type, state_code, state, args3.summary, args3.payment], (err, result) => {
        if (err) {
            log.error(err, 'query error');
            done();
            return;
        }
        let piid;
        for (var i = 0; i < args3.items.length; i++) {
            for (var item in args3.items[i]) {
                piid.push(item);
            }
        }
        db.query('INSERT INTO sale_order_ext(id, oid, pid, qid) VALUES($1,$2,$3,$4)', [sale_id, order_id, piid, args3.qid], (err, result) => {
            if (err) {
                log.error(err, 'query error');
                done();
                return;
            }
            db.query('INSERT INTO order_items(item_id, piid, price) VALUES($1,$2,$3,$4)', [item_id, piid, args3.items["piid"]], (err, result) => {
                if (err) {
                    log.error(err, 'query error');
                    done();
                    return;
                }
                db.query('INSERT INTO order_events(id, oid, uid, data) VALUES($1,$2,$3,$4)', [event_id, order_id, args3.uid], (err, result) => {
                    if (err) {
                        log.error(err, 'query error');
                        done();
                        return;
                    }
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
                        done();
                    });
                });
            });
        });
    });
});
processor.run();
console.log('Start processor at ' + config.addr);
