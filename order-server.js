"use strict";
const hive_server_1 = require('hive-server');
const Redis = require("redis");
const msgpack = require('msgpack-lite');
const bunyan = require('bunyan');
const hostmap = require('./hostmap');
const uuid = require('uuid');
let log = bunyan.createLogger({
    name: 'order-server',
    streams: [
        {
            level: 'info',
            path: '/var/log/order-server-info.log',
            type: 'rotating-file',
            period: '1d',
            count: 7
        },
        {
            level: 'error',
            path: '/var/log/order-server-error.log',
            type: 'rotating-file',
            period: '1w',
            count: 3
        }
    ]
});
let redis = Redis.createClient(6379, "redis");
let order_entities = "order-entities";
let orders = "orders-";
let order_key = "orders";
let driver_entities = "driver-entities-";
let order_vid = "order-vid-";
let config = {
    svraddr: hostmap.default["order"],
    msgaddr: 'ipc:///tmp/order.ipc'
};
let svc = new hive_server_1.Server(config);
let permissions = [['mobile', true], ['admin', true]];
svc.call('getAllOrders', permissions, (ctx, rep, start, limit) => {
    log.info('getallorder');
    redis.zrevrange(order_key, start, limit, function (err, result) {
        if (err) {
            rep([]);
        }
        else {
            let multi = redis.multi();
            for (let id of result) {
                multi.hget(order_entities, id);
            }
            multi.exec((err, result2) => {
                if (err) {
                    rep([]);
                }
                else {
                    rep(result2.map(e => JSON.parse(e)));
                }
            });
        }
    });
});
svc.call('getOrder', permissions, (ctx, rep, order_id) => {
    log.info('getorder');
    redis.hget(order_entities, order_id, function (err, result) {
        if (err) {
            rep([]);
        }
        else {
            rep(JSON.parse(result));
        }
    });
});
svc.call('getOrders', permissions, (ctx, rep, offset, limit) => {
    log.info('getorders');
    redis.zrange(orders + ctx.uid, offset, limit, function (err, result) {
        if (err) {
            log.info('get redis error in getorders');
            log.info(err);
            rep([]);
        }
        else {
            let multi = redis.multi();
            for (let order_key of result) {
                multi.hget(order_entities, order_key);
            }
            multi.exec((err2, replies) => {
                if (err2) {
                    log.error(err2, 'query error');
                }
                else {
                    log.info('replies==========' + replies);
                    rep(replies.map(e => JSON.parse(e)));
                }
            });
        }
    });
});
svc.call('getOrderState', permissions, (ctx, rep, vid, qid) => {
    log.info('getorderstate');
    redis.hget(order_vid + vid, qid, function (err, result) {
        if (err || result == null) {
            rep({ code: 500, state: "not found" });
        }
        else {
            rep(JSON.parse(result));
        }
    });
});
svc.call('getDriverOrders', permissions, (ctx, rep, vid) => {
    log.info('getorders');
    redis.hget(driver_entities, vid, function (err, result) {
        if (err) {
            log.info('get redis error in getDriverOrders');
            log.info(err);
            rep([]);
        }
        else {
            log.info('replies==========' + result);
            rep(JSON.parse(result));
        }
    });
});
svc.call('placeAnPlanOrder', permissions, (ctx, rep, vid, plans, qid, pmid, service_ratio, summary, payment, v_value, expect_at) => {
    let uid = ctx.uid;
    let order_id = uuid.v1();
    let args = { ctx: ctx, uid: uid, order_id: order_id, vid: vid, plans: plans, qid: qid, pmid: pmid, service_ratio: service_ratio, summary: summary, payment: payment, v_value: v_value, expect_at: expect_at };
    log.info('placeplanorder %j', args);
    ctx.msgqueue.send(msgpack.encode({ cmd: "placeAnPlanOrder", args: args }));
    rep({ status: "okay", order_id: order_id });
});
svc.call('placeAnDriverOrder', permissions, (ctx, rep, vid, dids, summary, payment) => {
    log.info('getDetail %j', ctx);
    let uid = ctx.uid;
    let args = { ctx: ctx, uid: uid, vid: vid, dids: dids, summary: summary, payment: payment };
    ctx.msgqueue.send(msgpack.encode({ cmd: "placeAnDriverOrder", args: args }));
    rep({ status: "okay" });
});
svc.call('placeAnSaleOrder', permissions, (ctx, rep, vid, qid, items, summary, payment) => {
    log.info('getDetail %j', ctx);
    let uid = ctx.uid;
    let args = { uid: uid, vid: vid, qid: qid, items: items, summary: summary, payment: payment };
    ctx.msgqueue.send(msgpack.encode({ cmd: "placeAnSaleOrder", args: args }));
    rep({ status: "okay" });
});
svc.call('updateOrderState', permissions, (ctx, rep, order_id, state_code, state) => {
    let uid = ctx.uid;
    let args = { ctx: ctx, uid: uid, order_id: order_id, state_code: state_code, state: state };
    log.info('updateOrderState', args);
    ctx.msgqueue.send(msgpack.encode({ cmd: "updateOrderState", args: args }));
    rep({ status: "okay" });
});
console.log('Start service at ' + config.svraddr);
svc.run();
