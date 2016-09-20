"use strict";
const hive_server_1 = require('hive-server');
const Redis = require("redis");
const msgpack = require('msgpack-lite');
const bunyan = require('bunyan');
const hostmap = require('./hostmap');
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
let uuid = require('node-uuid');
let redis = Redis.createClient(6379, "redis");
let order_entities = "order_entities";
let orders = "orders";
let config = {
    svraddr: hostmap.default["order"],
    msgaddr: 'ipc:///tmp/order.ipc'
};
let svc = new hive_server_1.Server(config);
let permissions = [['mobile', true], ['admin', true]];
svc.call('getOrder', permissions, (ctx, rep, order_id) => {
    log.info('getDetail %j', ctx);
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
    log.info('getDetail %j', ctx);
    let uid = "00000000-0000-0000-0000-000000000001";
    redis.hget(orders, uid, function (err, result) {
        if (err) {
            log.info('redis error');
            log.info(err);
            rep([]);
        }
        else {
            log.info(result);
            rep(JSON.parse(result));
        }
    });
});
svc.call('placeAnPlanOrder', permissions, (ctx, rep, vid, plans, qid, pmid, service_ratio, summary, payment, expect_at) => {
    let uid = ctx.uid;
    let args = { ctx: ctx, uid: uid, vid: vid, plans: plans, qid: qid, pmid: pmid, service_ratio: service_ratio, summary: summary, payment: payment, expect_at: expect_at };
    log.info('placeplanorder %j', args);
    ctx.msgqueue.send(msgpack.encode({ cmd: "placeAnPlanOrder", args: args }));
    rep({ status: "okay" });
});
svc.call('placeAnDriverOrder', permissions, (ctx, rep, vid, dids, summary, payment) => {
    log.info('getDetail %j', ctx);
    let uid = ctx.uid;
    let args = { uid: uid, vid: vid, dids: dids, summary: summary, payment: payment };
    ctx.msgqueue.send(msgpack.encode({ cmd: "placeAnDriverOrder", args: args }));
    rep({ status: "okay" });
});
svc.call('placeAnSaleOrder', permissions, (ctx, rep, vid, qid, items, summary, payment) => {
    log.info('getDetail %j', ctx);
    let uid = ctx.uid;
    let args = { uid: uid, vid: vid, qid: qid, items: items, summary: summary, payment: payment };
    ctx.msgqueue.send(msgpack.encode({ cmd: "placeAnSaleOrder", args1: args }));
    rep({ status: "okay" });
});
function ids2objects(key, ids, rep) {
    let multi = redis.multi();
    for (let id of ids) {
        multi.hget(key, id);
    }
    multi.exec(function (err, replies) {
        rep(replies);
    });
}
console.log('Start service at ' + config.svraddr);
svc.run();
