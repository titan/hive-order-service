import { Server, Config, Context, ResponseFunction, Permission } from 'hive-server';
import * as Redis from "redis";
import * as nanomsg from 'nanomsg';
import * as msgpack from 'msgpack-lite';
import * as bunyan from 'bunyan';
import * as hostmap from './hostmap';
import * as uuid from 'uuid';

let log = bunyan.createLogger({
  name: 'order-server',
  streams: [
    {
      level: 'info',
      path: '/var/log/order-server-info.log',  // log ERROR and above to a file
      type: 'rotating-file',
      period: '1d',   // daily rotation
      count: 7        // keep 7 back copies
    },
    {
      level: 'error',
      path: '/var/log/order-server-error.log',  // log ERROR and above to a file
      type: 'rotating-file',
      period: '1w',   // daily rotation
      count: 3        // keep 7 back copies
    }
  ]
});


let redis = Redis.createClient(6379, "redis"); // port, host

let order_entities = "order-entities";
let orders = "orders-";
let order_key = "orders";
let driver_entities = "driver-entities-";
let order_vid = "order-vid-";
// let orders_key = "uid";

let config: Config = {
  svraddr: hostmap.default["order"],
  msgaddr: 'ipc:///tmp/order.ipc'
};

let svc = new Server(config);

let permissions: Permission[] = [['mobile', true], ['admin', true]];
// 获取所有订单
svc.call('getAllOrders', permissions, (ctx: Context, rep: ResponseFunction, start: string, limit: string) => {
  // http://redis.io/commands/smembers
  log.info('getallorder');
  redis.zrevrange(order_key, start, limit, function (err, result) {
    if (err) {
      rep([]);
    } else {
      let multi = redis.multi();
      for (let id of result) {
        multi.hget(order_entities, id);
      }
      multi.exec((err, result2) => {
        if (err) {
          rep([]);
        } else {
          rep(result2.map(e => JSON.parse(e)));
        }
      });
    }
  });
});
// 获取订单详情
svc.call('getOrder', permissions, (ctx: Context, rep: ResponseFunction, order_id: string) => {
  // http://redis.io/commands/smembers
  log.info('getorder');
  redis.hget(order_entities, order_id, function (err, result) {
    if (err) {
      rep([]);
    } else {
      rep(JSON.parse(result));
    }
  });
});
// 获取订单列表
svc.call('getOrders', permissions, (ctx: Context, rep: ResponseFunction, offset: string, limit: string) => {
  // http://redis.io/commands/smembers
  log.info('getorders');
  redis.zrange(orders + ctx.uid, offset, limit, function (err, result) {
    // log.info(result);
    if (err) {
      log.info('get redis error in getorders');
      log.info(err);
      rep([]);
    } else {
      let multi = redis.multi();
      for (let order_key of result) {
        multi.hget(order_entities, order_key);
      }
      multi.exec((err2, replies) => {
        if (err2) {
          log.error(err2, 'query error');
        } else {
          log.info('replies==========' + replies);
          rep(replies.map(e => JSON.parse(e)));
        }
      });
    }
  });
});
//查看订单状态
svc.call('getOrderState', permissions, (ctx: Context, rep: ResponseFunction, vid: string, qid: string) => {
  // http://redis.io/commands/smembers
  log.info('getorderstate');
  redis.hget(order_vid + vid, qid, function (err, result) {
    if (err || result == null) {
      rep({ code: 500, state: "not found" });
    } else {
      rep(JSON.parse(result));
    }
  });
});


// 获取驾驶人信息
svc.call('getDriverOrders', permissions, (ctx: Context, rep: ResponseFunction, vid: string) => {
  // http://redis.io/commands/smembers
  log.info('getorders');
  redis.hget(driver_entities, vid, function (err, result) {
    if (err) {
      log.info('get redis error in getDriverOrders');
      log.info(err);
      rep([]);
    } else {
      log.info('replies==========' + result);
      rep(JSON.parse(result));
    }
  });
});




// 下计划单
svc.call('placeAnPlanOrder', permissions, (ctx: Context, rep: ResponseFunction, vid: string, plans: any, qid: string, pmid: string, service_ratio: string, summary: string, payment: string, v_value: string, expect_at: any) => {
  let uid = ctx.uid;
  let order_id = uuid.v1();
  let args = { ctx, uid, order_id, vid, plans, qid, pmid, service_ratio, summary, payment, v_value, expect_at };
  log.info('placeplanorder %j', args);
  ctx.msgqueue.send(msgpack.encode({ cmd: "placeAnPlanOrder", args: args }));
  rep({ status: "okay", order_id: order_id });
});
// 下司机单
svc.call('placeAnDriverOrder', permissions, (ctx: Context, rep: ResponseFunction, vid: string, dids: any, summary: string, payment: string) => {
  log.info('getDetail %j', dids);
  let uid = ctx.uid;
  let args = { ctx, uid, vid, dids, summary, payment };
  ctx.msgqueue.send(msgpack.encode({ cmd: "placeAnDriverOrder", args: args }));
  rep({ status: "okay" });
});

// 下第三方订单
svc.call('placeAnSaleOrder', permissions, (ctx: Context, rep: ResponseFunction, vid: string, qid: string, items: string[], summary: string, payment: string) => {
  log.info('getDetail %j', ctx);
  let uid = ctx.uid;
  let args = { uid, vid, qid, items, summary, payment };
  ctx.msgqueue.send(msgpack.encode({ cmd: "placeAnSaleOrder", args: args }));
  rep({ status: "okay" });
});

// 更改订单状态
svc.call('updateOrderState', permissions, (ctx: Context, rep: ResponseFunction, order_id: any, state_code: string, state: string) => {
  let uid = ctx.uid;
  let args = { ctx, uid, order_id, state_code, state };
  log.info('updateOrderState', args);
  ctx.msgqueue.send(msgpack.encode({ cmd: "updateOrderState", args: args }));
  rep({ status: "okay" });
});



console.log('Start service at ' + config.svraddr);

svc.run();
