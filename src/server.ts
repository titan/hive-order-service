import { Server, Config, Context, ResponseFunction, Permission } from 'hive-server';
import * as Redis from "redis";
import * as nanomsg from 'nanomsg';
import * as msgpack from 'msgpack-lite';
import * as bunyan from 'bunyan';
import * as hostmap from './hostmap';

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


let uuid = require('node-uuid');
let redis = Redis.createClient(6379, "redis"); // port, host

let order_entities  = "order_entities";
let orders = "orders";
// let orders_key = "uid";

let config: Config = {
  svraddr: hostmap.default["order"],
  msgaddr: 'ipc:///tmp/order.ipc'
};

let svc = new Server(config);

let permissions: Permission[] = [['mobile', true], ['admin', true]];


// 获取订单详情
svc.call('getOrder', permissions, (ctx: Context, rep: ResponseFunction,order_id:string) => {
  // http://redis.io/commands/smembers
  log.info('getDetail %j', ctx);
  redis.hget(order_entities, order_id ,function (err, result) {
    if (err) {
      rep([]);
    } else {
      // ids2objects(orders, result, rep);
      rep(JSON.parse(result));
    }
  });
});
// 获取订单列表
svc.call('getOrders', permissions, (ctx: Context, rep: ResponseFunction,offset:string,limit:string) => {
  // http://redis.io/commands/smembers
  log.info('getDetail %j', ctx);
   let uid = "00000000-0000-0000-0000-000000000001";
  redis.hget(orders, uid,function (err, result) {
            // log.info(result);
    if (err) {
      log.info('redis error');
      log.info(err);
      rep([]);
    } else {
      log.info(result);
      rep(JSON.parse(result));
      // ids2objects(uid, result, rep);
    }
  });
});

svc.call('placeAnPlanOrder', permissions, (ctx: Context, rep: ResponseFunction,vid:string, plans:any,qid:string, pmid:string, service_ratio:string, summary:string, payment:string,expect_at:any) => {
  let uid = ctx.uid; 
  let args = {ctx,uid, vid, plans, qid, pmid, service_ratio, summary, payment,expect_at};
    log.info('placeplanorder %j', args );
  ctx.msgqueue.send(msgpack.encode({ cmd: "placeAnPlanOrder", args: args}));
  rep({ status: "okay" });
});
svc.call('placeAnDriverOrder', permissions, (ctx: Context, rep: ResponseFunction,vid:string, dids:string[], summary:string, payment:string) => {
  log.info('getDetail %j', ctx);
  let uid = ctx.uid;
  let args = {uid,vid,dids,summary,payment};
  ctx.msgqueue.send(msgpack.encode({ cmd: "placeAnDriverOrder", args: args}));
  rep({ status: "okay" });
});
svc.call('placeAnSaleOrder', permissions, (ctx: Context, rep: ResponseFunction,vid:string,qid:string, items:string[],summary:string,payment:string) => {
  log.info('getDetail %j', ctx);
  let uid = ctx.uid;
  let args = {uid, vid, qid, items, summary, payment};
  ctx.msgqueue.send(msgpack.encode({ cmd: "placeAnSaleOrder", args1: args}));
  rep({ status: "okay" });
});


function ids2objects(key: string, ids: string[], rep: ResponseFunction) {
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
