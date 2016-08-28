import { Server, Config, Context, ResponseFunction, Permission } from 'hive-server';
import * as Redis from "redis";
import * as nanomsg from 'nanomsg';
import * as msgpack from 'msgpack-lite';
import * as bunyan from 'bunyan';

let log = bunyan.createLogger({
  name: 'order-server',
  streams: [
    {
      level: 'info',
      path: '/var/log/server-info.log',  // log ERROR and above to a file
      type: 'rotating-file',
      period: '1d',   // daily rotation
      count: 7        // keep 7 back copies
    },
    {
      level: 'error',
      path: '/var/log/server-error.log',  // log ERROR and above to a file
      type: 'rotating-file',
      period: '1w',   // daily rotation
      count: 3        // keep 7 back copies
    }
  ]
});


let uuid = require('node-uuid');
let redis = Redis.createClient(6379, "redis"); // port, host

let driver_order_entity = "driver-order-";
let driver_order_key = "driver_order";
let sale_order_entity = "sale-order-";
let sale_order_key = "sale_order";
let plan_order_entity = "plan-order-";
let plan_order_key = "plan_order";

let config: Config = {
  svraddr: 'tcp://0.0.0.0:4040',
  msgaddr: 'ipc:///tmp/queue.ipc'
};

let svc = new Service(config);

let permissions: Permission[] = [['mobile', true], ['admin', true]];



svc.call('getPlanOrder', permissions, (ctx: Context, rep: ResponseFunction,order_id:string) => {
  // http://redis.io/commands/smembers
  log.info('getDetail %j', ctx);
  redis.lrange(plan_order_entity + order_id,0,-1, function (err, result) {
    if (err) {
      rep([]);
    } else {
      ids2objects(plan_order_key, result, rep);
    }
  });
});
svc.call('getPlanOrders', permissions, (ctx: Context, rep: ResponseFunction) => {
  // http://redis.io/commands/smembers
  log.info('getDetail %j', ctx);
  redis.smembers(plan_order_entity + ctx.uid,0,-1, function (err, result) {
    if (err) {
      rep([]);
    } else {
      ids2objects(plan_order_key, result, rep);
    }
  });
});
svc.call('getDriverOrder', permissions, (ctx: Context, rep: ResponseFunction,order_id:string) => {
  // http://redis.io/commands/smembers
  log.info('getDetail %j', ctx);
  redis.smembers(driver_order_entity + order_id, function (err, result) {
    if (err) {
      rep([]);
    } else {
      ids2objects(driver_order_key, result, rep);
    }
  });
});
svc.call('getDriverOrders', permissions, (ctx: Context, rep: ResponseFunction) => {
  // http://redis.io/commands/smembers
  log.info('getDetail %j', ctx);
  redis.smembers(driver_order_entity + ctx.uid,0,-1, function (err, result) {
    if (err) {
      rep([]);
    } else {
      ids2objects(driver_order_key, result, rep);
    }
  });
});
svc.call('getSaleOrder', permissions, (ctx: Context, rep: ResponseFunction,order_id:string) => {
  // http://redis.io/commands/smembers
  log.info('getDetail %j', ctx);
  redis.smembers(sale_order_entity + order_id, function (err, result) {
    if (err) {
      rep([]);
    } else {
      ids2objects(sale_order_key, result, rep);
    }
  });
});
svc.call('getSaleOrders', permissions, (ctx: Context, rep: ResponseFunction) => {
  // http://redis.io/commands/smembers
  log.info('getDetail %j', ctx);
  redis.smembers(sale_order_entity + ctx.uid, function (err, result) {
    if (err) {
      rep([]);
    } else {
      ids2objects(sale_order_key, result, rep);
    }
  });
});
svc.call('placeAnPlanOrder', permissions, (ctx: Context, rep: ResponseFunction,vid:string, plans:string[], pmid:string, service_ratio:string, summary:string, payment:string) => {
  log.info('getDetail %j', ctx);
  let args = [vid,plans,pmid,service_ratio,summary,payment]
  ctx.msgqueue.send(msgpack.encode({ cmd: "placeAnPlanOrder", args1: args}));
  rep({ status: "okay" });
});
svc.call('placeAnDriverOrder', permissions, (ctx: Context, rep: ResponseFunction,vid:string, dids:string[], summary:string, payment:string) => {
  log.info('getDetail %j', ctx);
  let args = [vid,dids,summary,payment]
  ctx.msgqueue.send(msgpack.encode({ cmd: "placeAnDriverOrder", args2: args}));
  rep({ status: "okay" });
});
svc.call('placeAnSaleOrder', permissions, (ctx: Context, rep: ResponseFunction,vid,items,summary,payment) => {
  log.info('getDetail %j', ctx);
  let args = [vid,items,summary,payment]
  ctx.msgqueue.send(msgpack.encode({ cmd: "placeAnSaleOrder", args3: args}));
  rep({ status: "okay" });
});

//  svc.call('addDriver', permissions, (ctx: Context, rep: ResponseFunction, uid:string, pid:string, 
//  {name, gender, identity_no, phone, identity_frontal_view, identity_rear_view : license_nomodel, vinengine_no, register_date,
//   average_mileage, fuel_type, receipt_no, receipt_date, last_insurance_company, vehicle_license_frontal_view, vehicle_license_rear_view},
//   [name, gender, identity_no, phone, identity_frontal_view, identity_rear_view], 
//   service_ratio:string,price:string, actual_price:string) => {
//  let args = [ctx.uid, pid,{name, gender, identity_no, phone, identity_frontal_view, identity_rear_view : license_nomodel, vinengine_no, register_date,
//      average_mileage, fuel_type, receipt_no, receipt_date, last_insurance_company, vehicle_license_frontal_view, vehicle_license_rear_view},
//      [name, gender, identity_no, phone, identity_frontal_view, identity_rear_view], service_ratio, price, actual_price];
//  ctx.msgqueue.push(msgpack.encode({cmd: "addDriver", args: args,}));
//     rep({status: "okay"});
//  });

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
