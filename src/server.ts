import { Server, Config, Context, ResponseFunction, Permission, rpc, wait_for_response } from 'hive-server';
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
  msgaddr: 'ipc:///tmp/order.ipc',
  cacheaddr: process.env["CACHE_HOST"]
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
  log.info('getorderstate');
  redis.hget(order_vid + vid, qid, function (err, result) {
    log.info('===========' + result);
    if (err || result == null) {
      rep({ code: 500, state: "not found" });
    } else {
      redis.hget(order_entities, result, function (err1, result1) {
        if (err) {
          log.info(err + 'get order_entities err in getOrderState');
        } else {
          rep(JSON.parse(result1));
        }
      });
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
  let domain = ctx.domain;
  let args = { domain, uid, order_id, vid, plans, qid, pmid, service_ratio, summary, payment, v_value, expect_at };
  log.info('placeplanorder %j', args);
  ctx.msgqueue.send(msgpack.encode({ cmd: "placeAnPlanOrder", args: args }));
  rep({ status: "okay", order_id: order_id });
});
// 下司机单
svc.call('placeAnDriverOrder', permissions, (ctx: Context, rep: ResponseFunction, vid: string, dids: any, summary: string, payment: string) => {
  log.info('getDetail %j', dids);
  let uid = ctx.uid;
  let args = { uid, vid, dids, summary, payment };
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
  let args = { uid, order_id, state_code, state };
  log.info('updateOrderState', args);
  ctx.msgqueue.send(msgpack.encode({ cmd: "updateOrderState", args: args }));
  rep({ status: "okay" });
});

//生成核保
svc.call("createUnderwrite", permissions, (ctx: Context, rep: ResponseFunction, oid: string, plan_time: any, validate_place: string) => {
  log.info("createUnderwrite uuid is " + ctx.uid);
  let validate_update_time = new Date();
  let uwid = uuid.v1();
  let callback = uuid.v1();
  let args = { uwid, oid, plan_time, validate_place, validate_update_time, callback };
  log.info("args: " + args);
  ctx.msgqueue.send(msgpack.encode({ cmd: "createUnderwrite", args: args }));
  wait_for_response(ctx.cache, callback, rep);
});

//修改预约验车地点
svc.call("alterValidatePlace", permissions, (ctx: Context, rep: ResponseFunction, uwid: string, plan_time: any, validate_place: string) => {
  log.info("alterValidatePlace uuid is " + ctx.uid);
  let validate_update_time = new Date();
  let callback = uuid.v1();
  let args = { uwid, plan_time, validate_place, validate_update_time, callback };
  log.info("args: " + args);
  ctx.msgqueue.send(msgpack.encode({ cmd: "alterValidatePlace", args: args }));
  wait_for_response(ctx.cache, callback, rep);
});

//工作人员填充验车信息
svc.call("fillUnderwrite", permissions, (ctx: Context, rep: ResponseFunction, uwid: string, real_place: string, operator: string, certificate_state: number, problem_type: any, problem_description: any, photos: any) => {
  log.info("fillUnderwrite uuid is " + ctx.uid);
  let update_time = new Date();
  let callback = uuid.v1();
  let args = { uwid, real_place, update_time, operator, certificate_state, problem_type, problem_description, callback };
  log.info("args: " + args);
  ctx.msgqueue.send(msgpack.encode({ cmd: "fillUnderwrite", args: args }));
  wait_for_response(ctx.cache, callback, rep);
});

//提交审核结果
svc.call("submitUnderwriteResult", permissions, (ctx: Context, rep: ResponseFunction, uwid: string, underwrite_result: string) => {
  log.info("submitUnderwriteResult uuid is " + ctx.uid);
  let update_time = new Date();
  let callback = uuid.v1();
  let args = { uwid, underwrite_result, update_time, callback };
  log.info("args: " + args);
  ctx.msgqueue.send(msgpack.encode({ cmd: "submitUnderwriteResult", args: args }));
  wait_for_response(ctx.cache, callback, rep);
});

//修改审核结果
svc.call("alterUnderwriteResult", permissions, (ctx: Context, rep: ResponseFunction, uwid: string, underwrite_result: string) => {
  log.info("alterUnderwriteResult uuid is " + ctx.uid);
  let update_time = new Date();
  let callback = uuid.v1();
  let args = { uwid, underwrite_result, update_time, callback };
  log.info("args: " + args);
  ctx.msgqueue.send(msgpack.encode({ cmd: "alterUnderwriteResult", args: args }));
  wait_for_response(ctx.cache, callback, rep);
});

//修改实际验车地点
svc.call("alterRealPlace", permissions, (ctx: Context, rep: ResponseFunction, uwid: string, real_place: string) => {
  log.info("alterRealPlace uuid is " + ctx.uid);
  let update_time = new Date();
  let callback = uuid.v1();
  let args = { uwid, real_place, update_time, callback };
  log.info("args: " + args);
  ctx.msgqueue.send(msgpack.encode({ cmd: "alterRealPlace", args: args }));
  wait_for_response(ctx.cache, callback, rep);
});

//修改备注
svc.call("alterNote", permissions, (ctx: Context, rep: ResponseFunction, uwid: string, note: string) => {
  log.info("alterNote uuid is " + ctx.uid);
  let update_time = new Date();
  let callback = uuid.v1();
  let args = { uwid, note, update_time, callback };
  log.info("args: " + args);
  ctx.msgqueue.send(msgpack.encode({ cmd: "alterNote", args: args }));
  wait_for_response(ctx.cache, callback, rep);
});

//上传现场图片
svc.call("uploadPhotos", permissions, (ctx: Context, rep: ResponseFunction, uwid: string, photo: string) => {
  log.info("uploadPhotos uuid is " + ctx.uid);
  let update_time = new Date();
  let callback = uuid.v1();
  let args = { uwid, photo, callback };
  log.info("args: " + args);
  ctx.msgqueue.send(msgpack.encode({ cmd: "uploadPhotos", args: args }));
  wait_for_response(ctx.cache, callback, rep);
});

//根据订单编号得到核保信息
svc.call("getUnderwriteByOrderNumber", permissions, (ctx: Context, rep: ResponseFunction, oid: string) => {
  log.info("getUnderwriteByOrderNumber uuid is " + ctx.uid);
  let order_info = null;
  redis.hget("order-entities", oid, function (err, result) {
    if (err) {
      rep([]);
    } else {
      if (result != null) {
        order_info = JSON.parse(result);
        let order_id = order_info.order_id;
        redis.hget("underwrite-entities", order_id, function (err2, result2) {
          if (err2) {
            rep([]);
          } else {
            if (result2 != null) {
              rep(JSON.parse(result2));
            } else {
              rep({ code: 404, msg: "Not Found Underwrite" });
            }
          }
        });
      } else {
        rep({ code: 404, msg: "Not Found Order" });
      }
    }
  });
});

//根据订单号得到核保信息
svc.call("getUnderwriteByOrderId", permissions, (ctx: Context, rep: ResponseFunction, order_id: string) => {
  log.info("getUnderwriteByOrderId uuid is " + ctx.uid);
  redis.zrange("underwrite", 0, -1, function (err, result) {
    if (result) {
      let multi = redis.multi();
      for (let uwid of result) {
        multi.hget("underwrite-entities", uwid)
      }
      multi.exec((err2, result2) => {
        if (result2) {
          let uwinfo = null;
          result2.map(underwrite => {
            if (JSON.parse(underwrite).order_id === order_id) {
              uwinfo = JSON.parse(underwrite);
            }
          });
          if (uwinfo == null) {
            rep({
              code: 404,
              msg: "Not found underwrite"
            });
          } else {
            rep(uwinfo);
          }
        } else if (err2) {
          rep({
            code: 500,
            msg: err2.message
          });
        } else {
          rep({
            code: 404,
            msg: "Not found underwrites"
          });
        }
      });
    } else if (err) {
      rep({
        code: 500,
        msg: err.message
      });
    } else {
      rep({
        code: 404,
        msg: "Not found underwrites"
      });
    }
  });
});

//根据核保号得到核保信息
svc.call("getUnderwriteByUWId", permissions, (ctx: Context, rep: ResponseFunction, uwid: string) => {
  log.info("getUnderwriteByUWId uuid is " + ctx.uid + "uwid is " + uwid);
  let order_info = null;
  redis.hget("underwrite-entities", uwid, function (err, result) {
    if (result) {
      rep(JSON.parse(result));
    } else if (err) {
      rep({
        code: 500,
        msg: err.message
      });
    } else {
      rep({
        code: 404,
        msg: "Not found underwrites"
      });
    }
  });
});

console.log('Start service at ' + config.svraddr);

svc.run();
