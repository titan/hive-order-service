import { Server, Config, Context, ResponseFunction, Permission, rpc, wait_for_response } from "hive-server";
import { servermap } from "hive-hostmap";
import * as Redis from "redis";
import * as nanomsg from "nanomsg";
import * as msgpack from "msgpack-lite";
import * as bunyan from "bunyan";
import * as uuid from "node-uuid";
import { verify, uuidVerifier, stringVerifier, numberVerifier} from "hive-verify";

let log = bunyan.createLogger({
  name: "order-server",
  streams: [
    {
      level: "info",
      path: "/var/log/order-server-info.log",  // log ERROR and above to a file
      type: "rotating-file",
      period: "1d",   // daily rotation
      count: 7        // keep 7 back copies
    },
    {
      level: "error",
      path: "/var/log/order-server-error.log",  // log ERROR and above to a file
      type: "rotating-file",
      period: "1w",   // daily rotation
      count: 3        // keep 7 back copies
    }
  ]
});

let order_entities = "order-entities";
let orders = "orders-";
let order_key = "orders";
let plan_order_key = "plan-orders";
let driver_entities = "driver-entities-";
let order_vid = "order-vid-";
let orderNo_id = "orderNo-id";
let orderid_vid = "orderid-vid";
let vid_poid = "vid-poid";
let vid_doid = "vid-doid";
// let orders_key = "uid";

let config: Config = {
  svraddr: servermap["order"],
  msgaddr: "ipc:///tmp/order.ipc",
  cacheaddr: process.env["CACHE_HOST"]
};

let svc = new Server(config);

let permissions: Permission[] = [["mobile", true], ["admin", true]];
let allowAll: Permission[] = [["mobile", true], ["admin", true]];
let mobileOnly: Permission[] = [["mobile", true], ["admin", false]];

function formatNum(Source: string, Length: number): string {
  let strTemp = "";
  for (let i = 1; i <= Length - Source.length; i++) {
    strTemp += "0";
  }
  return strTemp + Source;
}

// 获取所有订单
svc.call("getAllOrders", permissions, (ctx: Context, rep: ResponseFunction, start: number, limit: number) => {
  log.info("getallorder");
  if (!verify([numberVerifier("start", start), numberVerifier("limit", limit)], (errors: string[]) => {
    rep({
      code: 400,
      msg: errors.join("\n")
    });
  })) {
    return;
  }
  ctx.cache.zrevrange(order_key, start, limit, function (err, result) {
    if (err) {
      rep({ code: 500, msg: err.message });
    } else {
      ctx.cache.zcount("plan-orders", "-inf", "+inf", function (err1, result1) {
        if (err1) {
          log.info("zcount planorders err" + err1);
        } else {
          let multi = ctx.cache.multi();
          for (let id of result) {
            multi.hget(order_entities, id);
          }
          multi.exec((err, result2) => {
            if (err) {
              rep({ code: 500, msg: err.message });
            } else {
              rep({ code: 200, data: result2.map(e => JSON.parse(e)), length: result1 });
            }
          });
        }
      });
    }
  });
});
// 获取订单详情
svc.call("getOrder", permissions, (ctx: Context, rep: ResponseFunction, order_id: string) => {
  // http://redis.io/commands/smembers
  log.info("getorder");
  if (!verify([uuidVerifier("order_id", order_id)], (errors: string[]) => {
    rep({
      code: 400,
      msg: errors.join("\n")
    });
  })) {
    return;
  }
  ctx.cache.hget(order_entities, order_id, function (err, result) {
    if (err) {
      rep({ code: 500, msg: err });
    } else {
      let nowDate = (new Date()).getTime();
      rep({ code: 200, data: JSON.parse(result), nowDate: nowDate });
    }
  });
});
// 获取订单列表
svc.call("getOrders", permissions, (ctx: Context, rep: ResponseFunction, offset: number, limit: number) => {
  // http://redis.io/commands/smembers
  log.info("getorders");
  if (!verify([numberVerifier("offset", offset), numberVerifier("limit", limit)], (errors: string[]) => {
    rep({
      code: 400,
      msg: errors.join("\n")
    });
  })) {
    return;
  }
  ctx.cache.zrange(orders + ctx.uid, offset, limit, function (err, result) {
    // log.info(result);
    if (err) {
      log.info("get redis error in getorders");
      log.info(err);
      rep({ code: 500, msg: err.message });
    }
    else if (result !== null && result !== "") {
      // log.info("[" + result + "]---------");
      let multi = ctx.cache.multi();
      for (let order_key of result) {
        multi.hget(order_entities, order_key);
      }
      multi.exec((err2, replies) => {
        if (err2) {
          log.error(err2, "query error");
        } else {
          // log.info("replies==========" + replies);
          let nowDate = (new Date()).getTime();
          rep({ code: 200, data: replies.map(e => JSON.parse(e)), nowDate: nowDate });
        }
      });
    }
    else {
      rep({ code: 404, msg: "not found" });
    }
  });
});
// 查看订单状态
svc.call("getOrderState", permissions, (ctx: Context, rep: ResponseFunction, vid: string, qid: string) => {
  log.info("getorderstate");
  if (!verify([uuidVerifier("vid", vid), uuidVerifier("qid", qid)], (errors: string[]) => {
    rep({
      code: 400,
      msg: errors.join("\n")
    });
  })) {
    return;
  }
  ctx.cache.hget(order_vid + vid, qid, function (err, result) {
    log.info("===========" + result);
    if (err) {
      rep({ code: 500, msg: err.message });
    } else if (result) {
      ctx.cache.hget(order_entities, result, function (err1, result1) {
        if (err1) {
          rep({ code: 500, msg: err1.message });
        } else if (result1) {
          rep({ code: 200, data: JSON.parse(result1) });
        } else {
          rep({ code: 404, msg: "Order not found" });
        }
      });
    } else {
      rep({ code: 404, msg: "Order not found" });
    }
  });
});

// 获取驾驶人信息
svc.call("getDriverOrders", permissions, (ctx: Context, rep: ResponseFunction, vid: string) => {
  log.info("getdriverorders");
  if (!verify([uuidVerifier("vid", vid)], (errors: string[]) => {
    rep({
      code: 400,
      msg: errors.join("\n")
    });
  })) {
    return;
  }
  ctx.cache.hget(driver_entities, vid, function (err, result) {
    if (err) {
      log.info("get redis error in getDriverOrders");
      rep({ code: 500, msg: err.message });
    } else if (result == null) {
      rep({ code: 404, msg: "not found" });
    } else {
      rep({ code: 200, data: JSON.parse(result) });
    }
  });
});

// 下计划单
svc.call("placeAnPlanOrder", permissions, (ctx: Context, rep: ResponseFunction, vid: string, plans: any, qid: string, pmid: string, promotion: number, service_ratio: number, summary: number, payment: number, v_value: number, expect_at: any) => {
  if (!verify([uuidVerifier("vid", vid), uuidVerifier("qid", qid), numberVerifier("promotion", promotion), numberVerifier("service_ratio", service_ratio), numberVerifier("summary", summary), numberVerifier("payment", payment), numberVerifier("v_value", v_value)], (errors: string[]) => {
    rep({
      code: 400,
      msg: errors.join("\n")
    });
  })) {
    return;
  }
  let uid = ctx.uid;
  let order_id = uuid.v1();
  let callback = order_id;
  let domain = ctx.domain;
  let args = [domain, uid, order_id, vid, plans, qid, pmid, promotion, service_ratio, summary, payment, v_value, expect_at, callback];
  log.info("placeplanorder %j", args);
  ctx.msgqueue.send(msgpack.encode({ cmd: "placeAnPlanOrder", args: args }));
  wait_for_response(ctx.cache, callback, rep);
});
// 下司机单
svc.call("placeAnDriverOrder", permissions, (ctx: Context, rep: ResponseFunction, vid: string, dids: any, summary: number, payment: number) => {
  log.info("getDetail %j", dids);
  if (!verify([uuidVerifier("vid", vid), numberVerifier("summary", summary), numberVerifier("payment", payment)], (errors: string[]) => {
    rep({
      code: 400,
      msg: errors.join("\n")
    });
  })) {
    return;
  }
  let callback = uuid.v1();
  let uid = ctx.uid;
  let domain = ctx.domain;
  let args = [domain, uid, vid, dids, summary, payment, callback];
  ctx.msgqueue.send(msgpack.encode({ cmd: "placeAnDriverOrder", args: args }));
  wait_for_response(ctx.cache, callback, rep);
});

// 更新订单状态
svc.call("updateOrderState", permissions, (ctx: Context, rep: ResponseFunction, uid: string, order_no: string, state_code: number, state: string) => {
  if (!verify([uuidVerifier("uid", uid), stringVerifier("order_no", order_no), numberVerifier("state_code", state_code), stringVerifier("state", state)], (errors: string[]) => {
    rep({
      code: 400,
      msg: errors.join("\n")
    });
  })) {
    return;
  }
  ctx.cache.hget(orderNo_id, order_no, function (err, result) {
    if (err) {
      rep({ code: 500, msg: err.message });
    } else if (result) {
      let order_id = result;
      ctx.cache.hget(orderid_vid, order_id, function (err1, result1) {
        if (err1) {
          log.info("get redis error in get orderid_vid" + err1);
          rep({ code: 500, msg: err1.message });
        } else if (result1) {
          let callback = uuid.v1();
          let vid = result1;
          let domain = ctx.domain;
          let args = [domain, uid, vid, order_id, state_code, state, callback];
          log.info("updateOrderState", args);
          ctx.msgqueue.send(msgpack.encode({ cmd: "updateOrderState", args: args }));
          wait_for_response(ctx.cache, callback, rep);
        } else {
          rep({ code: 404, msg: "Order not found" });
        }
      });
    } else {
      rep({ code: 404, msg: "Order no not found" });
    }
  });
});

// 更新订单编号
svc.call("updateOrderNo", permissions, (ctx: Context, rep: ResponseFunction, order_no: string) => {
  log.info(" updateOrderNo", order_no);
  if (!verify([stringVerifier("order_no", order_no)], (errors: string[]) => {
    rep({
      code: 400,
      msg: errors.join("\n")
    });
  })) {
    return;
  }
  ctx.cache.incr("order-no", (err, strNo) => {
    if (err) {
      log.info(err + "cache incr err");
      rep({ code: 500, msg: err.message });
    } else if (strNo) {
      let new_no = order_no.substring(0, 14);
      let strno = String(strNo);
      let no: string = formatNum(strno, 7);
      let new_order_no = new_no + no;
      const callback = uuid.v1();
      let args = [order_no, new_order_no, callback];
      ctx.msgqueue.send(msgpack.encode({ cmd: "updateOrderNo", args: args }));
      wait_for_response(ctx.cache, callback, rep);
    } else {
      rep({ code: 404, msg: "Order no not found" });
    }
  });
});



// 通过vid获取已生效计划单
svc.call("getPlanOrderByVehicle", permissions, (ctx: Context, rep: ResponseFunction, vid: string) => {
  log.info("getPlanOrderByVehicle");
  if (!verify([uuidVerifier("vid", vid)], (errors: string[]) => {
    rep({
      code: 400,
      msg: errors.join("\n")
    });
  })) {
    return;
  }
  ctx.cache.hget(vid_poid, vid, function (err, result) {
    if (err) {
      log.info("get redis error in getPlanOrderByVehicle");
      log.info(err);
      rep({ code: 500, msg: err.message });
    } else if (result === null) {
      log.info("not found planorder for this vid");
      rep({ code: 404, msg: "Order not found" });
    } else {
      ctx.cache.hget(order_entities, result, function (err1, result1) {
        if (err1) {
          log.info("get Redis in get order_entities ERROR" + err1);
          rep({ code: 500, msg: err1.message });
        } else if (result1) {
          log.info("replies==========" + result1);
          rep({ code: 200, data: JSON.parse(result1) });
        } else {
          rep({ code: 404, msg: "Order not found" });
        }
      });
    }
  });
});

// 通过vid获取司机单
svc.call("getDriverOrderByVehicle", permissions, (ctx: Context, rep: ResponseFunction, vid: string) => {
  log.info("getPlanOrderByVehicle");
  if (!verify([uuidVerifier("vid", vid)], (errors: string[]) => {
    rep({
      code: 400,
      msg: errors.join("\n")
    });
  })) {
    return;
  }
  ctx.cache.hget(vid_doid, vid, function (err, result) {
    if (err) {
      log.info("get redis error in getPlanOrderByVehicle");
      log.info(err);
      rep({ code: 500, msg: err.message });
    } else if (result === null) {
      log.info("not found planorder for this vid");
      rep({ code: 404, msg: "not found" });
    } else {
      log.info("replies==========" + result);
      rep({ code: 200, data: result.map(e => JSON.parse(e)) });
      // rep(JSON.parse(result));
    }
  });
});


// 下第三方订单
svc.call("placeAnSaleOrder", permissions, (ctx: Context, rep: ResponseFunction, vid: string, pid: string, qid: string, items: any, summary: number, payment: number) => {
  log.info("placeAnSaleOrder vid: %s, pid: %s, qid: %s, summary: %d, payment: %d", vid, pid, qid, summary, payment);
  if (!verify([uuidVerifier("vid", vid), uuidVerifier("qid", qid)], (errors: string[]) => {
    log.info("arg not match" + errors);
    rep({
      code: 400,
      msg: errors.join("\n")
    });
  })) {
    return;
  }
  let uid = ctx.uid;
  let order_id = uuid.v1();
  let callback = order_id;
  let domain = ctx.domain;
  let args = [uid, domain, order_id, vid, pid, qid, items, summary, payment, callback];
  ctx.msgqueue.send(msgpack.encode({ cmd: "placeAnSaleOrder", args: args }));
  wait_for_response(ctx.cache, callback, rep);
});

// 修改第三方订单
svc.call("updateSaleOrder", permissions, (ctx: Context, rep: ResponseFunction, order_id: string, items: any, summary: number, payment: number) => {
  log.info("updateSaleOrder %j", ctx);
  if (!verify([uuidVerifier("order_id", order_id)], (errors: string[]) => {
    rep({
      code: 400,
      msg: errors.join("\n")
    });
  })) {
    return;
  }
  let domain = ctx.domain;
  const callback = uuid.v1();
  let args = [domain, order_id, items, summary, payment, callback];
  ctx.msgqueue.send(msgpack.encode({ cmd: "updateSaleOrder", args: args }));
  wait_for_response(ctx.cache, callback, rep);
});

// 根据vid获取第三方保险
svc.call("getSaleOrder", permissions, (ctx: Context, rep: ResponseFunction, vid: string) => {
  log.info("getorderstate");
  if (!verify([uuidVerifier("vid", vid)], (errors: string[]) => {
    rep({
      code: 400,
      msg: errors.join("\n")
    });
  })) {
    return;
  }
  ctx.cache.hget("vehicle-order", vid, function (err, result) {
    log.info("===========" + result);
    if (err) {
      log.info(err);
      rep({ code: 500, msg: err.message });
    } else if (result) {
      ctx.cache.hget(order_entities, result, function (err1, result1) {
        if (err1 || result1 == null) {
          log.info(err1 + "get order_entities err in getOrderState");
          rep({ code: 500, msg: err1.message });
        } else if (result1) {
          rep({ code: 200, data: JSON.parse(result1) });
        } else {
          rep({ code: 404, msg: "Order not found" });
        }
      });
    } else {
      rep({ code: 404, msg: "Order not found" });
    }
  });
});

// 生成核保
svc.call("createUnderwrite", permissions, (ctx: Context, rep: ResponseFunction, oid: string, plan_time: any, validate_place: string) => {
  log.info("createUnderwrite uuid is " + ctx.uid);
  if (!verify([uuidVerifier("oid", oid), stringVerifier("validate_place", validate_place)], (errors: string[]) => {
    rep({
      code: 400,
      msg: errors.join("\n")
    });
  })) {
    return;
  }
  let validate_update_time = new Date();
  let uwid = uuid.v1();
  let callback = uuid.v1();
  let domain = ctx.domain;
  let args = [uwid, oid, plan_time, validate_place, validate_update_time, callback, domain];
  log.info("args: " + args);
  ctx.msgqueue.send(msgpack.encode({ cmd: "createUnderwrite", args: args }));
  wait_for_response(ctx.cache, callback, rep);
});

// 修改预约验车地点
svc.call("alterValidatePlace", permissions, (ctx: Context, rep: ResponseFunction, uwid: string, plan_time: any, validate_place: string) => {
  log.info("alterValidatePlace uuid is " + ctx.uid);
  if (!verify([uuidVerifier("uwid", uwid), stringVerifier("validate_place", validate_place)], (errors: string[]) => {
    rep({
      code: 400,
      msg: errors.join("\n")
    });
  })) {
    return;
  }
  let validate_update_time = new Date();
  let callback = uuid.v1();
  let args = [uwid, plan_time, validate_place, validate_update_time, callback];
  log.info("args: " + args);
  ctx.msgqueue.send(msgpack.encode({ cmd: "alterValidatePlace", args: args }));
  wait_for_response(ctx.cache, callback, rep);
});

// 工作人员填充验车信息
svc.call("fillUnderwrite", permissions, (ctx: Context, rep: ResponseFunction, uwid: string, real_place: string, opid: string, certificate_state: number, problem_type: any, problem_description: string, note: string, photos: any) => {
  log.info("fillUnderwrite uuid is " + ctx.uid);
  if (!verify([uuidVerifier("uwid", uwid), uuidVerifier("opid", opid), stringVerifier("real_place", real_place), stringVerifier("real_place", real_place), numberVerifier("certificate_state", certificate_state)], (errors: string[]) => {
    rep({
      code: 400,
      msg: errors.join("\n")
    });
  })) {
    return;
  }
  let update_time = new Date();
  let callback = uuid.v1();
  let domain = ctx.domain;
  // let uwid = "b2288950-8849-11e6-86a9-8d3457e084f0";
  // let real_place = "北京市东城区东直门东方银座23d";
  // let opid = "bcd9fcc0-882c-11e6-b850-8774c85fe33c";
  // let certificate_state = 0;
  // let problem_type = ["剐蹭", "调漆"];
  // let problem_description = "追尾。。。。";
  // let note = "你问我撞不撞，我当然要撞了.";
  // let photos = [
  //   "http://pic.58pic.com/58pic/13/19/86/55m58PICf9t_1024.jpg",
  //   "http://pic.58pic.com/58pic/13/19/86/55m58PICf9t_1024.jpg",
  //   "http://pic.58pic.com/58pic/13/19/86/55m58PICf9t_1024.jpg",
  //   "http://pic.58pic.com/58pic/13/19/86/55m58PICf9t_1024.jpg",
  //   "http://pic.58pic.com/58pic/13/19/86/55m58PICf9t_1024.jpg",
  //   "http://pic.58pic.com/58pic/13/19/86/55m58PICf9t_1024.jpg"
  // ];

  let args = [domain, uwid, real_place, update_time, opid, certificate_state, problem_type, problem_description, note, photos, callback];
  log.info("args: " + args);
  ctx.msgqueue.send(msgpack.encode({ cmd: "fillUnderwrite", args: args }));
  wait_for_response(ctx.cache, callback, rep);
});

// 提交审核结果
svc.call("submitUnderwriteResult", permissions, (ctx: Context, rep: ResponseFunction, uwid: string, underwrite_result: string) => {
  log.info("submitUnderwriteResult uuid is " + ctx.uid);
  if (!verify([uuidVerifier("uwid", uwid), stringVerifier("underwrite_result", underwrite_result)], (errors: string[]) => {
    rep({
      code: 400,
      msg: errors.join("\n")
    });
  })) {
    return;
  }
  let update_time = new Date();
  let callback = uuid.v1();
  let domain = ctx.domain;
  ctx.msgqueue.send(msgpack.encode({ cmd: "submitUnderwriteResult", args: [domain, uwid, underwrite_result, update_time, callback] }));
  wait_for_response(ctx.cache, callback, rep);
});

// 修改审核结果
svc.call("alterUnderwriteResult", permissions, (ctx: Context, rep: ResponseFunction, uwid: string, underwrite_result: string) => {
  log.info("alterUnderwriteResult uuid is " + ctx.uid);
  if (!verify([uuidVerifier("uwid", uwid), stringVerifier("underwrite_result", underwrite_result)], (errors: string[]) => {
    rep({
      code: 400,
      msg: errors.join("\n")
    });
  })) {
    return;
  }
  let update_time = new Date();
  let callback = uuid.v1();
  let domain = ctx.domain;
  ctx.msgqueue.send(msgpack.encode({ cmd: "submitUnderwriteResult", args: [domain, uwid, underwrite_result, update_time, callback] }));
  wait_for_response(ctx.cache, callback, rep);
});

// 修改实际验车地点
svc.call("alterRealPlace", permissions, (ctx: Context, rep: ResponseFunction, uwid: string, real_place: string) => {
  log.info("alterRealPlace uuid is " + ctx.uid);
  if (!verify([uuidVerifier("uwid", uwid), stringVerifier("real_place", real_place)], (errors: string[]) => {
    rep({
      code: 400,
      msg: errors.join("\n")
    });
  })) {
    return;
  }
  let update_time = new Date();
  let callback = uuid.v1();
  let args = [uwid, real_place, update_time, callback];
  log.info("args: " + args);
  ctx.msgqueue.send(msgpack.encode({ cmd: "alterRealPlace", args: args }));
  wait_for_response(ctx.cache, callback, rep);
});

// 修改备注
svc.call("alterNote", permissions, (ctx: Context, rep: ResponseFunction, uwid: string, note: string) => {
  log.info("alterNote uuid is " + ctx.uid);
  if (!verify([uuidVerifier("uwid", uwid), stringVerifier("note", note)], (errors: string[]) => {
    rep({
      code: 400,
      msg: errors.join("\n")
    });
  })) {
    return;
  }
  let update_time = new Date();
  let callback = uuid.v1();
  ctx.msgqueue.send(msgpack.encode({ cmd: "alterNote", args: [uwid, note, update_time, callback] }));
  wait_for_response(ctx.cache, callback, rep);
});

// 上传现场图片
svc.call("uploadPhotos", permissions, (ctx: Context, rep: ResponseFunction, uwid: string, photo: string) => {
  log.info("uploadPhotos uuid is " + ctx.uid);
  if (!verify([uuidVerifier("uwid", uwid), stringVerifier("photo", photo)], (errors: string[]) => {
    rep({
      code: 400,
      msg: errors.join("\n")
    });
  })) {
    return;
  }
  let update_time = new Date();
  let callback = uuid.v1();
  let args = [uwid, photo, update_time, callback];
  log.info("args: " + args);
  ctx.msgqueue.send(msgpack.encode({ cmd: "uploadPhotos", args: args }));
  wait_for_response(ctx.cache, callback, rep);
});

// 根据订单编号得到核保信息
svc.call("getUnderwriteByOrderNumber", permissions, (ctx: Context, rep: ResponseFunction, oid: string) => {
  log.info("getUnderwriteByOrderNumber uuid is " + ctx.uid);
  if (!verify([stringVerifier("oid", oid)], (errors: string[]) => {
    rep({
      code: 400,
      msg: errors.join("\n")
    });
  })) {
    return;
  }
  ctx.cache.hget("orderNo-id", oid, function (err, result) {
    if (result) {
      let orderid = result;
      ctx.cache.zrange("underwrite", 0, -1, function (err3, result3) {
        if (result3) {
          let multi = ctx.cache.multi();
          for (let uwid of result3) {
            multi.hget("underwrite-entities", uwid);
          }
          multi.exec((err4, result4) => {
            if (result4) {
              let underWrite: any;
              result4.map(underwrite => {
                if (JSON.parse(underwrite).order_id === orderid) {
                  log.info("underwrite" + JSON.parse(underwrite));
                  underWrite = JSON.parse(underwrite);
                }
              });
              if (underWrite != null) {
                rep({ code: 200, underWrite: underWrite });
              } else {
                rep({ code: 404, msg: "Not Found underwrite" });
              }
            } else if (err4) {
              rep({ code: 500, msg: err4 });
            } else {
              rep({ code: 404, msg: "Not Found underwrite-entities" });
            }
          });
        } else if (err3) {
          rep({ code: 500, msg: err3 });
        } else {
          rep({ code: 404, msg: "Not Found underwrites" });
        }
      });
    } else {
      rep({ code: 404, msg: "Cannot match orderid" });
    }
  });
});

// 根据订单号得到核保信息
svc.call("getUnderwriteByOrderId", permissions, (ctx: Context, rep: ResponseFunction, order_id: string) => {
  log.info("getUnderwriteByOrderId uuid is " + ctx.uid);
  if (!verify([uuidVerifier("order_id", order_id)], (errors: string[]) => {
    rep({
      code: 400,
      msg: errors.join("\n")
    });
  })) {
    return;
  }
  ctx.cache.zrange("underwrite", 0, -1, function (err, result) {
    if (result) {
      let multi = ctx.cache.multi();
      for (let uwid of result) {
        multi.hget("underwrite-entities", uwid);
      }
      multi.exec((err2, result2) => {
        if (result2) {
          let uwinfo: any;
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
            rep({ code: 200, data: uwinfo });
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

// 根据核保号得到核保信息
svc.call("getUnderwriteByUWId", permissions, (ctx: Context, rep: ResponseFunction, uwid: string) => {
  log.info("getUnderwriteByUWId uuid is " + ctx.uid + "uwid is " + uwid);
  if (!verify([uuidVerifier("uwid", uwid)], (errors: string[]) => {
    rep({
      code: 400,
      msg: errors.join("\n")
    });
  })) {
    return;
  }
  let order_info: any;
  ctx.cache.hget("underwrite-entities", uwid, function (err, result) {
    if (result) {
      rep({ code: 200, data: JSON.parse(result) });
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

svc.call("refresh", permissions, (ctx: Context, rep: ResponseFunction) => {
  log.info("refresh");
  ctx.msgqueue.send(msgpack.encode({ cmd: "refresh", args: [ctx.domain] }));
  rep({
    code: 200,
    msg: "Okay"
  });
});

// 判断一个VIN码是否有订单 ValidOrder
svc.call("ValidOrder", permissions, (ctx: Context, rep: ResponseFunction, VIN) => {
  log.info("ValidOrder");
  ctx.cache.hget("VIN-orderID", VIN, function (err, result) {
    if (err) {
      log.info(err);
      rep({
        code: 500,
        msg: err.message
      });
    } else if (result) {
      ctx.cache.hget("order-entities", result, (err2, result2) => {
        if (err2) {
          log.info(err2);
          rep({
            code: 500,
            msg: err2.message
          });
        } else if (result2) {
          let order = JSON.parse(result2);
          let user_id = order["vehicle"]["user_id"];
          if (ctx.uid === user_id) {
            let state_code = order["state_code"];
            let state = order["state"];
            switch (state_code) {
              case 1:
                state = "该车有尚未支付订单，请处理后再获取报价。";
                break;
              case 2:
                state = "该车有计划待生效订单，若要重新获取报价，请取消该订单。";
                break;
              case 3:
                state = "该车尚在核保，暂时不可以报价。";
                break;
              case 4:
                state = "该车存在生效订单，暂时不可以报价。";
                break;
              default:
                state = "该车存在失效订单，可以继续报价。";
            }
            if (state_code === 4) {
              let date = new Date();
              if ((order["stop_at"].getTime() - date.getTime()) < 7776000000) {
                state_code = 8;
                state = "该车距计划到期时间超过3个月，请在到期前3个月内获取报价。";
              }
            }
            rep({
              code: 200,
              data: { state: state_code, msg: state }
            });
          } else {
            rep({
              code: 200,
              data: { state: 10, msg: "该车已通过其他微信号生成订单,如非本人操作，请及时联系客服。" }
            });
          }
        } else {
          rep({
            code: 200,
            data: { state: 0, msg: "该车不存在订单，可以继续报价。" }
          });
        }
      });
    } else {
      rep({
        code: 200,
        data: { state: 0, msg: "该车不存在订单，可以继续报价。" }
      });
    }
  });
});

log.info("Start service at " + config.svraddr);

svc.run();
