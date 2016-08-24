import { Service, Config, Context, ResponseFunction, Permission } from 'hive-service';
import * as Redis from "redis";
import * as nanomsg from 'nanomsg';
import * as msgpack from 'msgpack-lite';

let uuid = require('node-uuid'); 
let redis = Redis.createClient(6379, "redis"); // port, host

let order_entity = "order-";
let order_key = "order";

let config: Config = {
  svraddr: 'tcp://0.0.0.0:4040',
  msgaddr: 'ipc:///tmp/queue.ipc'
};

let svc = new Service(config);

let permissions: Permission[] = [['mobile', true], ['admin', true]];



svc.call('getDetail', permissions, (ctx: Context, rep: ResponseFunction) => {
  // http://redis.io/commands/smembers
  redis.smembers(order_entity + ctx.uid, function (err, result) {
    if (err) {
      rep([]);
    } else {
      ids2objects(order_key, result, rep);
    }
  });
});

svc.call('addDriver', permissions, (ctx: Context, rep: ResponseFunction, id:string, pid:string,veid:string,orid:string,moid:string,licencse_no:string,
vin:string, engine_no:string, register_date:string, average_mileage:string, fuel_type:string, receipt_no:string, receipt_date:string, last_insurance_company:string,
 vehicle_license_frontal_view:string, vehicle_license_rear_view:string, did:string, name:string, gender:string,
 identity_no:string, phone:string, identity_frontal_view:string, identity_rear_view:string, service_ratio:string, price:string, actual_price:string) => {
 let args = [id, ctx.uid,pid,veid,orid,moid,licencse_no,vin,engine_no,receipt_date,average_mileage,fuel_type,receipt_no,receipt_date,last_insurance_company,
 vehicle_license_frontal_view,vehicle_license_rear_view,did,name,gender,identity_no,phone,identity_frontal_view,identity_rear_view,service_ratio,price,actual_price];
 ctx.msgqueue.send(msgpack.encode({cmd: "addDriver", args: args,id: uuid.v1}));
    rep({status: "okay"});
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
  multi.exec(function(err, replies) {
    rep(replies);
  });
}

console.log('Start service at ' + config.svraddr);

svc.run();
