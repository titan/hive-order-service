import { Processor, Config, ModuleFunction, DoneFunction } from 'hive-processor';
import { Client as PGClient, ResultSet } from 'pg';
import { createClient, RedisClient} from 'redis';
import * as bunyan from 'bunyan';

let log = bunyan.createLogger({
  name: 'order-processor',
  streams: [
    {
      level: 'info',
      path: '/var/log/processor-info.log',  // log ERROR and above to a file
      type: 'rotating-file',
      period: '1d',   // daily rotation
      count: 7        // keep 7 back copies
    },
    {
      level: 'error',
      path: '/var/log/processor-error.log',  // log ERROR and above to a file
      type: 'rotating-file',
      period: '1w',   // daily rotation
      count: 3        // keep 7 back copies
    }
  ]
});

let config: Config = {
  dbhost: process.env['DB_HOST'],
  dbuser: process.env['DB_USER'],
  database: process.env['DB_NAME'],
  dbpasswd: process.env['DB_PASSWORD'],
  cachehost: process.env['CACHE_HOST'],
  addr: "ipc:///tmp/queue.ipc"
};

let processor = new Processor(config);

processor.call('placeAnPlanOrder', (db: PGClient, cache: RedisClient, done: DoneFunction,args1,id,id1) => {
    log.info('placeAnOrder');
  db.query('INSERT INTO plan_order(id, vid, pmid, service_ratio, summary, payment,expect_at, start_at, stop_at, created_at,updated_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)',
    [id, args1.vid,args1.pmid, args1.service_ratio, args1.summary, args1.payment, args1.expect_at],(err:Error,result:ResultSet) =>{
    if (err) {
      log.error(err, 'query error');
      done();
      return;
    }
  db.query('INSERT INTO order_item(id1, piid, pid, price) VALUES($1,$2,$3,$4)',
    [id, args1.vid,args1.plans.piid, args1.pid, args1.plans.price],(err:Error,result:ResultSet) =>{
    if (err) {
      log.error(err, 'query error');
      done();
      return;
    }
// 三级缓存
    // let order = args;
    //      let multi = cache.multi();
    //         multi.hset("order-entity", id, JSON.stringify(args));
    //         multi.sadd("orders", id);
    //       multi.exec((err3, replies) => {
    //         if (err3) {
    //           log.error(err3, 'query error');
    //         }
    //         done(); // close db and cache connection
    //       });
     });
    });
});

processor.call('placeAnDriverOrder', (db: PGClient, cache: RedisClient, done: DoneFunction,args2,id ) => {
    log.info('placeAnOrder');
  db.query('INSERT INTO driver_order(id, vid, summary, payment) VALUES($1,$2,$3,$4)',
    [id,args2.vid,args2.summary,args2.payment],(err:Error,result:ResultSet) =>{
    if (err) {
      log.error(err, 'query error');
      done();
      return;
    }
    let driver_order = [];
         let multi = cache.multi();
            multi.hset("driver_order_entity", id, JSON.stringify(order));
            multi.sadd("driver_order_key", id);
          multi.exec((err3, replies) => {
            if (err3) {
              log.error(err3, 'query error');
            }
            done(); // close db and cache connection
          });
     });
});
processor.call('placeAnSaleOrder', (db: PGClient, cache: RedisClient, done: DoneFunction,args3,id) => {
    log.info('placeAnOrder');
  db.query('INSERT INTO sale_order(id, vid, pid,start_at,stop_at) VALUES($1,$2,$3,$4,$5)',
    [id, args3.vid, args3.pid, args3.start_at, args3.stop_at],(err:Error,result:ResultSet) =>{
    if (err) {
      log.error(err, 'query error');
      done();
      return;
    }
        let sale_order=args3;
            // sale_order.push(row2sale_order(args3));
            let multi = cache.multi();
            multi.hset("sale_order_entity", id, JSON.stringify(sale_order));
            multi.sadd("sale_order_key", id);
            multi.exec((err3, replies) => {
            if (err3) {
              log.error(err3, 'query error');
            }
            done(); // close db and cache connection
          });
     });
});
function row2driver_order(args2) {
  return {
    vid: args2.vid,
    dids:[],
    summary:args2.summary,
    payment:args2.payment
  };
}

function row2dids(args2) {
  return {
    // 新增司机数量从哪里取
     };
}
// function row2sale_order(args3) {
//   return {
//     vid: args3.vid,
//     summary:args3.summary,
//     payment:args3.payment,
//     items:[]
//   };
// }

// function row2items(args3) {
//   return {
//     items:args3.order_items
//      };
// }

// function row2vehicle(row) {
//   return {
//     veid: row.veid,
//     licencse_no: row.licencse_no,
//     model:[],
//     vin: row.vin,
//     engine_no: row.engine_no,
//     register_date: row.register_date,
//     average_mileage: row.average_mileage,
//     fuel_type: row.fuel_type,
//     receipt_no: row.receipt_no,
//     receipt_date: row.receipt_date,
//     last_insurance_company: row.last_insurance_company,
//     vehicle_license_frontal_view:row.vehicle_license_frontal_view? row.vehicle_license_frontal_view.trim():'',
//     vehicle_license_rear_view: row.vehicle_license_rear_view? row.vehicle_license_rear_view.trim():'',
//   };
// }


processor.run();

console.log('Start processor at ' + config.addr);
