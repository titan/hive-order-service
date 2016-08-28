import { Processor, Config, ModuleFunction, DoneFunction,rpc, } from 'hive-processor';
import { Client as PGClient, ResultSet } from 'pg';
import { createClient, RedisClient} from 'redis';
import * as bunyan from 'bunyan';
import * as uuid from 'uuid';

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

processor.call('placeAnPlanOrder', (db: PGClient, cache: RedisClient, done: DoneFunction, args1) => {
  log.info('placeAnOrder');
  let id = uuid.v1();
  let item_id = uuid.v1();
  db.query('INSERT INTO plan_order(id, vid, pmid, service_ratio, summary, payment) VALUES($1,$2,$3,$4,$5)',
    [id, args1.vid, args1.pmid, args1.service_ratio, args1.summary, args1.payment], (err: Error, result: ResultSet) => {
      if (err) {
        log.error(err, 'query error');
        done();
        return;
      }
      db.query('INSERT INTO order_item(item_id, pid, piid, price) VALUES($1,$2,$3,$4)',
        [item_id, args1.vid, args1.plans.pid, args1.plans.pid.piid, args1.plans.pid.price], (err: Error, result: ResultSet) => {
          if (err) {
            log.error(err, 'query error');
            done();
            return;
          }
          let planorder = [args1.vid, args1.plans, args1.pmid, args1.service_ratio, args1.summary, args1.payment]
          let multi = cache.multi();
          multi.hset("plan_order-entity", id, JSON.stringify(planorder));
          multi.sadd("plan_orders_key", id);
          multi.exec((err3, replies) => {
            if (err3) {
              log.error(err3, 'query error');
            }
            done(); // close db and cache connection
          });
        });
    });
});

processor.call('placeAnDriverOrder', (db: PGClient, cache: RedisClient, done: DoneFunction, args2) => {
  log.info('placeAnOrder');
  let id = uuid.v1();
  let drivers_id = uuid.v1();
  db.query('INSERT INTO driver_order(id, vid, summary, payment) VALUES($1,$2,$3,$4)',
    [id, args2.vid, args2.summary, args2.pay], (err: Error, result: ResultSet) => {
      if (err) {
        log.error(err, 'query error');
        done();
        return;
      }
      db.query('INSERT INTO order_drivers(drivers_id, pid) VALUES($1,$2)',
        [drivers_id, args2.dids], (err: Error, result: ResultSet) => {
          if (err) {
            log.error(err, 'query error');
            done();
            return;
          }
          // let p = rpc(args2.domain, 'tcp://vehicle:4040', args2.uid, 'getVehicleInfos', args2.vin, 0, -1);
          // p.then((drivers) => {
          // let driver_id = drivers.id;
          // let driver_name = drivers.name; 
          let driver_order = [args2.vid,args2.dids,args2.summary,args2.payment]; 
          let multi = cache.multi();
          multi.hset("driver_order_entity", id, JSON.stringify(driver_order));
          multi.sadd("driver_order_key", id);
          multi.exec((err3, replies) => {
            if (err3) {
              log.error(err3, 'query error');
            }
            done(); // close db and cache connection
          });
          // });
        });
    });
});
processor.call('placeAnSaleOrder', (db: PGClient, cache: RedisClient, done: DoneFunction, args3) => {
  log.info('placeAnOrder');
  let id = uuid.v1();
  let item_id = uuid.v1();
  db.query('INSERT INTO sale_order(id, vid, pid) VALUES($1,$2,$3)',
    [id, args3.vid, args3.pid,], (err: Error, result: ResultSet) => {
      if (err) {
        log.error(err, 'query error');
        done();
        return;
      }
      db.query('INSERT INTO order_items(item_id, pid,piid,price) VALUES($1,$2,$3,$4)',
        [item_id, args3.items, args3.items.piid, args3.items.price], (err: Error, result: ResultSet) => {
          if (err) {
            log.error(err, 'query error');
            done();
            return;
          }
          let sale_order = [args3.vid,args3.items,args3.summary,args3.payment];
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

  processor.run();

  console.log('Start processor at ' + config.addr);
