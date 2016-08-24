import { Processor, Config, ModuleFunction, DoneFunction } from 'hive-processor';
import { Client as PGClient, ResultSet } from 'pg';
import { createClient, RedisClient} from 'redis';

let config: Config = {
  dbhost: process.env['DB_HOST'],
  dbuser: process.env['DB_USER'],
  database: process.env['DB_NAME'],
  dbpasswd: process.env['DB_PASSWORD'],
  cachehost: process.env['CACHE_HOST'],
  addr: "ipc:///tmp/queue.ipc"
};

let processor = new Processor(config);

processor.call('placeAnOrder', (db: PGClient, cache: RedisClient, done: DoneFunction,id:string, pid:string,veid:string,orid:string,moid:string,licencse_no:string,
vin:string, engine_no:string, register_date:string, average_mileage:string, fuel_type:string, receipt_no:string, receipt_date:string, last_insurance_company:string,
 vehicle_license_frontal_view:string, vehicle_license_rear_view:string, did:string, name:string, gender:string,
 identity_no:string, phone:string, identity_frontal_view:string, identity_rear_view:string, service_ratio:string, price:string, actual_price:string) => {
  db.query('INSERT INTO drivers(did, orid, name, gender, identity_no, phone, identity_frontal_view, identity_rear_view) VALUE($1,$2,$3,$4,$5,$6,$7,$8)',
    [did, orid, name, gender, identity_no, phone, identity_frontal_view, identity_rear_view],(err:Error,result:ResultSet) =>{
    if (err) {
      console.error('query error',err.message,err.stack);
      done();
      return;
    }
    db.query('INSERT INTO vehicles(veid, orid, moid, licencse_no, vin, engine_no,register_date, average_mileage, fuel_type,receipt_no,receipt_date,last_insurance_company,\
    vehicle_license_frontal_view,vehicle_license_rear_view) VALUE($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)',
    [veid,orid,moid,licencse_no,vin,engine_no,register_date,average_mileage,fuel_type,receipt_no,receipt_date,last_insurance_company,
    vehicle_license_frontal_view,vehicle_license_rear_view],(err1:Error,result1:ResultSet) =>{
      if (err1) {
       console.error('query error',err.message,err.stack);
       done();
       return;
      }
    db.query('INSERT INTO orders(id, pid, service_ratio, price, actual_price) VALUE($1,$2,$3,$4,$5)',[id, pid, service_ratio, price,actual_price],(err2:Error,result2:ResultSet)=>{
      if (err2) {
        console.error('query error',err.message,err.stack);
        done();
        return;
      }
    let order = [id, pid, licencse_no, vin, engine_no, register_date, average_mileage, fuel_type, receipt_no,
          receipt_date, last_insurance_company, vehicle_license_frontal_view, vehicle_license_rear_view, did, name, gender, identity_no, phone,
          identity_frontal_view, identity_rear_view, service_ratio, price, actual_price];
         let multi = cache.multi();
            multi.hset("order-entity", id, JSON.stringify(order));
            multi.sadd("order", id);
          multi.exec((err, replies) => {
            if (err) {
              console.error(err);
            }
            done(); // close db and cache connection
          });
     });
   });
  });
});

// function row2order(row) {
//   return {
//     id: row.id,
//     pid: row.pid,
//     vehicles:[],
//     drivers:[],
//     service_ratio: row.service_ratio,
//     price: row.price,
//     actual_price: row.actual_price
//   };
// }

// function row2driver(row) {
//   return {
//     id: row.id,
//     name: row.name,
//     gender: row.gender,
//     identity_no: row.identity_no,
//     phone: row.phone,
//     identity_frontal_view: row.identity_frontal_view? row.identity_frontal_view.trim():'',
//     identity_rear_view: row.identity_rear_view? row.identity_rear_view.trim():'',
//   };
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
