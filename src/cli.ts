import * as nano from 'nanomsg';
import * as msgpack from 'msgpack-lite';

interface Context {
  domain: string,
  ip: string,
  uid: string
}

let req = nano.socket('req');

let addr = 'tcp://0.0.0.0:4040';

req.connect(addr);

let params = {
  ctx: {domain: 'mobile', ip: 'localhost', uid: ''},
  fun: 'refresh',
  args: ['licencse_no', 'vin', 'engine_no', 'receipt_date,average_mileage','fuel_type','receipt_no','receipt_date','last_insurance_company',
 'vehicle_license_frontal_view','vehicle_license_rear_view','did','name','gender','identity_no,phone','identity_frontal_view','identity_rear_view','service_ratio','price','actual_price']
};

req.send(msgpack.encode(params));
req.on('data', function (msg) {
  console.log(msgpack.decode(msg));
  req.shutdown(addr);
});
