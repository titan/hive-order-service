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
  fun: 'placeAnOrder',
  args:{}
};

req.send(msgpack.encode(params));
req.on('data', function (msg) {
  console.log(msgpack.decode(msg));
  req.shutdown(addr);
});
