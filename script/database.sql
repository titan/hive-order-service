
-- 钱包模块
CREATE TABLE wallets(id uuid PRIMARY KEY,balance numeric NOT NULL);
CREATE TABLE accounts(
  id uuid PRIMARY KEY,
  wid uuid NOT NULL REFERENCES wallets ON DELETE CASCADE,
  type boolean NOT NULL,
  vid uuid REFERENCES vehicles NOT NULL,
  balance0 numeric NOT NULL,
  balance1 numeric NOT NULL
);
CREATE TABLE transations(
  id uuid PRIMARY KEY,
  aid uuid NOT NULL REFERENCES accounts ON DELETE CASCADE,
  type integer NOT NULL,
  title char(100) NOT NULL,
  occurred_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  amount integer NOT NULL   
);
-- 互助模块
CREATE TABLE mutual_aids(
  id uuid PRIMARY KEY,
  no char(40) NOT NULL,
  pid uuid NOT NULL REFERENCES plans,
  uid uuid NOT NULL REFERENCES users ON DELETE CASCADE,
  driver_id uuid NOT NULL, 
  vin char(30) NOT NULL,
  city char(40) NOT NULL,
  district char(40) NOT NULL,
  street char(100) NOT NULL,
  phone char(20) NOT NULL,
  occurred_at timestamp NOT NULL,
  responsibility char(20) NOT NULL,
  situation text NOT NULL,
  description text NOT NULL,
  scene_view char(1024) NOT NULL,
  vehicle_damaged_view char(1024) NOT NULL,
  vehicle_frontal_view char(1024) NOT NULL,
  driver_view char(1024) NOT NULL,
  driver_license_view char(1024) NOT NULL,
  state boolean NOT NULL
);
CREATE TABLE recompense(
  id uuid PRIMARY KEY,
  mid uuid NOT NULL REFERENCES mutual_aids ON DELETE CASCADE,
  personal_fee char(20) NOT NULL,
  personal_balance numeric NOT NULL,
  small_hive_fee char(20) NOT NULL,
  small_hive_balance numeric NOT NULL,
  big_hive_fee char(20) NOT NULL,
  big_hive_balance numeric NOT NULL,
  paid_at timestamp NOT NULL
);
-- 订单模块
CREATE TABLE driver_order(
  id uuid PRIMARY KEY,
  vid uuid NOT NULL REFERENCES vehicles ON DELETE CASCADE,
  summary numeric DEFAULT 0.0,
  payment numeric NOT NULL,
  created_at timestamp DEFAULT CURRENT_TIMESTAMP,
  updated_at timestamp DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE sale_order(
  id uuid PRIMARY KEY,
  vid uuid NOT NULL REFERENCES vehicles ON DELETE CASCADE,
  pid uuid NOT NULL REFERENCES plans,
  start_at timestamp DEFAULT CURRENT_TIMESTAMP,
  stop_at timestamp DEFAULT CURRENT_TIMESTAMP,
  created_at timestamp DEFAULT CURRENT_TIMESTAMP,
  updated_at timestamp DEFAULT CURRENT_TIMESTAMP,
);
CREATE TABLE plan_order(
  id uuid PRIMARY KEY,
  vid uuid NOT NULL REFERENCES vehicles ON DELETE CASCADE,
  pmid uuid  REFERENCES promotions,
  service_ratio char(5) NOT NULL,
  summary numeric DEFAULT 0.0,
  payment numeric NOT NULL,
  expect_at timestamp DEFAULT CURRENT_TIMESTAMP,
  start_at timestamp DEFAULT CURRENT_TIMESTAMP,
  stop_at timestamp DEFAULT CURRENT_TIMESTAMP,
  created_at timestamp DEFAULT CURRENT_TIMESTAMP,
  updated_at timestamp DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE order_drivers(
  id1 uuid PRIMARY KEY,
  pid uuid NOT NULL REFERENCES person ON DELETE CASCADE
);
CREATE TABLE order_item(
  id uuid PRIMARY KEY,
  piid uuid NOT NULL REFERENCES plan_items ON DELETE CASCADE,
  pid uuid NOT NULL,
  price numeric NOT NULL
);