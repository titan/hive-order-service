CREATE TABLE plans (
  id uuid PRIMARY KEY,
  title char(128) NOT NULL,
  description text NULL,
  image char(1024) NOT NULL,
  thumbnail char(1024) NOT NULL,
  period integer NOT NULL DEFAULT 0
);

CREATE TABLE plan_rules (
  id uuid PRIMARY KEY,
  pid uuid NOT NULL REFERENCES plans ON DELETE CASCADE,
  name char(128) NULL,
  title char(128) NULL,
  description text NULL
);

CREATE TABLE plan_items (
  id uuid PRIMARY KEY,
  pid uuid NOT NULL REFERENCES plans ON DELETE CASCADE,
  title char(128) NULL,
  description text NULL,
  price float NOT NULL DEFAULT 0.0
);
-- 钱包模块
CREATE TABLE wallets(id uuid PRIMARY KEY,balance numeric NOT NULL);
CREATE TABLE accounts(
  id uuid PRIMARY KEY,
  wid uuid NOT NULL REFERENCES wallet ON DELETE CASCADE,
  type boolean NOT NULL,
  vid uuid REFERENCES vehicle NOT NULL,
  balance0 numeric NOT NULL,
  balance1 numeric NOT NULL
);
CREATE TABLE transations(
  id uuid PRIMARY KEY,
  aid uuid REFERENCES accounts ON DELETE CASCADE,
  type integer NOT NULL,
  title char(100) NOT NULL,
  occurred_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  amount integer NOT NULL   
)
-- 互助模块
CREATE TABLE mutual_aids(
  id uuid NOT NULL,
  no char(40) NOT NULL,
  pid uuid NOT NULL REFERENCES plans,
  uid uuid NOT NULL REFERENCES users ON DELETE CASCADE,
  did uuid NOT NULL,
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
  state boolean NOT NULL,
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
) 
-- 订单模块
CREATE TABLE drivers(
  did uuid PRIMARY KEY,
  orid uuid NOT NULL REFERENCES orders ON DELETE CASCADE,
  mid uuid NOT NULL REFERENCES mutual_aids ON DELETE CASCADE,
  name char(40) NOT NULL,
  gender char(4) NOT NULL,
  identity_no integer(18) NOT NULL,
  phone char(20) NOT NULL,
  identity_frontal_view char(1024) NOT NULL,
  identity_rear_view char(1024) NOT NULL,
);

CREATE TABLE orders(
  id uuid PRIMARY KEY,
  service_ratio char(4) NOT NULL,
  price numeric NOT NULL,
  actual-price numeric NOT NULL
);
CREATE TABLE vehicles(
  veid uuid PRIMARY KEY,
  orid uuid NOT NULL REFERENCES orders ON DELETE CASCADE,
  moid uuid NOT NULL REFERENCES vehicle_model,
  mid  uuid NOT NULL REFERENCES mutual_aids,
  license-no char(20) NOT NULL,
  vin char(40) NOT NULL,
  engine_no char(20) NOT NULL,
  register_date timestamp NOT NULL,
  average_mileage char(20) NOT NULL,
  fuel_type char(20) NOT NULL, 
  receipt_no char(20) NOT NULL,
  receipt_date timestamp NOT NULL,
  last_insurance_company text NOT NULL,
  vehicle_license_frontal_view char(1024) NOT NULL,
  vehicle_license_rear_view char(1024) NOT NULL
);
CREATE TABLE ordertodriver(
  id integer PRIMARY KEY,
  drid uuid NOT NULL REFERENCES drivers ON DELETE CASCADE,
  orid uuid NOT NULL REFERENCES orders ON DELETE CASCADE
);
