-- 订单模块
CREATE TABLE driver_order(
  id uuid PRIMARY KEY,
  vid uuid NOT NULL REFERENCES vehicles ON DELETE CASCADE,
  status_code char(5) NOT NULL DEFAULT 0,
  status char(5),
  summary numeric DEFAULT 0.0,
  payment numeric NOT NULL,
  created_at timestamp DEFAULT CURRENT_TIMESTAMP,
  updated_at timestamp DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE sale_order(
  id uuid PRIMARY KEY,
  vid uuid NOT NULL REFERENCES vehicles ON DELETE CASCADE,
  pid uuid NOT NULL REFERENCES plans,
  status_code char(5) NOT NULL DEFAULT 0,
  status char(5),
  start_at timestamp DEFAULT CURRENT_TIMESTAMP,
  stop_at timestamp DEFAULT CURRENT_TIMESTAMP,
  created_at timestamp DEFAULT CURRENT_TIMESTAMP,
  updated_at timestamp DEFAULT CURRENT_TIMESTAMP,
);
CREATE TABLE plan_order(
  id uuid PRIMARY KEY,
  vid uuid NOT NULL REFERENCES vehicles ON DELETE CASCADE,
  pmid uuid  REFERENCES promotions,
  status_code char(5) NOT NULL DEFAULT 0,
  status char(5),
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
  drivers_id uuid PRIMARY KEY,
  pid uuid NOT NULL REFERENCES person ON DELETE CASCADE
);
CREATE TABLE order_item(
  item_id uuid PRIMARY KEY,
  pid uuid NOT NULL,
  piid uuid NOT NULL REFERENCES plan_items ON DELETE CASCADE,
  price numeric NOT NULL
);
CREATE TABLE order_event(
  id uuid PRIMARY KEY,
  oid uuid NOT NULL,
  uid uuid NOT NULL REFERENCES users ON DELETE CASCADE,
  data json NOT NULL,
  occurred_at timestamp
);