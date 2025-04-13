CREATE TABLE orders (
    order_id UInt32,
    user_id UInt32,
    user_name String, 
    user_email String,
    order_date DateTime,
    payment_status Enum8('paid' = 1, 'pending' = 2, 'cancelled' = 3, 'refunded' = 4),
    total_amount Decimal(10, 2),
    delivery_address String,
    delivery_status Enum8('delivered' = 1, 'in_transit' = 2, 'processing' = 3, 'cancelled' = 4),
    PRIMARY KEY (order_id)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(order_date)
ORDER BY (order_id, user_id);

CREATE TABLE order_items (
    order_item_id UInt32,
    order_id UInt32,
    product_id UInt32,
    product_name String, 
    quantity UInt16,
    price Decimal(10, 2),
    discount Decimal(10, 2) DEFAULT 0.00,
    PRIMARY KEY (order_item_id)
) ENGINE = MergeTree()
ORDER BY (order_item_id, order_id, product_id);

