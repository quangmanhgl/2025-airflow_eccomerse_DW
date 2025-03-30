SET FOREIGN_KEY_CHECKS = 0;

-- Load product categories first
LOAD DATA INFILE '/var/lib/mysql-files/dataset/product_category_name_translation.csv'
IGNORE INTO TABLE product_categories
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- Load geolocation data with IGNORE to skip duplicates
LOAD DATA INFILE '/var/lib/mysql-files/dataset/olist_geolocation_dataset.csv'
IGNORE INTO TABLE geolocation
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- Load sellers
LOAD DATA INFILE '/var/lib/mysql-files/dataset/olist_sellers_dataset.csv'
IGNORE INTO TABLE sellers
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- Load customers
LOAD DATA INFILE '/var/lib/mysql-files/dataset/olist_customers_dataset.csv'
IGNORE INTO TABLE customers
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- Load products (fix typo in path - remove backtick)
LOAD DATA INFILE '/var/lib/mysql-files/dataset/olist_products_dataset.csv'
IGNORE INTO TABLE products
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- Load orders
LOAD DATA INFILE '/var/lib/mysql-files/dataset/olist_orders_dataset.csv'
IGNORE INTO TABLE orders
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- Load order items
LOAD DATA INFILE '/var/lib/mysql-files/dataset/olist_order_items_dataset.csv'
IGNORE INTO TABLE order_items
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- Load order payments
LOAD DATA INFILE '/var/lib/mysql-files/dataset/olist_order_payments_dataset.csv'
IGNORE INTO TABLE order_payments
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- Load order reviews (depends on orders) with IGNORE to skip duplicates
LOAD DATA INFILE '/var/lib/mysql-files/dataset/olist_order_reviews_dataset.csv'
IGNORE INTO TABLE order_reviews
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- Re-enable foreign key checks
SET FOREIGN_KEY_CHECKS = 1;
