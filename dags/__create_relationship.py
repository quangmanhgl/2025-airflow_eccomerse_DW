from dags.Postgres_conn import Postgres

postgres = Postgres('postgres_2')
def create_relationship():
    # First add primary keys to tables
    add_primary_keys = """
    -- Add primary keys to tables IF they don't exist
    DO $$
    BEGIN
        -- Check and add primary key for customers
        IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pk_customers') THEN
            ALTER TABLE olist_db.customers 
            ADD CONSTRAINT pk_customers PRIMARY KEY (customer_id);
        END IF;

        -- Check and add primary key for orders
        IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pk_orders') THEN
            ALTER TABLE olist_db.orders 
            ADD CONSTRAINT pk_orders PRIMARY KEY (order_id);
        END IF;

        -- Check and add primary key for products
        IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pk_products') THEN
            ALTER TABLE olist_db.products 
            ADD CONSTRAINT pk_products PRIMARY KEY (product_id);
        END IF;

        -- Check and add primary key for sellers
        IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pk_sellers') THEN
            ALTER TABLE olist_db.sellers 
            ADD CONSTRAINT pk_sellers PRIMARY KEY (seller_id);
        END IF;

        -- Check and add primary key for dim_date
        IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pk_dim_date') THEN
            ALTER TABLE olist_db.dim_date 
            ADD CONSTRAINT pk_dim_date PRIMARY KEY (date);
        END IF;

        -- Add unique constraint to geolocation zip code if doesn't exist
        IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'uq_geolocation_zip_code_prefix') THEN
            ALTER TABLE olist_db.geolocation 
            ADD CONSTRAINT uq_geolocation_zip_code_prefix UNIQUE (geolocation_zip_code_prefix);
        END IF;
    END $$;
    """

    try:
        postgres.execute_query(add_primary_keys, schema='olist_db')
        print("Successfully added primary keys")
    except Exception as e:
        print(f"Error adding primary keys: {str(e)}")

    # Now add foreign key relationships
    add_relationships = """

    -- Customers relationships
    ALTER TABLE olist_db.orders 
    ADD CONSTRAINT fk_orders_customer_id
    FOREIGN KEY (customer_id) REFERENCES olist_db.customers (customer_id);

    -- Orders relationships
    ALTER TABLE olist_db.order_items 
    ADD CONSTRAINT fk_order_items_order_id 
    FOREIGN KEY (order_id) REFERENCES olist_db.orders (order_id);

    ALTER TABLE olist_db.order_payments 
    ADD CONSTRAINT fk_order_payments_order_id 
    FOREIGN KEY (order_id) REFERENCES olist_db.orders (order_id);

    -- Products relationships
    ALTER TABLE olist_db.order_items 
    ADD CONSTRAINT fk_order_items_product_id 
    FOREIGN KEY (product_id) REFERENCES olist_db.products (product_id);

    -- Sellers relationships
    ALTER TABLE olist_db.order_items 
    ADD CONSTRAINT fk_order_items_seller_id 
    FOREIGN KEY (seller_id) REFERENCES olist_db.sellers (seller_id);


    ALTER TABLE olist_db.orders 
    ADD CONSTRAINT fk_orders_order_approved_date 
    FOREIGN KEY (order_approved_at) REFERENCES olist_db.dim_date (date);


    -- Geolocation relationships
    ALTER TABLE olist_db.customers 
    ADD CONSTRAINT fk_customers_geolocation 
    FOREIGN KEY (customer_zip_code_prefix) REFERENCES olist_db.geolocation (geolocation_zip_code_prefix);

    ALTER TABLE olist_db.sellers 
    ADD CONSTRAINT fk_sellers_geolocation 
    FOREIGN KEY (seller_zip_code_prefix) REFERENCES olist_db.geolocation (geolocation_zip_code_prefix);
    """

    try:
        postgres.execute_query(add_relationships, schema='olist_db')
        print("Successfully added foreign key relationships")
    except Exception as e:
        print(f"Error adding relationships: {str(e)}")