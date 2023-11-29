-- Adds a foreign key constraint named 'fk_store_code' to the 'orders_table'.
-- This constraint links 'store_code' in 'orders_table' to the 'store_code' in 'dim_store_details',
-- enforcing referential integrity between the two tables.
ALTER TABLE orders_table
ADD CONSTRAINT fk_store_code
FOREIGN KEY (store_code)
REFERENCES dim_store_details (store_code);