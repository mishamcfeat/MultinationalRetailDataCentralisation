-- Adds a foreign key constraint named 'fk_product_code' to the 'orders_table'.
-- It ensures that each 'product_code' in 'orders_table' is a valid entry in the 'dim_products' table.
ALTER TABLE orders_table
ADD CONSTRAINT fk_product_code
FOREIGN KEY (product_code)
REFERENCES dim_products (product_code);