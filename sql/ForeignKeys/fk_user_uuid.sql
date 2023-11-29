-- Adds a foreign key constraint named 'fk_user_uuid' to the 'orders_table'.
-- This constraint ensures that every 'user_uuid' in 'orders_table' must have a corresponding value in 'dim_users'.
ALTER TABLE orders_table
ADD CONSTRAINT fk_user_uuid
FOREIGN KEY (user_uuid)
REFERENCES dim_users (user_uuid);
