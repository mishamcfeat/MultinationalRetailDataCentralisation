-- Establishes a foreign key relationship named 'fk_date_uuid' from 'orders_table' to 'dim_date_times'.
-- This guarantees that each 'date_uuid' in 'orders_table' corresponds to an entry in 'dim_date_times'.
ALTER TABLE orders_table
ADD CONSTRAINT fk_date_uuid
FOREIGN KEY (date_uuid)
REFERENCES dim_date_times (date_uuid);