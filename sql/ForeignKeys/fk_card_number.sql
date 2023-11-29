-- Implements a foreign key constraint named 'fk_card_number' for the 'orders_table'.
-- This assures that every 'card_number' in 'orders_table' has a matching 'card_number' in 'dim_card_details'.
ALTER TABLE orders_table
ADD CONSTRAINT fk_card_number
FOREIGN KEY (card_number)
REFERENCES dim_card_details (card_number);
