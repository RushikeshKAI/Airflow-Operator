INSERT INTO my_table (table_value)
VALUES ("my_value")
ON CONFLICT(table_value)
DO NOTHING
