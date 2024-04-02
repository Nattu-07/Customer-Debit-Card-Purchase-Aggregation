CREATE DATABASE IF NOT EXISTS rds_db;
USE rds_db;
CREATE TABLE aggregated_transactions (
    customer_id INT PRIMARY KEY,
    debit_card_number VARCHAR(255),
    bank_name VARCHAR(255),
    total_amount_spent DECIMAL(10, 2)
);
