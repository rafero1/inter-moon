CREATE TABLE IF NOT EXISTS customers (
    c_id bigint PRIMARY KEY,
    c_name varchar,
    c_email varchar,
    c_gender varchar,
    c_phone varchar,
    c_birth_date date,
    c_type varchar
);
