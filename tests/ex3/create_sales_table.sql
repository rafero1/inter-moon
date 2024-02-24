CREATE TABLE IF NOT EXISTS sales (
    s_id bigint PRIMARY KEY,
    s_invoice_id varchar,
    s_branch varchar,
    s_city varchar,
    s_product_line varchar,
    s_unit_price decimal,
    s_quantity int,
    s_tax_5_pct decimal,
    s_total decimal,
    s_date date,
    s_time varchar,
    s_payment varchar,
    s_cogs decimal,
    s_gross_margin_percentage decimal,
    s_gross_income decimal,
    s_rating decimal,
    c_id bigint REFERENCES customers(c_id)
);
