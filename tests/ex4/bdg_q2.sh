curl -X POST -d "bdrel(select c_type, sum(s_total) from customers c join bdcast(bdtext({'op': 'scan', 'table': 'sales'}), sales_acc_2, '(s_invoice_id varchar, s_branch varchar, s_city varchar, s_product_line varchar, s_unit_price decimal, s_quantity int, s_tax_5_pct decimal, s_total decimal, s_date date, s_time varchar, s_payment varchar, s_cogs decimal, s_gross_margin_percentage decimal, s_gross_income decimal, s_rating decimal, c_id bigint)', relational) s on c.c_id = s.c_id group by c_type;)" http://192.168.0.104:8080/bigdawg/query