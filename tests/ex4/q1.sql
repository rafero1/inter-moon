select c_name, s_unit_price from customers c join sales s on c.c_id = s.c_id order by s.s_unit_price desc limit 10;