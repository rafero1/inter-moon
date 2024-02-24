import os
import re
import time
import sqlparse

from sql_metadata import Parser
from sqlparse import sql as SQL
from sqlparse import tokens as T
from timeit import timeit

from moon.mapper.sql_analyzer import QueryAnalyzer


def using_parser(query: str):
    return QueryAnalyzer.get_entities_parser(query)

def using_custom(query: str):
    return QueryAnalyzer.get_entities(query)

def test_parser(query: str, expected: list):
    rs = using_parser(query)
    assert rs == expected, f"expected: {expected}, got: {rs}"
    return rs

def test_custom(query: str, expected: list):
    rs = using_custom(query)
    assert rs == expected, f"expected: {expected}, got: {rs}"
    return rs

test_queries = [
    ("select * from table2 as t2, table3 t3", ['table2', 'table3']),
    ("select * from (select * from (select * from table_original) t0) t1", ['table_original']),
    ("select t2.num from (select * from table_original) t2;", ['table_original']),
    ("select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, sum(l_extendedprice * (1 - l_discount)) as sum_disc_price, sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge, avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order from lineitem where l_shipdate <= date '1998-12-01' - interval '90 days' group by l_returnflag, l_linestatus order by l_returnflag, l_linestatus limit 100 ;", ['lineitem']),
    ("SELECT supp_nation, cust_nation, l_year, sum(volume) AS revenue FROM (SELECT n1.n_name AS supp_nation, n2.n_name AS cust_nation, extract(YEAR FROM l_shipdate) AS l_year, l_extendedprice * (1 - l_discount) AS volume  FROM supplier, lineitem, orders, customer, nation n1, nation n2  WHERE s_suppkey = l_suppkey  AND o_orderkey = l_orderkey  AND c_custkey = o_custkey  AND s_nationkey = n1.n_nationkey  AND c_nationkey = n2.n_nationkey  AND ((n1.n_name = 'FRANCE'  AND n2.n_name = 'GERMANY') OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE'))  AND l_shipdate BETWEEN date '1995-01-01' AND date '1996-12-31' ) AS shipping GROUP BY supp_nation,  cust_nation,  l_year ORDER BY supp_nation,  cust_nation,  l_year ;", ['part', 'supplier', 'lineitem', 'partsupp', 'orders', 'nation']),
    ("select o_year, sum(case when nation = 'BRAZIL' then volume else 0 end) / sum(volume) as mkt_share from ( select extract(year from o_orderdate) as o_year, l_extendedprice * (1 - l_discount) as volume, n2.n_name as nation from part, supplier, lineitem, orders, customer, nation n1, nation n2, region where p_partkey = l_partkey and s_suppkey = l_suppkey and l_orderkey = o_orderkey and o_custkey = c_custkey and c_nationkey = n1.n_nationkey and n1.n_regionkey = r_regionkey and r_name = 'AMERICA' and s_nationkey = n2.n_nationkey and o_orderdate between date '1995-01-01' and date '1996-12-31' and p_type = 'ECONOMY ANODIZED STEEL' ) as all_nations group by o_year order by o_year;", ['part', 'supplier', 'lineitem', 'orders', 'customer', 'nation', 'nation', 'region']),
    ("select nation, o_year, sum(amount) as sum_profit from ( select n_name as nation, extract(year from o_orderdate) as o_year, l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount from part, supplier, lineitem, partsupp, orders, nation where s_suppkey = l_suppkey and ps_suppkey = l_suppkey and ps_partkey = l_partkey and p_partkey = l_partkey and o_orderkey = l_orderkey and s_nationkey = n_nationkey and p_name like '%green%' ) as profit group by nation, o_year order by nation, o_year desc limit 20 ;", ['part', 'supplier', 'lineitem', 'partsupp', 'orders', 'nation']),
    ("select c_count, count(*) as custdist from ( select c_custkey, count(o_orderkey) from customer left outer join orders on c_custkey = o_custkey and o_comment not like '%special%requests%' group by c_custkey ) as c_orders (c_custkey, c_count) group by c_count order by custdist desc, c_count desc;", ['customer', 'orders']),
    ("select s_name, count(*) as numwait from supplier, lineitem l1, orders, nation where s_suppkey = l1.l_suppkey and o_orderkey = l1.l_orderkey and o_orderstatus = 'F' and l1.l_receiptdate > l1.l_commitdate and exists ( select * from lineitem l2 where l2.l_orderkey = l1.l_orderkey and l2.l_suppkey <> l1.l_suppkey ) and not exists ( select * from lineitem l3 	where l3.l_orderkey = l1.l_orderkey and l3.l_suppkey <> l1.l_suppkey and l3.l_receiptdate > l3.l_commitdate ) and s_nationkey = n_nationkey and n_name = 'SAUDI ARABIA' group by s_name order by numwait desc, s_name;", ['supplier', 'lineitem', 'orders', 'nation']),
    ("select cntrycode, count(*) as numcust, sum(c_acctbal) as totacctbal from ( select substring(c_phone from 1 for 2) as cntrycode, c_acctbal from customer where substring(c_phone from 1 for 2) in ('13', '31', '23', '29', '30', '18', '17') and c_acctbal > ( select avg(c_acctbal) from customer where c_acctbal > 0.00 and substring(c_phone from 1 for 2) in ('13', '31', '23', '29', '30', '18', '17') ) and not exists ( select * from orders where o_custkey = c_custkey ) ) as custsale group by cntrycode order by cntrycode;", ['customer', 'orders']),
]

# for query, expected in test_queries:
#     print("query:", query)
#     print("tokens:", sqlparse.parse(query)[0].tokens)

#     test_parser(query, expected)
#     print("parser passed")

#     test_custom(query, expected)
#     print("custom passed")
#     print("----")

# timers_parser = []
# timers_custom = []
# for _ in range(1000):
#     query = test_queries[3][9]

#     start = time.time()
#     using_parser(query)
#     timers_parser.append(time.time() - start)

#     start = time.time()
#     using_custom(query)
#     timers_custom.append(time.time() - start)

# print("parser:", (sum(timers_parser) / len(timers_parser)) * 1000, "ms")
# print("custom:", (sum(timers_custom) / len(timers_custom)) * 1000, "ms")

# load queries from sql file
dir_path = os.path.dirname(os.path.realpath(__file__))
with open(f"{dir_path}/queries-dss.sql", "r") as f:
    test_queries = " ".join(f.read().split()).split(";")[:-1]

for query in test_queries:
# for query in [test_queries[14], test_queries[18], test_queries[19]]:
# for query in [test_queries[14]]:
    print()
    query = query.strip()
    print(query)

    start = time.time()
    rs1 = using_parser(query)
    print(f"parser: {rs1} ({round((time.time() - start) * 1000, 2)} ms)")

    start = time.time()
    rs2 = using_custom(query)
    print(f"custom: {rs2} ({round((time.time() - start) * 1000, 2)} ms)")

    print("MATCH ✅" if rs1 == rs2 else "NOT MATCH ❌")
