import os
import re
import time
import sqlparse
import sys

from sql_metadata import Parser
from sqlparse import sql as SQL
from sqlparse import tokens as T
from timeit import timeit

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../moon')))
from mapper.sql_analyzer import QueryAnalyzer


def using_parser(query: str):
    return set(QueryAnalyzer.get_entities_parser(query))

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

# load queries from sql file
dir_path = os.path.dirname(os.path.realpath(__file__))
with open(f"{dir_path}/queries-dss.sql", "r") as f:
    test_queries = " ".join(f.read().split()).split(";")[:-1]

# for query in test_queries:
# for query in [test_queries[14], test_queries[18], test_queries[19]]:
# for query in [test_queries[1]]:
for query in ["SELECT * FROM table1 WHERE id IN (1,2,3)", "INSERT INTO table1 (id, name) VALUES (1, 'name')", "UPDATE table1 SET name = 'name' WHERE id = 1", "DELETE FROM table1 WHERE id = 1", ]:
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
