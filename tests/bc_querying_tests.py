from datetime import datetime
import random as random
import typing
import uuid as uuid
from bigchaindb_driver import BigchainDB
from bigchaindb_driver.crypto import generate_keypair
from faker import Faker
from communication.request import Request
from bcclient.bcclient import ClientBlockchain
from dotenv import load_dotenv

load_dotenv()


def query_bc_using_moon(query, entities, q_type):
    alice = generate_keypair()

    req = Request(
        q_query=query,
        q_type=q_type,
        q_entities=entities,
        db_user="postgres",
        db_name="test",
        db_password="postgres",
        db_port=5432,
        db_host="127.0.0.1",
        bc_host="0.0.0.0",
        bc_port=9984,
        bc_public_key=None,
        bc_private_key=None
    )

    client = ClientBlockchain(request=req)
    client.start()
    client.join()
    return client.get_result()


class Query:
    def __init__(self, sql: str, assets: typing.List[str], q_type: int) -> None:
        self.sql = sql
        self.assets = assets
        self.q_type = q_type

    def __repr__(self) -> str:
        return f"Query(sql={self.sql}, assets={self.assets}, q_type={self.q_type})"

    def __str__(self) -> str:
        return self.sql

if __name__ == "__main__":  # pragma: no cover
    fake = Faker()

    queries: typing.List[Query] = [
        Query(f"INSERT INTO lab_results (uid, patient_id, content_base64, datetime, lab_name, lab_site, expired) VALUES ('{uuid.uuid4()}', {random.randint(1, 100)}, 'a1FxdFdnUzdPUks4QTdCWXB2VG45NFJX', '20{random.randint(10, 20)}-01-09', 'Lab {random.choice(['A', 'B', 'C'])}', {random.randint(1, 2)}, 1);", ["lab_results"], Request.INSERT),
        Query("SELECT * FROM lab_results ORDER BY datetime DESC;", ["lab_results"], Request.SELECT),
        Query("SELECT * FROM lab_results WHERE datetime < '2020-01-01' ORDER BY datetime DESC LIMIT 1;", ["lab_results"], Request.SELECT),
        Query("SELECT lab_name, count(lab_name) AS result_count FROM lab_results GROUP BY lab_name;", ["lab_results"], Request.SELECT),
        Query("UPDATE lab_results SET expired = true WHERE uid = '5c4031b7-0cab-4c2c-b36c-48a76f0d97cf';", ["lab_results"], Request.UPDATE),
        Query("DELETE FROM lab_results WHERE uid = '5c4031b7-0cab-4c2c-b36c-48a76f0d97cf';", ["lab_results"], Request.DELETE),
    ]

    print("Choose a query to test:")
    queries_s = [f"{i}: {query}\n" for i, query in enumerate(queries)]
    choice = 0
    try:
        choice = int(input("".join(queries_s)))
    finally:
        print("")

    print(f"SQL: {queries[choice].sql}")
    print(f"Assets: {queries[choice].assets}", "\n")

    result = query_bc_using_moon(queries[choice].sql, queries[choice].assets, queries[choice].q_type)
    print("Result:", result, "\n")
