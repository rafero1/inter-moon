from datetime import datetime
import random as random
import uuid as uuid
from bigchaindb_driver import BigchainDB
from bigchaindb_driver.crypto import generate_keypair
from faker import Faker
from moon.communication.request import Request
from moon.bcclient.bcclient import ClientBlockchain


def make_transactions():
    fake = Faker()

    alice = generate_keypair()
    bdb = BigchainDB('http://{}:{}'.format('0.0.0.0', 9984))

    tx = bdb.transactions.prepare(
        operation='CREATE',
        signers=alice.public_key,
        asset={
            'data': {
                'name': fake.name(),
                'addr': fake.address()
            }
        },
        metadata={
            'version': 0,
            'created_at': str(datetime.now()),
            'updated_at': str(datetime.now()),
            'deleted_at': str(datetime.now())
        }
    )

    signed_tx = bdb.transactions.fulfill(tx, private_keys=alice.private_key)
    transaction_sent = bdb.transactions.send_sync(signed_tx)

    print(transaction_sent)


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


if __name__ == "__main__":  # pragma: no cover
    fake = Faker()

    queries = [
        # ("SELECT * FROM users3",
        #  ["users3"], Request.SELECT),
        # ("SELECT name, COUNT(name) FROM users3 WHERE COUNT(name) > 0 GROUP BY name",
        #  ["users3"], Request.SELECT),
        # ("SELECT * from users3 as a, statuses as b WHERE a.status = b.id AND b.id = 2",
        #  ["users3", "statuses"], Request.SELECT),
        # ("SELECT CHAR_LENGTH(name), COUNT(CHAR_LENGTH(name)) from users3 WHERE CHAR_LENGTH(name) > 0 GROUP BY CHAR_LENGTH(name)",
        #  ["users3"], Request.SELECT),
        # ("SELECT * FROM statuses",
        #  ["statuses"], Request.SELECT),
        # ("SELECT * from users3 WHERE EXISTS (SELECT id FROM statuses WHERE id = 3)",
        #  ["users3", "statuses"], Request.SELECT),
        # (f"INSERT INTO users2 (uid, name, money) VALUES ('{uuid.uuid4()}', '{fake.name()}', {random.randint(10, 9999)})",
        #  ["users2"], Request.INSERT),
        # (f"INSERT INTO statuses (id, name) VALUES (2, '{fake.catch_phrase()}')",
        #  ["statuses"], Request.INSERT),
        # (f"INSERT INTO statuses (id, name) VALUES ('{random.randint(1, 9999)}', '{fake.catch_phrase()}')",
        #  ["statuses"], Request.INSERT),
        # (f"UPDATE statuses SET name = '{fake.catch_phrase()}', id = {random.randint(1, 9999)}",
        #  ["statuses"], Request.UPDATE),
        ("SELECT * FROM statuses",
         ["statuses"], Request.SELECT),
        # ("DELETE FROM statuses WHERE id % 2 = 0",
        #  ["statuses"], Request.DELETE),
    ]

    for i, query in enumerate(queries):
        print(f"Query {i}:")
        print(f"SQL: {query[0]}")
        print(f"Assets: {query[1]}")
        print("")
        result = query_bc_using_moon(query[0], query[1], query[2])
        print("")
        print("Result:", result)
        print("")
        print("---")
        print("")
