import time
import unittest
from moon.communication.request import Request
from moon.sqlclient.clientsql import INSERT, SELECT
from moon.bcclient.bcclient import ClientBlockchain


class JoinTest(unittest.TestCase):
    def test(self):
        # insert
        req = Request(
            q_query="INSERT INTO entity_a(id_a, name) '\
                'VALUES (123456, 'hai')",
            q_type=INSERT,
            q_entities=["entity_a"],
            db_user="postgres",
            db_name="index_bc",
            db_password="ufc123",
            db_port=5432,
            db_host="localhost",
            bc_host="0.0.0.0",
            bc_port=9984,
            bc_public_key=None,
            bc_private_key=None
        )
        client = ClientBlockchain(request=req)
        client.start()
        client.join()
        result = client.get_result()
        print("RESULT CLIENT:", result)
        self.assertEqual(result, 1)

        time.sleep(4)

        req = Request(
            q_query="INSERT INTO entity_b(id_b, name) '\
                'VALUES (123456, 'hey')",
            q_type=INSERT,
            q_entities=["entity_b"],
            db_user="postgres",
            db_name="index_bc",
            db_password="ufc123",
            db_port=5432,
            db_host="localhost",
            bc_host="0.0.0.0",
            bc_port=9984,
            bc_public_key=None,
            bc_private_key=None
        )
        client = ClientBlockchain(request=req)
        client.start()
        client.join()
        result = client.get_result()
        print("RESULT CLIENT:", result)
        self.assertEqual(result, 1)

        time.sleep(4)

        # select
        req.q_query = "SELECT * FROM entity_a, entity_b ' \
            'WHERE entity_a.id_a = entity_b.id_b"
        req.q_type = SELECT
        req.q_entities = ["entity_a", "entity_b"]
        client = ClientBlockchain(request=req)
        client.start()
        client.join()
        result = client.get_result()
        print("RESULT CLIENT:", result)
        self.assertEqual(1, len(result))


if __name__ == "__main__":  # pragma: no cover
    t = JoinTest()
    t.test()
