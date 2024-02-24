import unittest
from communication.request import Request
from sqlclient.clientsql import INSERT, SELECT
from bcclient.bcclient import ClientBlockchain


class ClientBlockchainTest(unittest.TestCase):
    def test(self):
        # insert
        req = Request(
            q_query="INSERT INTO entity_a(id_a, name) '\
                'VALUES (123456, 'test_name_value')",
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
        # client.start()
        # client.join()
        # result = client.get_result()
        # print("RESULT CLIENT:", result)
        # self.assertEqual(result, 1)

        # select
        req.q_query = "SELECT * FROM entity_a"
        req.q_type = SELECT
        client = ClientBlockchain(request=req)
        client.start()
        client.join()
        result = client.get_result()
        print("RESULT CLIENT:", result)
        self.assertEqual(1, len(result))

        # update
        # req.q_query = "UPDATE entity_a SET
        # name='test_name_value' WHERE name='testsssa'"
        # req.q_type = UPDATE
        # client = ClientBlockchain(request=req)
        # client.start()
        # client.join()
        # result = client.get_result()
        # print("RESULT CLIENT:", result)
        # self.assertEqual(result, 1)


if __name__ == "__main__":  # pragma: no cover
    t = ClientBlockchainTest()
    t.test()
