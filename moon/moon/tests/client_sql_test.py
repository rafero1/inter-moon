import unittest
from moon.sqlclient.clientsql import ClientSQL
from moon.communication.request import Request
from moon.sqlclient.clientsql import DELETE, UPDATE, INSERT, SELECT


class ClientSQLTest(unittest.TestCase):
    def test(self):
        # insert
        req = Request(
            q_query="INSERT INTO table_test(name, number) '\
                'VALUES ('test_name_value', 123456)",
            q_type=INSERT,
            q_entities=["table_test"],
            db_user='postgres',
            db_name='db_test',
            db_password='ufc123',
            db_host='localhost',
            db_port=5432,
            bc_port=9984,
            bc_host="0.0.0.0",
            bc_public_key=None,
            bc_private_key=None
        )
        client = ClientSQL(request=req)
        client.start()
        client.join()
        result = client.get_result()
        self.assertEqual(result, 1)

        # select
        req.q_query = "SELECT * FROM table_test"
        req.q_type = SELECT
        client = ClientSQL(request=req)
        client.start()
        client.join()
        result = client.get_result()
        self.assertEqual(result, [('test_name_value', 123456)])

        # update
        req.q_query = "UPDATE table_test SET name= 'testt' "
        req.q_type = UPDATE
        client = ClientSQL(request=req)
        client.start()
        client.join()
        result = client.get_result()
        self.assertEqual(result, 1)

        # delete
        req.q_query = "DELETE FROM table_test WHERE name= 'testt'"
        req.q_type = DELETE
        client = ClientSQL(request=req)
        client.start()
        client.join()
        result = client.get_result()
        self.assertEqual(result, 1)


if __name__ == "__main__":  # pragma: no cover
    t = ClientSQLTest()
    t.test()
