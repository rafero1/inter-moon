import sys
import psycopg2
from moon.logger import log
from threading import Thread
from moon.communication.query_type import DELETE, INSERT, SELECT, UPDATE


class ClientSQL(Thread):
    def __init__(self, request):
        super().__init__()
        self.result = None
        self.request = request
        self.q_query = request.q_query
        self.q_type = request.q_type
        self.q_entities = request.q_entities
        self.db_user = request.db_user
        self.db_name = request.db_name
        self.db_password = request.db_password
        self.db_host = request.db_host
        self.db_port = request.db_port
        self.bc_host = request.bc_host
        self.bc_port = request.bc_port
        self.bc_public_key = request.bc_public_key
        self.bc_private_key = request.bc_private_key

    def run(self):
        if SELECT == self.q_type:
            self.result = self._read_db()
        elif self.q_type in [DELETE, UPDATE, INSERT]:
            self.result = self._write_db()
        else:
            raise Exception('Unrecognized Request Type')

    def _write_db(self):
        """
        Executes write in relational database
        :return: number of affected rows
        """
        affected_rows = 0
        try:
            connection = psycopg2.connect(user=self.db_user,
                                          password=self.db_password,
                                          host=self.db_host,
                                          port=self.db_port,
                                          database=self.db_name)
            cursor = connection.cursor()
            cursor.execute(self.q_query)
            connection.commit()
            affected_rows = cursor.rowcount
            if connection:
                cursor.close()
                connection.close()
        except (Exception, psycopg2.Error):
            log.e('Client SQL Module', sys.exc_info())
        finally:
            return affected_rows

    def _read_db(self):
        """
        execute select in relational database
        :return: list of data
        """
        all_data = []
        try:
            connection = psycopg2.connect(user=self.db_user,
                                          password=self.db_password,
                                          host=self.db_host,
                                          port=self.db_port,
                                          database=self.db_name)
            cursor = connection.cursor()
            cursor.execute(self.q_query)
            data = cursor.fetchall()
            for d in data:
                all_data.append(d)
            if connection:
                cursor.close()
                connection.close()
        except (Exception, psycopg2.Error):
            log.e('Client SQL Module', sys.exc_info())
        finally:
            return all_data

    def get_result(self):
        return self.result
