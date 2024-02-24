import sys
from logger import log
from threading import Thread
from communication.query_type import DELETE, INSERT, SELECT, UPDATE
from configuration.config import Configuration
from sqlclient.generic_driver import GenericDBDriver


class ClientSQL(Thread):
    def __init__(self, request):
        super().__init__()

        self.result = []
        self.request = request

    def run(self):
        if SELECT == self.request.q_type:
            self.result = self._read_db()
        elif self.request.q_type in [DELETE, UPDATE, INSERT]:
            self.result = self._write_db()
        else:
            raise Exception('Unrecognized Request Type')

    def _write_db(self):
        """
        Executes write in relational database
        :return: number of affected rows
        """
        config = Configuration.get_instance()
        affected_rows = [0]

        try:
            driver = GenericDBDriver.get_instance()
            connection = driver.adapter.connect(
                user=config.db_user,
                password=config.db_password,
                host=config.db_host,
                port=config.db_port,
                database=self.request.db_name or config.db_name
            )

            cursor = connection.cursor()
            cursor.execute(self.request.q_query)
            connection.commit()
            affected_rows = [cursor.rowcount]

            if connection:
                cursor.close()
                connection.close()
        except (Exception):
            log.e('Client SQL Module', sys.exc_info())
        finally:
            return affected_rows

    def _read_db(self):
        """
        execute select in relational database
        :return: list of data
        """
        config = Configuration.get_instance()
        all_data = []

        try:
            driver = GenericDBDriver.get_instance()
            connection = driver.adapter.connect(
                user=config.db_user,
                password=config.db_password,
                host=config.db_host,
                port=config.db_port,
                database=self.request.db_name or config.db_name
            )

            cursor = connection.cursor()
            cursor.execute(self.request.q_query)
            data = cursor.fetchall()

            for d in data:
                all_data.append(d)

            if connection:
                cursor.close()
                connection.close()
        except (Exception):
            log.e('Client SQL Module', sys.exc_info())
        finally:
            return all_data

    def get_result(self) -> list:
        return self.result
