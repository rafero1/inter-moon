import sys
import csv
import datetime
import psycopg2
from itertools import cycle
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

DDL_PATH = 'voter/ddl.txt'
QUERIES_PATH = 'voter/queries.txt'


class ClientSQL():
    def __init__(self, user, db_name, password, hosts, port):
        self.user = user
        self.db_name = db_name
        self.password = password
        self.hosts = cycle(hosts)
        self.port = port
        self.logfile = input('Log Filename:')
        if(
            self.logfile is None or
            self.logfile == '' or
            '.' in self.logfile
        ):
            self.logfile = 'log.csv'
        else:
            self.logfile += '.csv'

    def exec(self, query):
        if query is not None or query == '':
            query = query.rstrip()
            query_type = query.split(' ')[0].upper()
            if query_type == 'SELECT':
                before = datetime.datetime.now()
                result = self._read_db(query)
                after = datetime.datetime.now()
            elif query_type in ['DELETE', 'UPDATE', 'INSERT']:
                before = datetime.datetime.now()
                result = self._write_db(query)
                after = datetime.datetime.now()

            response_time = self._calculate_response_time(before, after)
            self._log_result(query, result, response_time)

    def exec_ddl(self, query):
        if query is not None and 'CREATE DATABASE' in query.upper():
            database_name = 'postgres'
        else:
            database_name = self.db_name
        try:
            connection = psycopg2.connect(
                user=self.user,
                password=self.password,
                host=next(self.hosts),
                port=self.port,
                database=database_name
            )
            connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            connection.autocommit = True
            cursor = connection.cursor()
            cursor.execute(query)
            if connection:
                cursor.close()
                connection.close()
        except (Exception, psycopg2.Error):
            print(sys.exc_info())

    def _write_db(self, query):
        affected_rows = 0
        try:
            connection = psycopg2.connect(user=self.user,
                                          password=self.password,
                                          host=next(self.hosts),
                                          port=self.port,
                                          database=self.db_name)
            cursor = connection.cursor()
            cursor.execute(query)
            connection.commit()
            affected_rows = cursor.rowcount
            if connection:
                cursor.close()
                connection.close()
        except (Exception, psycopg2.Error):
            print(sys.exc_info())
        finally:
            return affected_rows

    def _read_db(self, query):
        all_data = []
        try:
            connection = psycopg2.connect(user=self.user,
                                          password=self.password,
                                          host=next(self.hosts),
                                          port=self.port,
                                          database=self.db_name)
            cursor = connection.cursor()
            cursor.execute(query)
            data = cursor.fetchall()
            for d in data:
                all_data.append(d)
            if connection:
                cursor.close()
                connection.close()
        except (Exception, psycopg2.Error):
            print(sys.exc_info())
        finally:
            return all_data

    def _log_result(self, query, result, response_time):
        with open('logs/{}'.format(self.logfile), 'a', newline='') as log_file:
            writer = csv.writer(log_file)
            writer.writerow([query, result, response_time])

    def _calculate_response_time(self, before, after):
        response_time = after-before
        rt_millis = response_time.days * 86400000
        rt_millis += response_time.seconds * 1000
        rt_millis += round(response_time.microseconds/float(1000), 1)
        return rt_millis


if __name__ == "__main__":

    cli = ClientSQL(
        user='postgres',
        db_name='voter',
        password='ufc123',
        hosts=['localhost'],
        port=5432
    )

    with open(DDL_PATH, 'r', newline='') as ddl_file:
        for line in ddl_file:
            cli.exec_ddl(line)

    with open(QUERIES_PATH, 'r', newline='') as queries_file:
        for line in queries_file:
            cli.exec(line)
