import sys
import typing
import pydash
from communication.request import Request
from helpers.floats import s_to_ms
from logger import log
from mapper.column import DEFAULT_ENTITY, DEFAULT_HASH
from mapper.mapper import Mapper
from timeit import default_timer as timer

from mapper.schema_manager import SchemaManager
from helpers.strings import add_single_quotes, ellipsize, truncate
from configuration.config import Configuration
from sqlclient.generic_driver import GenericDBDriver
from sql_analyzer.sql_analyzer import SQLAnalyzer


class DataTempManager:
    @staticmethod
    def select_with_conditionals_in_rdb(request: Request, query: str, bc_data, bc_entities: typing.List[str], with_hash: bool = False):
        """
        Executes a SELECT operation on the relational database using the given parameters
        and returns a tuple containing the results.

        :param request Request: The Request object.
        :param query str: the SQL query to execute.
        :param bc_data list: List of lists of blockchain tx data relevant to the query.
        :param bc_entities list[str]: List of blockchain entity names relevant to the query.
        :param with_hash bool: Whether the query should return bc hashes or not.

        :return: Tuple containing the query results. An empty tuple if the query has no results.
        """
        config = Configuration.get_instance()

        creates: typing.List[str] = []
        inserts: typing.List[str] = []

        for i, entity_name in enumerate(bc_entities):
            # get list of unique columns among all given assets
            # start = timer()
            columns = Mapper.get_columns_from_assets(entity_name, bc_data[i])
            # end = timer()
            # print("finished getting columns in", s_to_ms(end-start, 5), "ms")

            # generate CREATE TEMP TABLE statements
            # start = timer()
            stmt = Mapper.generate_sql_create_temp_table_from_columns(
                entity_name, columns, with_hash)
            if stmt:
                creates.append(stmt)
            # end = timer()

            # print("prepared stmt:", ellipsize(stmt, 50))
            # print("finished preparing creates in", s_to_ms(end-start, 5), "ms")

            # generate INSERT INTO TABLE statements
            # TODO: optimize this
            # start = timer()
            # stmt = Mapper.generate_sql_insert_into_temp_table_from_bc_data(
            #     entity_name, columns, bc_data[i], with_hash)
            # if stmt:
            #     inserts.append(stmt)
            for j in range(0, len(bc_data[i]), 1000):
                bc_data_slice = bc_data[i][j:j+1000]
                stmt = Mapper.generate_sql_insert_into_temp_table_from_bc_data(entity_name, columns, bc_data_slice, with_hash)
                if stmt:
                    inserts.append(stmt)
            # end = timer()

            # print("prepared stmt:", ellipsize(str(stmt), 50))
            # print('finished preparing inserts in',
            #     s_to_ms(end-start, 5), 'ms')


        if not inserts:
            return ()

        try:
            # creating connection
            driver = GenericDBDriver.get_instance()
            connection = driver.adapter.connect(
                user=config.db_user,
                password=config.db_password,
                host=config.db_host,
                port=config.db_port,
                database=request.db_name or config.db_name
            )

            cursor = connection.cursor()

            # start = timer()
            for create in creates:
                cursor.execute(create)
            # end = timer()
            # print('finished executing creates in',
            #       s_to_ms(end-start, 5), 'ms')

            start = timer()
            for insert in inserts:
                cursor.execute(insert)
            end = timer()
            print('finished executing inserts in',
                  s_to_ms(end-start, 5), 'ms')

            # start = timer()
            # print("executing stmt:", ellipsize(query, 50))
            cursor.execute(query)
            # end = timer()
            # print('finished executing query in',
            #       s_to_ms(end-start, 5), 'ms')

            data_return = cursor.fetchall()

            if connection:
                cursor.close()
                connection.close()

            return data_return
        except (Exception):
            log.e('Data Temp Manager Module', sys.exc_info())
            return ()

    @staticmethod
    def update_bc_in_rdb(request, query_update, query_select, data, entity):
        """
        Receives bc data, insert it into rdb and updates the data

        :param request: request information to access a database and bc
        :param data: data which will be inserted on a entity
        :param query: sql query
        :param entity: entity to insert the data
        :return: a list with the response of the sql query
        """
        config = Configuration.get_instance()

        # It will be a new data to send to blockchain
        data_return = None

        # get list of unique columns among all given assets
        columns = Mapper.get_columns_from_assets(entity, data)

        # generate CREATE TEMP TABLE statements
        create = Mapper.generate_sql_create_temp_table_from_columns(
            entity, columns, True)
        print("prepared stmt:", ellipsize(create, 50))

        # generate INSERT INTO TABLE statements
        insert = Mapper.generate_sql_insert_into_temp_table_from_bc_data(
            entity, columns, data, True)
        if insert:
            print("prepared stmt:", ellipsize(insert, 50))

        try:
            # Creating connection
            driver = GenericDBDriver.get_instance()
            connection = driver.adapter.connect(
                user=config.db_user,
                password=config.db_password,
                host=config.db_host,
                port=config.db_port,
                database=request.db_name or config.db_name
            )

            cursor = connection.cursor()

            # Creating the temp table
            if create:
                start = timer()
                cursor.execute(create)
                end = timer()
                print('finished executing creates in',
                      s_to_ms(end-start, 5), 'ms')

            # Inserting into the temp table
            if insert:
                start = timer()
                cursor.execute(insert)
                end = timer()
                print('finished executing inserts in',
                      s_to_ms(end-start, 5), 'ms')

            # executing the received query
            start = timer()
            print("executing stmt:", ellipsize(query_select, 50))
            cursor.execute(query_select)
            end = timer()
            print('finished executing stmt in',
                  s_to_ms(end-start, 5), 'ms')

            to_update = cursor.fetchall()
            if len(to_update) == 0:
                return ()

            start = timer()
            print("executing stmt:", ellipsize(query_update, 50))
            cursor.execute(query_update)
            end = timer()
            print('finished executing stmt in',
                  s_to_ms(end-start, 5), 'ms')

            query_select_updated = SQLAnalyzer('').generate_select_all_where_in(
                entity, [add_single_quotes(x[0]) for x in to_update], DEFAULT_HASH)

            start = timer()
            print("executing stmt:", ellipsize(query_select_updated, 50))
            cursor.execute(query_select_updated)
            end = timer()
            print('finished executing stmt in',
                  s_to_ms(end-start, 5), 'ms')

            data_return = cursor.fetchall()

            if connection:
                cursor.close()
                connection.close()

            return data_return
        except (Exception):
            log.e('Data Temp Manager Module', sys.exc_info())
            return ()
