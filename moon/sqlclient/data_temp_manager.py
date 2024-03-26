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
    async def select_with_conditionals_in_rdb(request: Request, query: str, bc_data, bc_entities: typing.List[str], with_hash: bool = False) -> list[tuple] | None:
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

        start = timer()

        creates: typing.List[str] = []
        inserts: typing.List[str] = []

        for i, entity_name in enumerate(bc_entities):
            columns = Mapper.get_entity_columns(entity_name)

            stmt = Mapper.generate_sql_create_temp_table_from_columns(
                entity_name, columns, with_hash)
            if stmt:
                creates.append(stmt)

            # TODO: optimize this
            for j in range(0, len(bc_data[i]), 1000):
                bc_data_slice = bc_data[i][j:j+1000]
                stmt = Mapper.generate_sql_insert_into_temp_table_from_bc_data(
                    entity_name, columns, bc_data_slice, with_hash)
                if stmt:
                    inserts.append(stmt)

            log.i('Data Temp Manager Module',
                  f'({s_to_ms(timer()-start, 5)}) creates: {creates}')

        if not inserts:
            return None

        try:
            driver = GenericDBDriver.get_instance()
            connection = driver.adapter.connect(
                user=config.db_user,
                password=config.db_password,
                host=config.db_host,
                port=config.db_port,
                database=request.db_name or config.db_name
            )

            cursor = connection.cursor()

            for create in creates:
                cursor.execute(create)

            log.i('Data Temp Manager Module',
                  f'({s_to_ms(timer()-start, 5)}) executed creates: {creates}')

            for insert in inserts:
                cursor.execute(insert)

            log.i('Data Temp Manager Module',
                  f'({s_to_ms(timer()-start, 5)}) executed inserts: {inserts}')

            cursor.execute(query)

            log.i('Data Temp Manager Module',
                  f'({s_to_ms(timer()-start, 5)}) executed query: {query}')

            result = cursor.fetchall()

            if connection:
                cursor.close()
                connection.close()

            return result
        except (Exception):
            log.e('Data Temp Manager Module', sys.exc_info())
            return None

    @staticmethod
    async def update_bc_in_rdb(request, query_update, query_select, data, entity) -> list[tuple] | None:
        """
        Receives bc data, insert it into rdb and updates the data

        :param request: request information to access a database and bc
        :param data: data which will be inserted on a entity
        :param query: sql query
        :param entity: entity to insert the data
        :return: a list with the response of the sql query
        """
        config = Configuration.get_instance()

        start = timer()

        columns = Mapper.get_entity_columns(entity)

        create = Mapper.generate_sql_create_temp_table_from_columns(
            entity, columns, True)
        if create:
            log.i('Data Temp Manager Module',
                  f'create: {create}')

        insert = Mapper.generate_sql_insert_into_temp_table_from_bc_data(
            entity, columns, data, True)
        if insert:
            log.i('Data Temp Manager Module',
                  f'query_update: {insert}')

        try:
            driver = GenericDBDriver.get_instance()
            connection = driver.adapter.connect(
                user=config.db_user,
                password=config.db_password,
                host=config.db_host,
                port=config.db_port,
                database=request.db_name or config.db_name
            )

            cursor = connection.cursor()

            if create:
                cursor.execute(create)
                log.i('Data Temp Manager Module',
                      f'({s_to_ms(timer()-start, 5)}) executed create: {create}')

            if insert:
                cursor.execute(insert)
                log.i('Data Temp Manager Module',
                      f'({s_to_ms(timer()-start, 5)}) executed insert: {insert}')

            cursor.execute(query_select)

            log.i('Data Temp Manager Module',
                  f'({s_to_ms(timer()-start, 5)}) executed select: {query_select}')

            to_update = cursor.fetchall()
            if len(to_update) == 0:
                return None

            cursor.execute(query_update)

            log.i('Data Temp Manager Module',
                  f'({s_to_ms(timer()-start, 5)}) executed update: {query_update}')

            # TODO: Verify if the update actually checks for changes before continuing
            query_select_updated = SQLAnalyzer('').generate_select_all_where_in(
                entity, [add_single_quotes(x[0]) for x in to_update], DEFAULT_HASH)

            cursor.execute(query_select_updated)

            log.i('Data Temp Manager Module',
                  f'({s_to_ms(timer()-start, 5)}) executed select updated: {query_select_updated}')

            result = cursor.fetchall()

            if connection:
                cursor.close()
                connection.close()

            return result
        except (Exception):
            log.e('Data Temp Manager Module', sys.exc_info())
            return None
