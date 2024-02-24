import sys
import typing
import psycopg2
from moon.logger import log
from moon.mapper.mapper import Mapper
from timeit import default_timer as timer


class DataTempManager:
    @staticmethod
    def select_with_conditionals_in_rdb(request, bc_data, query, bc_entities):
        i = 0
        creates = []
        inserts = ''

        start = timer()

        while i < len(bc_entities):
            entity = bc_entities[i]
            creates.append(
                Mapper.generate_sql_create_temp_table(entity, False)
            )
            sql_insert_temp = Mapper.generate_sql_insert_temp_table(
                entity, False)
            for dt in bc_data[i]:
                inserts += (
                    sql_insert_temp +
                    DataTempManager.transform_dict_text_to_insert_temp(dt, False) +
                    ';'
                )
            i = i + 1

        end = timer()
        #print('finished preparing creates/inserts in', round(end - start, 5), 'ms')

        if not inserts:
            return 0

        try:
            # creating connection
            connection = psycopg2.connect(user=request.db_user,
                                          password=request.db_password,
                                          host=request.db_host,
                                          port=request.db_port,
                                          database=request.db_name)
            cursor = connection.cursor()

            start = timer()
            for create in creates:
                cursor.execute(create)
            end = timer()
            #print('finished executing creates in', round(end - start, 5), 'ms')

            start = timer()
            cursor.execute(inserts)
            end = timer()
            #print('finished executing inserts in', round(end - start, 5), 'ms')

            start = timer()
            cursor.execute(query)
            end = timer()
            #print('finished executing query in', round(end - start, 5), 'ms')

            data_return = cursor.fetchall()
            if connection:
                cursor.close()
                connection.close()
            return data_return
        except (Exception, psycopg2.Error):
            log.e('Data Temp Manager Module', sys.exc_info())

    @staticmethod
    def select_hashes_with_conditionals_in_rdb(request, bc_data, query, bc_entities) -> typing.Any:
        i = 0
        creates = []
        inserts = ""

        start = timer()

        while i < len(bc_entities):
            entity = bc_entities[i]
            creates.append(
                Mapper.generate_sql_create_temp_table(entity, True)
            )
            sql_insert_temp = Mapper.generate_sql_insert_temp_table(
                entity, True)
            for dt in bc_data[i]:
                inserts += (
                    sql_insert_temp +
                    DataTempManager.transform_dict_text_to_insert_temp(dt, True) +
                    ';'
                )
            i = i + 1

        end = timer()
        #print('finished preparing creates/inserts in', round(end - start, 5), 'ms')

        if not inserts:
            return 0

        try:
            # creating connection
            connection = psycopg2.connect(user=request.db_user,
                                          password=request.db_password,
                                          host=request.db_host,
                                          port=request.db_port,
                                          database=request.db_name)
            cursor = connection.cursor()

            start = timer()
            for create in creates:
                cursor.execute(create)
            end = timer()
            #print('finished executing creates in', round(end - start, 5), 'ms')

            start = timer()
            cursor.execute(inserts)
            end = timer()
            #print('finished executing inserts in', round(end - start, 5), 'ms')

            start = timer()
            cursor.execute(query)
            end = timer()
            #print('finished executing query in', round(end - start, 5), 'ms')

            data_return = cursor.fetchall()
            if connection:
                cursor.close()
                connection.close()
            return data_return
        except (Exception, psycopg2.Error):
            log.e('Data Temp Manager Module', sys.exc_info())

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
        # It will be a new data to send to blockchain
        data_return = None

        # Getting a SQL data to create a temp table for the wanted table
        sql_create_temp = Mapper.generate_sql_create_temp_table(entity, True)

        # Insert statement compatible with this entity
        sql_insert_temp = Mapper.generate_sql_insert_temp_table(entity, True)

        try:
            # Creating connection
            connection = psycopg2.connect(user=request.db_user,
                                          password=request.db_password,
                                          host=request.db_host,
                                          port=request.db_port,
                                          database=request.db_name)
            cursor = connection.cursor()

            # Creating the temp table
            cursor.execute(sql_create_temp)

            # Inserting the data for each tuple of data
            data_to_insert = data[0]

            sql_to_insert = ''
            for d in data_to_insert:
                sql_to_insert += (
                    sql_insert_temp +
                    DataTempManager.transform_dict_text_to_insert_temp(d, True) +
                    ';'
                )
            cursor.execute(sql_to_insert)

            # executing the received query
            cursor.execute(query_update)
            cursor.execute(query_select)
            data_return = cursor.fetchall()

            if connection:
                cursor.close()
                connection.close()
        except (Exception, psycopg2.Error):
            log.e('Data Temp Manager Module', sys.exc_info())
        finally:
            return data_return

    @staticmethod
    def transform_dict_text_to_insert_temp(input_list, with_hash):
        """
        Get a dict and returns a string with values
        of the dict on a format to be inserted on a INSERT statement
        :param input_list: a dict
        :return: a string in the format to put it in a insert statement
        """
        entity = input_list['entity']
        try:
            del input_list['entity']
        except KeyError:
            # if doesnt exists 'entity'
            pass

        try:
            del input_list['before']
        except KeyError:
            # if doesnt exists 'before'
            pass

        if not with_hash:
            try:
                del input_list['hash']
            except KeyError:
                # if doesnt exists 'hash'
                pass

        # Get only data of input_list
        input_list = list(input_list.values())

        # Get string to put the insert
        insert_text, attributes = Mapper.get_elements_insert_with_type(entity)

        # Put the space for 'hash' attribute space
        # and closing the insert request
        if with_hash:
            insert_text += ',\'{}\')'
        else:
            insert_text += ')'

        # if len(input_list) < len(attributes):

        # print(attributes)
        # print(insert_text)
        # print(input_list)
        txt = insert_text.format(*input_list)
        return txt
