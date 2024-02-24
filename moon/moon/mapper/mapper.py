import sqlparse
from moon.mapper.schema_manager import SchemaManager


class Mapper:
    @staticmethod
    def sql_insert_to_bc(query, entity_name):
        """
        Get a sql query and map it to dict adopted by Blockchain.\n
        Accepted format of query:
        INSERT INTO table_name (col1,col2...) VALUES (val1,val2...)
        :param query: sql query
        :param entity_name: name of the entity
        :return: list of dicts with keys and values
            which will be stored on blockchain
        """
        res = sqlparse.parse(query)
        stmt = res[0]
        tokens = stmt.tokens
        keys, values = [], []
        count = 0
        for t in tokens:
            # getting only keys and values --> contains'('
            if str(t).count('('):
                count = count + 1
                if count == 1:
                    k = str(t).replace(' ', '')
                    keys = (k[k.index('(') + 1:k.rfind(')')]).split(',')
                elif count == 2:
                    v = str(t).replace(' ', '')
                    values = (v[v.index('(') + 1:v.rfind(')')]).split(',')
        # putting the name of the entity on the beginning
        keys.insert(0, 'entity')
        values.insert(0, entity_name)
        return [dict(zip(keys, values))]

    @staticmethod
    def sql_update_to_bc(query):
        """
        Gets a SQL query and maps it to dict on Blockchains format.\n
        Accepted format of query:
        \tUPDATE table_name SET col1=1, col2=2 WHERE col3=2
        :param query: SQL Query
        :return: A dict with values to be stored on blockchain
        """
        res = sqlparse.parse(query)
        stmt = res[0]
        tokens = stmt.tokens
        keys, values = [], []

        # Entity involved on update
        entity = str(tokens[2])
        keys.append('entity')
        values.append(entity)

        # New data to insert into blockchain
        # Tokens[6] is the data after 'SET ='
        # Generates a dict with desired columns to change
        data = str(tokens[6]).replace(' ', '').split(',')
        for d in data:
            t = d.split('=')
            keys.append(t[0])
            values.append(t[1])

        # Columns with new values
        new_data_to_store = dict(zip(keys, values))

        # Removing special chars of new data
        for item in new_data_to_store.items():
            new_data_to_store[item[0]] = Mapper.remove_special_chars(item[1])

        # If there a WHERE clause, it is in the position eight
        if len(tokens) > 8 and tokens[8] != '':
            conditionals = str(tokens[8]).replace('WHERE', '')
            conditionals = conditionals.replace('where', '')
            conditionals = conditionals.strip()
            select_data_to_updated_in_db = 'SELECT * FROM {} WHERE {}'.format(
                entity,
                conditionals
            )
        else:
            select_data_to_updated_in_db = 'SELECT * FROM {}'.format(entity)

        return select_data_to_updated_in_db

    @staticmethod
    def generate_sql_create_temp_table(entity_name, with_hash):
        """
        Generate an SQL to create a temporary table with
        the same structure as the entity
        :param entity_name: Entity name
        :return: SQL to create temp table
        """
        attributes = SchemaManager.get_attributes_by_entity(entity_name)
        columns = ''
        for a in attributes:
            columns += a['name'] + ' ' + a['type'] + ','

        if with_hash:
            columns += 'hash varchar'
        else:
            columns = columns[:-1]

        sql = 'CREATE TEMP TABLE {} ({})'.format(entity_name, columns)
        return sql

    @staticmethod
    def generate_sql_insert_temp_table(entity_name, with_hash):
        """
        Generates a incomplete SQL insert into
        temp table of an entity
        \n\tThis SQL does not include the values
        \n\n\tEx: "INSERT INTO entity_a (col1, col2) VALUES ("
        :param entity_name: Entity name
        :return: SQL to insert into a temp table
        """
        attributes = SchemaManager.get_attributes_by_entity(entity_name)
        columns = ''
        for a in attributes:
            columns += a['name'] + ','

        if with_hash:
            columns += 'hash'
        else:
            columns = columns[:-1]
        return 'INSERT INTO {} ({}) VALUES ('.format(entity_name, columns)

    @staticmethod
    def get_sql_delete_index_entries(entity, hashes_list):
        """
        Generates a sql delete to indexes

        :param entity: Entity name
        :param hashes_list: Python list of blockchain hashes
        :return: SQL query to delete index entries
        """
        hashes_to_delete = str(hashes_list)
        hashes_to_delete = hashes_to_delete.replace('[', '(')
        hashes_to_delete = hashes_to_delete.replace(']', ')')
        sql_stmt = 'DELETE FROM {}_index WHERE bc_entry IN {}'.format(
            entity,
            hashes_to_delete
        )
        return sql_stmt

    @staticmethod
    def remove_special_chars(text):
        """
        Removes single quotes
        """
        text = text.replace('\'', '')
        return text

    @staticmethod
    def get_elements_insert_with_type(entity_name):
        """
        Creates a string formatted according to
        the types of entity attributes
        :param entity_name: Entity Name
        :return: A formated string, ready to put the values
        ex: a int, b varchar --> {},'{}'
        """
        text = ''
        attributes = SchemaManager.get_attributes_by_entity(entity_name)
        for attribute in attributes:
            if attribute['type'] == 'integer':
                text += '{},'
            elif attribute['type'] == 'varchar':
                text += '\'{}\','
        text = text[:-1]
        return text, attributes

# print(Mapper.get_elements_insert_with_type("entity_a"))
# print(Mapper.
# sql_insert_to_bc("INSERT INTO table_name (col1,col2)
# VALUES (val1,val2)", "entty"))
# print(
#     Mapper.sql_update_to_bc(
#         "UPDATE table_name SET a = 1, b = 2 WHERE c = 2"
#     )
# )
# print(Mapper.generate_sql_insert_temp_table("entity_b"))
