import os
import json
from threading import Lock
import typing
import pydash
from mapper.column import Attribute, Column, ColumnList, SQLColumnType
from mapper.persistence_model import BLOCKCHAIN, RELATIONAL_DATABASE


class SchemaManager:
    entities = None
    mutex = Lock()

    @staticmethod
    def _get_data():
        """
        Gets the data from the data schema file
        :return: A dict with the schema
        """
        if SchemaManager.entities is None:
            SchemaManager.mutex.acquire()
            if SchemaManager.entities is None:
                with open(os.path.dirname(os.path.abspath(__file__)) +
                          '/catalog.json') as f:
                    data = json.load(f)
                SchemaManager.entities = data['entities']
            SchemaManager.mutex.release()

    @staticmethod
    def get_entity_list():
        """
        Get a list with all entities of the schema
        :return: list, names of the entities
        """
        SchemaManager._get_data()
        list_entities = []
        for e in SchemaManager.entities:
            list_entities.append(e['name'])
        return list_entities

    @staticmethod
    def get_entity_by_name(name):
        """
        Get a entity by name
        :param name: Entity name
        :return: Selected entity data or None
        """
        SchemaManager._get_data()
        for e in SchemaManager.entities:
            if e['name'] == name:
                return e

        raise Exception(
            'Entity \'{}\' Not Found'.format(name)
        )

    @staticmethod
    def get_attributes_by_entity(name):
        """
        Get the list of entity attributes
        :param name: Entity name
        :return: The list of entity attributes (name and type) or None
        """
        SchemaManager._get_data()
        ent = SchemaManager.get_entity_by_name(name)
        return ent['attributes']

    @staticmethod
    def get_entity_names_attributes(name):
        """
        Get the list attributes names of entity
        :param name: Entity name
        :return: The list attributes names of entity
        """
        SchemaManager._get_data()
        ent = SchemaManager.get_entity_by_name(name)
        result = []
        for aux in ent['attributes']:
            result.append(aux['name'])
        return result

    @staticmethod
    def get_attributes_name_type_pair_by_entity(name):
        """
        Get a dict of (name, type) pairs of entity attributes by entity name.

        :param name: Entity name
        :return: The dict of entity attributes (name and type) or None
        """
        SchemaManager._get_data()
        return {attr['name']: attr['type']
                for attr in SchemaManager.get_attributes_by_entity(name)}

    @staticmethod
    def get_primary_key_by_entity(name):
        """
        Gets a primary key to specific entity
        :param name: Entity name
        :return: The name of the primary key or None
        """
        SchemaManager._get_data()
        ent = SchemaManager.get_entity_by_name(name)
        pk_list = ent['primary_key']
        pk_str = ''
        for pk in pk_list:
            pk_str += pk_str + pk['name']
        return pk_str

    @staticmethod
    def get_foreign_keys_by_entity(name):
        """
        Gets a foreing keys to specific entity
        :param name: Entity name
        :return: List with the foreing keys or None
        """
        SchemaManager._get_data()
        ent = SchemaManager.get_entity_by_name(name)
        return ent['foreign_key']

    @staticmethod
    def get_entity_db(name):
        """
        Get the persistence model used on the entity
        :param name: Entity name
        :return: BLOCKCHAIN or RELATIONAL_DATABASE
        """
        SchemaManager._get_data()
        ent = SchemaManager.get_entity_by_name(name)

        if str(ent['persistence']).lower() == 'blockchain':
            return BLOCKCHAIN
        elif str(ent['persistence']).lower() == 'relational_database':
            return RELATIONAL_DATABASE
        else:
            raise Exception(
                'Persistence Model Unrecognized for entity \'{}\''.format(name)
            )
