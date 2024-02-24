# import unittest
# from moon.mapper.sql_analyzer import QueryAnalyzer
# from moon.mapper.mapper import Mapper, SchemaManager
# from moon.mapper.persistence_model import BLOCKCHAIN
# from moon.communication.query_type import DELETE, INSERT, SELECT, UPDATE


# class QueryAnalyzerTest(unittest.TestCase):
#     def test(self):
#         self._test_mapper()
#         self._test_schema_manager()
#         self._test_sql_analyzer()

#     def _test_mapper(self):
#         sql_insert = "INSERT INTO table_name (col1,col2) VALUES (val1,val2)"
#         exp_res = {
#             "entity": "table_name",
#             "col1": "val1",
#             "col2": "val2"
#         }
#         rec_res = Mapper.sql_insert_to_bc(
#             sql_insert,
#             "table_name"
#         )
#         self.assertDictEqual(exp_res, rec_res[0])

#         sql_update = "UPDATE table_name SET a = 1, b = 2 WHERE c = 2"
#         exp_res = ({
#             'entity': 'table_name',
#             'a': '1',
#             'b': '2'
#         }, 'select * from table_name where c = 2')
#         rec_res = Mapper.sql_update_to_bc(sql_update)
#         self.assertTupleEqual(exp_res, rec_res)

#         sql_update = "UPDATE table_name SET a = 1, b = 2 WHERE c = 2"
#         exp_res = ({
#             'entity': 'table_name',
#             'a': '1',
#             'b': '2'
#         }, 'select * from table_name where c = 2')
#         rec_res = Mapper.sql_update_to_bc(sql_update)
#         self.assertTupleEqual(exp_res, rec_res)

#         rec_res = Mapper.generate_sql_create_temp_table(
#             "entity_a"
#         )
#         exp_res = "create temp table entity_a '\
#             '(id_a integer,label_a_1 varchar,'\
#                 'label_a_2 integer,label_a_3 varchar)"
#         self.assertEqual(exp_res, rec_res)

#         rec_res = Mapper.generate_sql_insert_temp_table(
#             "entity_a"
#         )
#         exp_res = "INSERT INTO entity_a (id_a,label_a_1,label_a_2,label_a_3) '\
#             'VALUES ("
#         self.assertEqual(exp_res, rec_res)

#     def _test_schema_manager(self):
#         entity_list = SchemaManager.get_entity_list()
#         attributes_entity = SchemaManager.get_attributes_by_entity(
#             entity_list[0]
#         )
#         persistence_model = SchemaManager.get_persistence_by_entity(
#             entity_list[0]
#         )
#         entity = SchemaManager.get_entity_by_name(entity_list[0])
#         fk = SchemaManager.get_foreign_keys_by_entity(entity_list[0])
#         pk = SchemaManager.get_primary_key_by_entity(entity_list[0])
#         self.assertEqual(entity_list, ['entity_a', 'entity_b'])
#         self.assertEqual(attributes_entity, [
#             {
#                 'name': 'id_a',
#                 'type': 'integer'
#             },
#             {
#                 'name': 'label_a_1',
#                 'type': 'varchar'
#             },
#             {
#                 'name': 'label_a_2',
#                 'type': 'integer'
#             },
#             {
#                 'name': 'label_a_3',
#                 'type': 'varchar'
#             }
#         ])
#         self.assertEqual(persistence_model, BLOCKCHAIN)
#         self.assertEqual(entity, {
#             'attributes':
#                 [
#                     {
#                         'name': 'id_a',
#                         'type': 'integer'
#                     },
#                     {
#                         'name': 'label_a_1',
#                         'type': 'varchar'
#                     },
#                     {
#                         'name': 'label_a_2',
#                         'type': 'integer'
#                     },
#                     {
#                         'name': 'label_a_3',
#                         'type': 'varchar'
#                     }
#                 ],
#             'foreign_key':
#                 [
#                     {
#                         'ext_ref_name': 'entity_name',
#                         'fields': 'id',
#                         'name': 'label_a_1'
#                     }
#                 ],
#             'name': 'entity_a',
#             'persistence': 'blockchain',
#             'primary_key':
#                 [
#                     {
#                         'name': 'id_a'
#                     }
#                 ]
#             }
#         )
#         self.assertEqual(fk, [
#             {
#                 'ext_ref_name': 'entity_name',
#                 'fields': 'id',
#                 'name': 'label_a_1'
#             }
#         ])
#         self.assertEqual(pk, [{'name': 'id_a'}])

#     def _test_sql_analyzer(self):
#         sql_select_without_join = "SELECT * FROM table_name WHERE b = 2"
#         sql_select_join = "SELECT * FROM table1 NATURAL JOIN table2"
#         sql_insert = "INSERT INTO table_name (col1, col2, col3) '\
#             'VALUES (1, 2, 3)"
#         sql_update = "UPDATE table_name SET a = 1, b = 2 WHERE c = 3"
#         sql_delete = "DELETE * FROM table_name WHERE a = 2"
#         type_select = QueryAnalyzer.get_type_query(sql_select_without_join)
#         self.assertEqual(type_select, SELECT)
#         type_insert = QueryAnalyzer.get_type_query(sql_insert)
#         self.assertEqual(type_insert, INSERT)
#         type_update = QueryAnalyzer.get_type_query(sql_update)
#         self.assertEqual(type_update, UPDATE)
#         type_delete = QueryAnalyzer.get_type_query(sql_delete)
#         self.assertEqual(type_delete, DELETE)
#         entities = QueryAnalyzer.get_involved_entities(INSERT, sql_insert)
#         self.assertEqual(entities, ['table_name'])
#         has_join = QueryAnalyzer.is_join(sql_select_join)
#         self.assertEqual(has_join, True)
#         hasnt_join = QueryAnalyzer.is_join(sql_select_without_join)
#         self.assertEqual(hasnt_join, False)
#         has_conditional = QueryAnalyzer.has_conditional(
#             sql_select_without_join
#         )
#         self.assertEqual(has_conditional, True)


# if __name__ == "__main__":  # pragma: no cover
#     t = QueryAnalyzerTest()
#     t.test()
