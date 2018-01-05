from database_object_module.data_model import DatabaseObject
from database_object_module.database_object_module import DatabaseObjectModule

from nose.tools import assert_equal, assert_true, assert_false, raises


class DatabaseObjectTest_1(DatabaseObject):

    def __init__(self) -> None:
        DatabaseObject.__init__(self)
        self.value = 1

class DatabaseObjectTest_2(DatabaseObject):

    def __init__(self) -> None:
        DatabaseObject.__init__(self)
        self.int_arg = int(5)
        self.bool_arg = bool(False)
        self.str_arg = str('cadena de texto')
        self.list_arg = ['one thing', 'another thing']
        self.float_arg = float(7.9)
        self.dict_arg = {'key1': 'value1', 'key2': 'value2'}


class TestDatabaseObjectModule(object):

    @classmethod
    def setup_class(cls):
        """This method is run once for each class before any tests are run"""

    @classmethod
    def teardown_class(cls):
        """This method is run once for each class _after_ all tests are run"""

    def setup(self):
        """This method is run once before _each_ test method is executed"""
        self.module = DatabaseObjectModule()

    def teardown(self):
        """This method is run once after _each_ test method is executed"""

    def test_put_1(self) -> None:
        data = DatabaseObjectTest_1()
        schema = 'TEST'
        object_name = data.__class__.__name__
        result_1 = self.module.put_object(schema, object_name, data)

        id_1 = result_1.data['_id']
        result_2 = self.module.get(schema, object_name, [('_id', '=', id_1)])
        id_2 = result_2.data['_id']
        # print(database_object_result1.data)

        assert_equal(id_1, id_2)

        # self.assertEqual(id_1, id_2)


#     def test_put_2(self) -> None:
#         print(' Testing ' + self.__class__.__name__ + ' put 2 -> ')
#         for x in range(10000):
#             data_for = DatabaseObjectTest(str(x))
#             self.module.put_object('TEST', 'DatabaseObjectTest', data_for)
#
#         self.assertEqual(True, True)
#
#     def test_3_get_1(self) -> None:
#         print(' Testing ' + self.__class__.__name__ + ' get 1 -> ')
#         database_object_result3 = self.module.get('TEST', 'DatabaseObjectTest')
#         print(len(database_object_result3.data) / 190)
#
#         self.assertEqual(True, True)
#
#     def test_4_get_2(self) -> None:
#         print(' Testing ' + self.__class__.__name__ + ' get 2 -> ')
#         database_object_result4 = self.module.get('TEST', 'DatabaseObjectTest', condition=('_id', '=', 'hola mundo'))
#         print(database_object_result4.data)
#
#         self.assertEqual(True, True)
#
#     def test_5_update_1(self) -> None:
#         print(' Testing ' + self.__class__.__name__ + ' update 1 -> ')
#         data_update1 = DatabaseObjectTest()
#         data_update1.list_arg.append('Un tercer objecto')
#         database_object_result5 = self.module.update_object('TEST', 'DatabaseObjectTest', data_update1,
#                                                             condition=('list_arg', '=', ['una cosa', 'otra cosa']))
#         print(database_object_result5.data)
#
#         self.assertEqual(True, True)
#
#     def test_6_get_3(self) -> None:
#         print(' Testing ' + self.__class__.__name__ + ' get 3 -> ')
#         database_object_result6 = self.module.get('TEST', 'DatabaseObjectTest', condition=('_id', '<=', '2'))
#         print(database_object_result6.data)
#
#     def test_7_remove_1(self) -> None:
#         print(' Testing ' + self.__class__.__name__ + ' remove 1 -> ')
#         database_object_result7 = self.module.remove('TEST', 'DatabaseObjectTest')
#         print(database_object_result7.data)
#
#         self.assertEqual(True, True)
#
#     def test_8_get_4(self) -> None:
#         print(' Testing ' + self.__class__.__name__ + ' get 4 -> ')
#         database_object_result8 = self.module.get('TEST', 'DatabaseObjectTest')
#         print(database_object_result8.data)
#
#
# if __name__ == '__main__':
#     unittest.main()
