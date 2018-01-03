import unittest

from src.data_model import DatabaseObject
from src.database_object_module import DatabaseObjectModule


class DatabaseObjectTest(DatabaseObject):

    def __init__(self, _id: str = '') -> None:
        super(DatabaseObjectTest, self).__init__(_id)
        self.int_arg = int(5)
        self.bool_arg = bool(False)
        self.str_arg = str('cadena de texto')
        self.list_arg = ['una cosa', 'otra cosa']
        self.float_arg = float(7.9)
        self.dict_arg = {'key1': 'value1', 'ke2': 'value2'}


class TestDatabaseObjectModule(unittest.TestCase):

    def setUp(self) -> None:
        print(' In setUp() ')
        self.module = DatabaseObjectModule()

    def tearDown(self) -> None:
        print(' In tearDown() ')
        print('---------------------')

    def test_1_put_1(self) -> None:
        print(' Testing ' + self.__class__.__name__ + ' put 1 -> ')
        data = DatabaseObjectTest('hola mundo')
        database_object_result1 = self.module.put_object('TEST', 'DatabaseObjectTest', data)
        print(database_object_result1.data)

        self.assertEqual(True, True)

    def test_2_put_2(self) -> None:
        print(' Testing ' + self.__class__.__name__ + ' put 2 -> ')
        for x in range(10000):
            data_for = DatabaseObjectTest(str(x))
            self.module.put_object('TEST', 'DatabaseObjectTest', data_for)

        self.assertEqual(True, True)

    def test_3_get_1(self) -> None:
        print(' Testing ' + self.__class__.__name__ + ' get 1 -> ')
        database_object_result3 = self.module.get('TEST', 'DatabaseObjectTest')
        print(len(database_object_result3.data) / 190)

        self.assertEqual(True, True)

    def test_4_get_2(self) -> None:
        print(' Testing ' + self.__class__.__name__ + ' get 2 -> ')
        database_object_result4 = self.module.get('TEST', 'DatabaseObjectTest', condition=('_id', '=', 'hola mundo'))
        print(database_object_result4.data)

        self.assertEqual(True, True)

    def test_5_update_1(self) -> None:
        print(' Testing ' + self.__class__.__name__ + ' update 1 -> ')
        data_update1 = DatabaseObjectTest()
        data_update1.list_arg.append('Un tercer objecto')
        database_object_result5 = self.module.update_object('TEST', 'DatabaseObjectTest', data_update1,
                                                            condition=('list_arg', '=', ['una cosa', 'otra cosa']))
        print(database_object_result5.data)

        self.assertEqual(True, True)

    def test_6_get_3(self) -> None:
        print(' Testing ' + self.__class__.__name__ + ' get 3 -> ')
        database_object_result6 = self.module.get('TEST', 'DatabaseObjectTest', condition=('_id', '<=', '2'))
        print(database_object_result6.data)

    def test_7_remove_1(self) -> None:
        print(' Testing ' + self.__class__.__name__ + ' remove 1 -> ')
        database_object_result7 = self.module.remove('TEST', 'DatabaseObjectTest')
        print(database_object_result7.data)

        self.assertEqual(True, True)

    def test_8_get_4(self) -> None:
        print(' Testing ' + self.__class__.__name__ + ' get 4 -> ')
        database_object_result8 = self.module.get('TEST', 'DatabaseObjectTest')
        print(database_object_result8.data)


if __name__ == '__main__':
    unittest.main()
