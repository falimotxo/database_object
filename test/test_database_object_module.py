import unittest

from src.data_model import DatabaseObject
from src.database_object_module import DatabaseObjectModule


class DatabaseObjectTest(DatabaseObject):
    def __init__(self, id: str = None) -> None:
        super().__init__(id)
        self.attr1 = 'attr1'
        self.attr2 = 'attr2'
        self.attr3 = 'attr3'


class TestDatabaseObjectModule(unittest.TestCase):

    def setUp(self):
        print('In setUp()')
        self.module = DatabaseObjectModule()

    def tearDown(self):
        print('In tearDown()')

    def test_1_put(self):
        print('Testing ' + self.__class__.__name__ + ' put')
        data = DatabaseObjectTest()
        database_object_result = self.module.put_object('TEST', 'DatabaseObjectTest', data)

        self.assertEqual(True, True)

    def test_2_get(self):
        print('Testing ' + self.__class__.__name__ + ' get')
        database_object_result = self.module.get('TEST', 'DatabaseObjectTest')

        self.assertEqual(True, True)



if __name__ == '__main__':
    unittest.main()