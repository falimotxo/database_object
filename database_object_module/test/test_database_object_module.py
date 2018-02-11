from nose.tools import assert_equal, assert_true

from database_object_module.data_model import DatabaseObject, DatabaseObjectResult
from database_object_module.database_object_module import DatabaseObjectModule


class DatabaseObjectTest1(DatabaseObject):

    def __init__(self) -> None:
        DatabaseObject.__init__(self)
        self.value = 1

    def __repr__(self):
        return str(self.__dict__)

    def __str__(self):
        return str(self.__dict__)


class DatabaseObjectTest2(DatabaseObject):

    def __init__(self) -> None:
        DatabaseObject.__init__(self)
        self.int_arg = int(5)
        self.bool_arg = bool(False)
        self.str_arg = str('cadena de texto')
        self.list_arg = ['one thing', 'another thing']
        self.float_arg = float(7.9)
        self.dict_arg = {'key1': 'value1', 'key2': 'value2'}

    def __repr__(self):
        return str(self.__dict__)

    def __str__(self):
        return str(self.__dict__)


class DatabaseObjectTestError(object):

    def __init__(self) -> None:
        self.value = 1

    def __repr__(self):
        return str(self.__dict__)


class TestDatabaseObjectModule(object):

    module = None

    def __init__(self):
        pass


    @classmethod
    def setup_class(cls):
        """
        This method is run once for each class before any tests are run
        """
        TestDatabaseObjectModule.module = DatabaseObjectModule()

    @classmethod
    def teardown_class(cls):
        """
        This method is run once for each class _after_ all tests are run
        """
        TestDatabaseObjectModule.module.exit()

    def setup(self):
        """
        This method is run once before _each_ test method is executed
        """

        data_1 = DatabaseObjectTest1()
        data_2 = DatabaseObjectTest2()
        schema = 'TEST'
        object_name_1 = data_1.__class__.__name__
        object_name_2 = data_2.__class__.__name__
        self.module.remove(schema, object_name_1)
        self.module.remove(schema, object_name_2)
        print('Deleted all data')

    def teardown(self):
        """
        This method is run once after _each_ test method is executed
        """

    def test_put_get(self) -> None:
        """
        Insercion de objeto y recuperacion por id para verificar que se inserto correctamente
        """

        data = DatabaseObjectTest2()
        schema = 'TEST'
        object_name = data.__class__.__name__

        result_put = self.module.put_object(schema, object_name, data)
        inst_put = result_put.get_object_from_data()
        id_put = inst_put[0].get_id()

        result_get = self.module.get(schema, object_name, [('_id', '=', id_put)])
        inst_get = result_get.get_object_from_data(DatabaseObjectTest2())
        id_get = inst_get[0].get_id()

        assert_equal(id_put, id_get)

    def test_put_error(self) -> None:
        """
        Insercion de objeto y fallo al insertar
        """

        data = DatabaseObjectTestError()
        schema = 'TEST'
        object_name = data.__class__.__name__

        result = self.module.put_object(schema, object_name, data)
        assert_equal(result.code, DatabaseObjectResult.CODE_KO)

    def test_get_1(self) -> None:
        """
        Insercion de objeto y recuperacion fallida
        """

        data = DatabaseObjectTest2()
        schema = 'TEST'
        object_name = data.__class__.__name__

        self.module.put_object(schema, object_name, data)

        result_module = self.module.get(schema, object_name, [('int_arg', '=', 'wrong_value')])
        result = result_module.get_object_from_data(DatabaseObjectTest2())

        assert_true(len(result) == 0)

    def test_get_2(self) -> None:
        """
        Insercion de objeto y recuperacion por atributo entero
        """

        data = DatabaseObjectTest2()
        schema = 'TEST'
        object_name = data.__class__.__name__

        self.module.put_object(schema, object_name, data)

        result_get = self.module.get(schema, object_name, [('int_arg', '=', 5)])
        result = result_get.get_object_from_data(DatabaseObjectTest2())

        assert_equal(result[0].int_arg, data.int_arg)

    def test_get_3(self) -> None:
        """
        Insercion de objeto y recuperacion por atributo bool
        """

        data = DatabaseObjectTest2()
        schema = 'TEST'
        object_name = data.__class__.__name__

        result_put = self.module.put_object(schema, object_name, data)
        inst_put = result_put.get_object_from_data()
        id_put = inst_put[0].get_id()

        result_get = self.module.get(schema, object_name, [('bool_arg', '=', False)])
        inst_get = result_get.get_object_from_data()
        id_get = inst_get[0].get_id()

        assert_equal(id_put, id_get)

    def test_get_4(self) -> None:
        """
        Insercion de objeto y recuperacion por atributo string
        """

        data = DatabaseObjectTest2()
        schema = 'TEST'
        object_name = data.__class__.__name__

        result_put = self.module.put_object(schema, object_name, data)
        inst_put = result_put.get_object_from_data()
        id_put = inst_put[0].get_id()

        result_get = self.module.get(schema, object_name, [('str_arg', '=', 'cadena de texto')])
        inst_get = result_get.get_object_from_data(DatabaseObjectTest2())
        id_get = inst_get[0].get_id()

        assert_equal(id_put, id_get)

    def test_get_5(self) -> None:
        """
        Insercion de objeto y recuperacion por atributo float
        """

        data = DatabaseObjectTest2()
        schema = 'TEST'
        object_name = data.__class__.__name__

        result_put = self.module.put_object(schema, object_name, data)
        inst_put = result_put.get_object_from_data()
        id_put = inst_put[0].get_id()

        result_get = self.module.get(schema, object_name, [('float_arg', '=', 7.9)])
        inst_get = result_get.get_object_from_data(DatabaseObjectTest2())
        id_get = inst_get[0].get_id()

        assert_equal(id_put, id_get)

    def test_get_6(self) -> None:
        """
        Insercion de objeto y recuperacion por atributo list
        """

        data = DatabaseObjectTest2()
        schema = 'TEST'
        object_name = data.__class__.__name__

        result_put = self.module.put_object(schema, object_name, data)
        inst_put = result_put.get_object_from_data()
        id_put = inst_put[0].get_id()

        result_get = self.module.get(schema, object_name, [('list_arg', '=', ['one thing', 'another thing'])])
        inst_get = result_get.get_object_from_data(DatabaseObjectTest2())
        id_get = inst_get[0].get_id()

        assert_equal(id_put, id_get)

    def test_get_7(self) -> None:
        """
        Insercion de objeto y recuperacion por atributos multiples
        """

        data = DatabaseObjectTest2()
        schema = 'TEST'
        object_name = data.__class__.__name__

        result_put = self.module.put_object(schema, object_name, data)
        inst_put = result_put.get_object_from_data()
        id_put = inst_put[0].get_id()

        result_get = self.module.get(schema, object_name, [('int_arg', '=', 5), ('bool_arg', '=', False),
                                                           ('str_arg', '=', 'cadena de texto')])
        inst_get = result_get.get_object_from_data(DatabaseObjectTest2())
        id_get = inst_get[0].get_id()

        assert_equal(id_put, id_get)

    def test_get_8(self) -> None:
        """
        Insercion de objeto y recuperacion por atributos multiples
        """

        data = DatabaseObjectTest2()
        schema = 'TEST'
        object_name = data.__class__.__name__

        self.module.put_object(schema, object_name, data)

        result_get = self.module.get(schema, object_name, [('int_arg', '=', 5), ('bool_arg', '=', False),
                                                           ('str_arg', '=', 'XXXcadena de texto')])
        inst_get = result_get.get_object_from_data(DatabaseObjectTest2())

        assert_true(len(inst_get) == 0)






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
