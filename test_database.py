#!/usr/bin/env python3

# This is proprietary software.
# part of cluster monitoring project.
# PEP8 code style used, python version 3

""" Test Suite for database module. """

import unittest
import random
import datetime
from sqlalchemy.exc import InternalError, ProgrammingError
from sqlalchemy_utils import functions as sqlfunctions

import nms.utility as utility
from nms.core.devices import Device
import nms.core.database as db
from nms.core.context import ContextCreator
import nms.config as cfg



class TestCollectorDB(unittest.TestCase):
    """ Check, if db work fine, by serialization/deserialization tests. """

    @classmethod
    def setUpClass(cls):
        """ Create database. """
        context = ContextCreator('buer').from_file()
        parameters_dict = context.parameters
        cls.TEST_PAR_DICT = {'CpuTemp2': parameters_dict['CpuTemp2']}
        cls.TEST_DEV_DICT = {'TestCollDBDev': Device('TestCollDBDev', '',
                                                     'snmp', ['CpuTemp2'],
                                                     parameters_dict)}
        cls.PAR_NAME = 'CpuTemp2'
        cls.device = cls.TEST_DEV_DICT['TestCollDBDev']
        cls.db_name = 'database_operator_data'
        db.SessionManager.create_db(cls.db_name)
        cls.session_manager = db.SessionManager(cls.db_name)
        cls.db_op = db.CollectedDataOperator(cls.session_manager)
        cls.db_op.create_tables(cls.TEST_DEV_DICT)
        cls.begin_t = utility.time_to_int(
            datetime.datetime(2005, 7, 14, 12, 30))

    @classmethod
    def tearDownClass(cls):
        """ Rm database. """
        cls.session_manager.delete()

    def test_get_time_interval(self):
        """ TestCollectorDB: Save 2 entries to database with 20 second step.
            Then check, if time interval equals 20 sec.
        """
        exp_interval = (self.begin_t, self.begin_t + 20)
        self.db_op.save_data(self.begin_t, [self.device])
        self.db_op.save_data(self.begin_t + 20, [self.device])
        self.assertEqual(self.db_op.get_time_interval(self.PAR_NAME),
                         exp_interval)

    def test_save_zero_value(self):
        """ TestCollectorDB: Save 0 to database as value. """
        self.device.params_dict[self.PAR_NAME].value = 0.0
        self.device.params_dict[self.PAR_NAME].last_collected = self.begin_t
        self.db_op.save_data(self.begin_t, [self.device])
        get_values = [value for _time, value in self.db_op.get_times_values(
            self.device.name, self.PAR_NAME)]
        self.assertTrue(0.0 in get_values)

    def test_get_size(self):
        """ TestCollectorDB: Test for get size of database. """
        self.assertTrue(self.db_op.get_size())

    def test__repr(self):
        """ TestCollectorDB: Test db operator representation. """
        self.assertTrue(self.db_op.__repr__())

    def test_create_database(self):
        """ TestCollectorDB: Create db. """
        table_names = list(self.db_op.metadata.tables.keys())
        self.assertEqual(table_names, [self.PAR_NAME])


class TestAlterCollectorDB(unittest.TestCase):
    """ TestAlterCollectorDB: test alert database operations.  """
    @classmethod
    def setUpClass(cls):
        """ Create database. """
        context = ContextCreator('buer').from_file()
        parameters_dict = context.parameters
        cls.parameters_dict = parameters_dict
        cls.TEST_PAR_DICT = {'CpuTemp2': parameters_dict['CpuTemp2']}
        cls.TEST_DEV_DICT = {'TestCollDBDev': Device('TestCollDBDev', '',
                                                     'snmp', ['CpuTemp2'],
                                                     parameters_dict)}
        cls.PAR_NAME = 'CpuTemp2'
        cls.device = cls.TEST_DEV_DICT['TestCollDBDev']
        cls.db_name = 'database_operator_data'
        db.SessionManager.create_db(cls.db_name)
        cls.session_manager = db.SessionManager(cls.db_name)
        cls.db_op = db.CollectedDataOperator(cls.session_manager)
        cls.db_op.create_tables(cls.TEST_DEV_DICT)
        cls.begin_t = utility.time_to_int(
            datetime.datetime(2005, 7, 14, 12, 30))

    @classmethod
    def tearDownClass(cls):
        """ Rm database. """
        cls.session_manager.delete()

    def test_add_device(self):
        """ TestAlterCollectorDB: test_add_device. """
        additional_dev = Device('TestCollDBDev2', '',
                                'snmp', ['CpuTemp1', 'CpuTemp2'],
                                self.parameters_dict)
        additional_dev.params_dict[self.PAR_NAME].value = 0.0
        additional_dev.params_dict[self.PAR_NAME].last_collected = self.begin_t
        dic = self.TEST_DEV_DICT
        dic.update({additional_dev.name: additional_dev})
        self.db_op.create_tables(dic)
        self.db_op.print_tables()
        self.db_op.save_data(self.begin_t, [additional_dev])


class TestGetData(unittest.TestCase):
    """ Tests Collected db op. """

    @classmethod
    def setUpClass(cls):
        """ Create database. """
        context = ContextCreator('buer').from_file()
        parameters_dict = context.parameters
        cls.TEST_PAR_DICT = {'CpuTemp1': parameters_dict['CpuTemp1']}
        cls.TEST_DEV_DICT = {'TestGetDataDev' : Device(
            'TestGetDataDev', '', 'snmp', ['CpuTemp1'], parameters_dict)}
        cls.device = cls.TEST_DEV_DICT['TestGetDataDev']
        cls.db_name = 'database_operator_data'
        db.SessionManager.create_db(cls.db_name)
        cls.session_manager = db.SessionManager(cls.db_name)
        cls.db_op = db.CollectedDataOperator(cls.session_manager)
        cls.db_op.create_tables(cls.TEST_DEV_DICT)

        cls.num_of_values = 10
        cls.time_interval = 20  # sec
        cls.PAR_NAME = 'CpuTemp1'

        cls.begin_t = utility.time_to_int(
            datetime.datetime(2005, 7, 14, 12, 30))
        cls.end_t = cls.begin_t + cls.num_of_values * cls.time_interval
        cls.half_t = int(cls.begin_t + (cls.end_t - cls.begin_t)/2 + 1)
        cls.control_data = []
        cls.fill_db_with_data(cls)

    @classmethod
    def tearDownClass(cls):
        """ Delete session. """
        cls.session_manager.delete()

    def fill_db_with_data(self):
        """ Create random data. """
        maximum = 20
        minimum = 50
        cur_time = self.begin_t
        for _ in range(self.num_of_values):
            cur_time += self.time_interval
            value = round(random.random()*(maximum - minimum) + minimum,
                          cfg.DEFAULT_SIGNIFICANT_DIGITS)
            self.control_data.append([cur_time, value])
            self.device.params_dict[self.PAR_NAME].value = value
            self.db_op.save_data(cur_time, [self.device])
        self.control_data.sort(reverse=True, key=lambda x: x[0])

    def test_get_values_all(self):
        """ TestCollectorDB: Get all data from dev in db. """
        get_data = self.db_op.get_times_values(
            self.device.name, self.device.params_dict[self.PAR_NAME].name,
            self.begin_t, self.end_t)
        self.assertEqual(get_data, self.control_data)

    def test_get_values_half(self):
        """ TestCollectorDB: Get first half of data from dev in db. """
        get_data = self.db_op.get_times_values(
            self.device.name, self.device.params_dict[self.PAR_NAME].name,
            self.begin_t, self.half_t)
        self.assertEqual(get_data, self.control_data[5:])

    def test_get_values_half_sec(self):
        """ TestCollectorDB: Get second half of data from dev in db. """
        get_data = self.db_op.get_times_values(
            self.device.name, self.device.params_dict[self.PAR_NAME].name,
            self.half_t, self.end_t)
        self.assertEqual(get_data, self.control_data[:5])


class TestSystemDatabase(unittest.TestCase):
    """ Test save online status. """

    @classmethod
    def setUpClass(cls):
        """ Create all databases. """
        cls.db_name = 'system_db_test'
        db.SessionManager.create_db(cls.db_name)
        cls.session_manager = db.SessionManager(cls.db_name)
        cls.system = db.SystemDataOperator(cls.session_manager)
        cls.context = ContextCreator('buer').from_file()
        cls.system.create_tables(cls.context.general_devices)

    @classmethod
    def tearDownClass(cls):
        """ Delete all databases. """
        cls.session_manager.delete()

    def test_device_online_status(self):
        """ TestSystemDatabase: Save and load last online status. """
        online_list = ['ControlServer', 'MonitoringServer', 'RAID01A']
        for g_dev in self.context.general_devices.values():
            g_dev.is_online = g_dev.name in online_list
        self.system.save_devices_online_status(self.context.general_devices)
        for g_dev in self.context.general_devices.values():
            g_dev.is_online = True
        self.system.load_devices_online_status(self.context.general_devices)
        for g_dev in self.context.general_devices.values():
            self.assertEqual(g_dev.is_online, g_dev.name in online_list)

    #@unittest.skip('Problems with databases in paralleled execution.')
    def test_administrative_status(self):
        """ TestSystemDatabase: Set and get adm status for one device. """
        self.assertEqual(
            self.system.get_admin_status('MonitoringServer'), 'on')
        self.system.set_admin_status('MonitoringServer', 'off')
        self.assertEqual(
            self.system.get_admin_status('MonitoringServer'), 'off')
        self.system.set_admin_status('MonitoringServer', 'on')
        self.assertEqual(
            self.system.get_admin_status('MonitoringServer'), 'on')




class TestSessionManager(unittest.TestCase):
    """ Test database creation, deletion. """
    DB_NAME = 'session_manager_db'
    DB_NOT_EXISTS_NAME = 'wrong_session_manager_db'
    DB_CREATED = 'session_created_manager_db'

    @classmethod
    def setUpClass(cls):
        try:
            db.SessionManager.create_db(cls.DB_NAME)
        except ProgrammingError:  # Db exists
            pass

    @classmethod
    def tearDownClass(cls):
        try:
            db.SessionManager.drop_database(cls.DB_CREATED)
        except InternalError:  # Db do not exists
            pass

    def test_creation(self):
        """ TestSessionManager: Must have engine and session. """
        smanager = db.SessionManager(self.DB_NAME)
        self.assertTrue(smanager.engine)
        self.assertTrue(smanager.sessionmaker)

    def test_db_not_exists(self):
        """ TestSessionManager: Raises error when connected to non existing db.
        """
        self.assertRaises(ConnectionError, db.SessionManager,
                          self.DB_NOT_EXISTS_NAME)

    def test_create_db(self):
        """ TestSessionManager: Can create and delete database. """
        url = db.SessionManager.get_url(self.DB_CREATED)
        self.assertFalse(sqlfunctions.database_exists(url))
        db.SessionManager.create_db(self.DB_CREATED)
        self.assertTrue(sqlfunctions.database_exists(url))
        smanager = db.SessionManager(self.DB_CREATED)
        smanager.delete()
        self.assertFalse(sqlfunctions.database_exists(url))

    def test_get_session(self):
        """ TestSessionManager: Get session object. """
        db.SessionManager.create_db(self.DB_CREATED)
        smanager = db.SessionManager(self.DB_CREATED)
        with smanager.sessionmaker() as session:
            session.commit()

