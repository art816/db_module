#!/usr/bin/env python3

# This is proprietary software.
# Part of cluster monitoring project.
# PEP8 codestyle used, python version 3.

# pylint: disable=star-args, protected-access, too-few-public-methods
# pylint: disable=no-member, no-value-for-parameter
# pylint: disable=too-many-instance-attributes

""" Sqlalchemy nms db manager. """

import time
import sys
import subprocess
import sqlalchemy as alch
import sqlalchemy.exc as sqlexc
from sqlalchemy import asc, desc, Table, Column, Integer, MetaData
from sqlalchemy import create_engine, select, between, Boolean, Text
from sqlalchemy.orm import sessionmaker, scoped_session
from decimal import Decimal
from sqlalchemy_utils import functions as sqlfunctions
from sqlalchemy.orm.session import Session

import nms.config as cfg
import nms.utility as utility
from nms.core.alerts import OldAlert, BASE


class SafeSession(Session):
    """ This type of sessino can be used in with statement and it guarantees
        that session will commit on exit or error.
    """
    def __enter__(self):
        """ On enter in with """
        return self

    def __exit__(self, _type, value, traceback):
        """ On exit of with """
        try:
            self.commit()
        except sqlexc.InvalidRequestError:
            utility.log_color(str(sys.exc_info()), 'red', 'error')
            self.rollback()
            utility.log_color('Rolling back', 'red', 'error')
        self.close()


class SessionManager(object):
    """ SessionManager goal is storing session and engine to database to be
        passed to DatabaseOperator classes.
        Manager can create or delete database.
        It is single point between database and any database user.
        Any changes in database can be committed from here.
    """
    def __init__(self, db_name):
        """ Raises ConnectionError if there is no such database. """
        self.name = db_name
        self.database_url = self.get_url(db_name)
        if not sqlfunctions.database_exists(self.database_url):
            raise ConnectionError("Can't connect to " + self.database_url)
        self.engine = create_engine(self.database_url,
                                    pool_size=cfg.POOL_SIZE,
                                    echo=cfg.DB_ECHO,
                                    echo_pool=cfg.DB_POOL_ECHO)
        session_factory = sessionmaker(bind=self.engine,
                                       class_=SafeSession)
        # Thread safe session maker
        self.sessionmaker = scoped_session(session_factory)

    @staticmethod
    def create_db(db_name):
        """ Create database by name if not exists. """
        try:
            sqlfunctions.create_database(SessionManager.get_url(db_name))
        except sqlexc.ProgrammingError:  # Db exists
            pass

    @staticmethod
    def drop_database(db_name):
        """ Drop database by name is exists. """
        sqlfunctions.drop_database(SessionManager.get_url(db_name))

    def finalize(self):
        """ Fix all changes to database explicitly. """
        self.sessionmaker.close_all()

    def delete(self):
        """ Drop current database. """
        self.finalize()
        sqlfunctions.drop_database(self.database_url)

    @staticmethod
    def get_url(db_name):
        """ Form url for db_name. """
        return '{}/{}?charset=utf8'.format(cfg.DB_PREFIX_NAME, db_name)


class DatabaseOperator(object):
    """ Database abstract class for other database instances. """

    def __init__(self, session_manager):
        """ Init METADATA and connect. """
        self.session_manager = session_manager
        self.name = session_manager.name
        self.engine = session_manager.engine
        self.sessionmaker = session_manager.sessionmaker
        self.metadata = MetaData(bind=self.engine)
        self.last_get_size_time = 0
        self.size_update_time = 60 * 60 * 3  # in seconds
        self.cached_db_size = None

    def delete_database(self):
        """ Delete all tables from database.  Clear matadata. """
        self.session_manager.finalize()
        self.engine.dispose()

    def delete_table(self, table):
        """ Clear table. """
        with self.sessionmaker() as session:
            session.execute(table.__table__.delete())

    def get_size(self):
        """ Return size of database in MB(type decimal). """
        if time.time() > self.last_get_size_time + self.size_update_time:
            query = ('SELECT * from '
                     '(SELECT table_schema db_name, '
                     'SUM( data_length + index_length ) / 1024 / 1024 '
                     ' "db_size_mb"'
                     ' FROM information_schema.TABLES group by table_schema) '
                     'as a1'
                     ' where a1.db_name="{}";').format(self.name)
            with self.sessionmaker() as session:
                responce = session.execute(query)
                size_of_database_in_mb = responce.first()[1]
            self.last_get_size_time = time.time()
            self.cached_db_size = size_of_database_in_mb
        else:
            size_of_database_in_mb = self.cached_db_size
        return size_of_database_in_mb

    def _insert_in_session(self, table_name, column_values_dict, session):
        """ Insert data in table for given session. """
        table = Table(table_name, self.metadata, autoload=True)
        try:
            session.execute(table.insert(), column_values_dict)
        except sqlexc.IntegrityError:  # Non unique field.
            session.execute(table.update().where(
                table.c.time == column_values_dict['time']).values(
                    **column_values_dict))

    def _insert(self, table_name, column_values_dict):
        """ Insert data in table. """
        table = Table(table_name, self.metadata, autoload=True)
        with self.sessionmaker() as session:
            try:
                session.execute(table.insert(), column_values_dict)
            except sqlexc.IntegrityError:  # Non unique field.
                session.execute(table.update().where(
                    table.c.time == column_values_dict['time']).values(
                        **column_values_dict))

    def alter_table(self, table_name, column_names, type_):
        """ Add columns to given table if table exits and dont have
            valid columns.
        """
        try:
            table = Table(table_name, self.metadata, autoload=True)
            existing_columns = list(table.columns)
            existing_column_names = [col.name for col in existing_columns]
            for dev in column_names:
                new_col = Column(dev, type_)
                if new_col.name not in existing_column_names:
                    self._add_column(table, new_col)
        except sqlexc.NoSuchTableError:
            pass
    
    def _add_column(self, table, column):
        """ Add column to existing database. """
        table_name = table.description
        column_name = column.compile(dialect=self.engine.dialect)
        column_type = column.type.compile(self.engine.dialect)
        self.engine.execute('ALTER TABLE %s ADD COLUMN %s %s' % (
            table_name, column_name, column_type))

    def __repr__(self):
        """ Print all tables with data. """
        return 'DBop: ' + self.name


class CollectedDataOperator(DatabaseOperator):
    """ Data operator to store all collected values of parameters.
        It stores values in tables with ParameterName and columns DeviceName.
        Each table entry has time of data collection in int format.
        +----------------------+
        |     Parameter 1      |
        +------+-------+-------+
        | time | Dev01 | Dev02 |
        +------+-------+-------+
        | 1230 |  0.0  |  3.5  |
        +------+-------+-------+
        | 1235 |  0.3  |  3.3  |
        +------+-------+-------+

        Most notable methods are:
            * save data        - to write data to database
            * get times values - to get values from database
    """

    def create_tables(self, devices):
        """ Call create table for existing devices and call create table
            online status.
        """
        all_parameters = {}
        for dev in devices.values():
            all_parameters.update(dev.params_dict)

        # create tables for parameters
        for param in all_parameters.values():
            list_of_devices = []
            for dev in devices.values():
                if param.name in dev.params_dict:
                    list_of_devices.append(dev)
            if list_of_devices:
                self.__create_table(param, list_of_devices)
        self.metadata.create_all(self.engine, checkfirst=True)
        self.metadata.reflect()

    def __create_table(self, parameter, list_of_devices):
        ''' Create one table in existing database.  Parameters is dict.  '''
        self.alter_table(parameter.name, [dev.name for dev in list_of_devices], cfg.DB_TYPES[parameter._type])
        columns = [Column('time', Integer, primary_key=True)]
        for dev in list_of_devices:
            columns.append(Column(dev.name, cfg.DB_TYPES[parameter._type]))
        Table(parameter.name, self.metadata, *columns, extend_existing=True)

    @staticmethod
    def get_data_to_save(cur_time, given_devices, do_time_check=False):
        """ Return dictionary, that contain values that will be inserted into
            database.
            If do_time_check = false, save all values, otherwise save only
            parameters, which last_collected == cur_time.
            For example: {'Param1': {'time': 1212, 'dev1': 0.2, 'dev2': 0.3}}
        """
        data_to_save = {}
        for device in given_devices:
            for param in device.params_dict.values():
                # Save data only for parameter, that has been
                # collected right now.
                if do_time_check and param.last_collected != cur_time and param.name != 'online':
                    continue
                # Create new table.
                if not param.name in data_to_save:
                    data_to_save[param.name] = {}
                    # add time column
                    data_to_save[param.name]['time'] = cur_time
                data_to_save[param.name][device.name] = param.value
        return data_to_save

    def save_data(self, cur_time, given_devices, do_time_check=False):
        """ Save data about davices in database.
            given_devices - object Device (type list, it is impotent)).
        """
        data_to_save = self.get_data_to_save(cur_time, given_devices,
                                             do_time_check)
        with self.sessionmaker() as session:
            for param_name in data_to_save:
                self._insert_in_session(
                    param_name, data_to_save[param_name], session)

    @staticmethod
    def get_query_according_to_times(table, device_name, begin_time, end_time):
        """ Return query with time column > begin time and < end_time begin
            and end set, or just all times otherwise.
        """
        param_column = [table.columns.time, table.columns[device_name]]
        query = select(param_column).order_by(desc(table.columns.time))
        time_col = table.columns['time']
        if begin_time and end_time:
            query = query.where(between(time_col, begin_time, end_time))
        elif begin_time:
            query = query.where(time_col >= begin_time)
        elif end_time:
            query = query.where(time_col <= end_time)
        return query

    def get_times_values(self, device_name, parameter_name, begin_time=None,
                         end_time=None):
        """ Return: list of pairs [time, value] of given parameter where
            begin_time <= time <= end_name in descending order of time.
        """

        table = Table(parameter_name, self.metadata, autoload=True)
        times_values = []
        query = self.get_query_according_to_times(table, device_name,
                                                  begin_time, end_time)

        with self.sessionmaker() as session:
            res = session.execute(query)
            for _time, value in res:
                if isinstance(value, Decimal):
                    value = float(value)
                times_values.append([_time, value])
        return times_values

    def query_device_parameter(self, device_name, parameter_name):
        """ Return list of tuples (time, value) where time is integer,
            value converted to float if possible.
            List sorted by time decreasing.
            Note: uses pure mysql, seems to be 5 times faster
            then get_last_data.
        """
        db_user = cfg.DB_USER
        db_password = cfg.DB_PASSWD
        db_name = self.session_manager.name
        query = ('SELECT {par}.time, {par}.{dev} '
                 'FROM {par} ORDER BY {par}.time DESC;').format(
                     par=parameter_name, dev=device_name)
        command = 'echo "{}" | mysql -u {} -p{} {}'.format(
            query, db_user, db_password, db_name)
        out = subprocess.check_output(command, shell=True)
        lines = out.decode('utf8')
        values = [[int(line.split()[0]), line.split()[1]] for line
                  in lines.split('\n')[1:] if line]
        for time_value in values:
            if time_value[1] == 'NULL':
                time_value[1] = None
                continue
            try:
                time_value[1] = float(time_value[1])
            except:
                pass
        return values

    def get_last_data(self, device_name, parameter_name, limit=1):
        """ Return times and values with given limit. """
        table = Table(parameter_name, self.metadata, autoload=True)
        param_column = [table.columns.time, table.columns[device_name]]
        query = select(param_column)
        query = query.order_by(desc(table.columns.time))
        query = query.limit(limit)
        times_values = []
        with self.sessionmaker() as session:
            res = session.execute(query)
            for _time, value in res:
                if isinstance(value, Decimal):
                    value = float(value)
                times_values.append([_time, value])
        return times_values

    def get_time_interval(self, table_name):
        """ Return time interval. """
        table = Table(table_name, self.metadata, autoload=True)
        with self.sessionmaker() as session:
            rows_with_data = session.execute(
                select([table.columns.time]).order_by(asc(table.columns.time)))
            row_list = [row for row in rows_with_data]
        return (row_list[0][0], row_list[-1][0])

    def print_tables(self):
        """ Debug function """
        print('-----')
        for table in self.metadata.tables:
            print(table)
            for col in self.metadata.tables[table].columns:
                print('\t', col)


class AlertDataOperator(DatabaseOperator):
    """ Data operator which manages Alert and OldAlert creation and deletions.
        All application logic stored in alerts.py AlertHandler.
        Alerts and OldAlerts contained in separate tables.
        +--------+    +-----------+
        | Alerts |    | OldAlerts |
        +--------+    +-----------+
    """

    def __init__(self, session_manager):
        super().__init__(session_manager)
        BASE.metadata.create_all(bind=self.engine)

    def add_alert(self, alert, alert_statistics=None):
        """ Crete new alert and return it. Change alert_statistics accordingly.
        """
        if not alert.level:
            return
        with self.sessionmaker() as session:
            session.add(alert)
            if alert_statistics:
                alert_statistics['new_alert'][0] += 1
                alert_statistics['new_alert'][1].append(alert.to_dict())

    def kill_alert(self, alert, level, alert_statistics=None):
        """ Delete alert from table if not level and
            change alert_statistics accordingly.
        """
        if level:
            return
        with self.sessionmaker() as session:
            session.delete(alert)
            if alert_statistics:
                alert_statistics['kill_alert'][0] += 1
                alert_statistics['kill_alert'][1].append(alert_statistics['update_alert'][1].pop())
                alert_statistics['update_alert'][0] -= 1

    def update_alert(self, alert, level, alert_statistics=None):
        """ Update alert and create oldalert in table with oldalerts.
            change alert_statistics accordingly.
        """
        if level == alert.level:
            return
        old_alert = OldAlert(alert)
        alert.level = level
        with self.sessionmaker() as session:
            session.add(old_alert)
            if alert_statistics:
                alert_statistics['update_alert'][0] += 1
                alert_statistics['update_alert'][1].append(old_alert.to_dict())
                alert_statistics['update_alert'][1][-1]['new_level'] = alert.level

    def get_entries(self, object_class, _filter=None):
        """ Select data from database.
            Return list of result of sqlalchemy query.
        """
        query_entries = []
        if not _filter:
            _filter = dict()

        with self.sessionmaker() as session:
            query = session.query(object_class)
            query = query.filter_by(**_filter)
            query = query.order_by(alch.desc(object_class.start_time))
            query_entries = list(query.all())
            session.expunge_all()
        return query_entries


class SystemDataOperator(DatabaseOperator):
    """ SystemDataOperator manages following tables:

        * Last online table, which is exactly online state of every
          GeneralDevice. It has exactly one row.
        +--------------------------+
        |        LastOnline        |
        +--------+--------+--------+
        | GDev01 | GDev02 | GDev03 |
        +--------+--------+--------+
        | 0      | 1      | 0      |
        +--------+--------+--------+

        * AdministrativeStatus table, to show if device is turned down
          administratively.  It has exactly one row. Possible values are
          "on"
          "off" - when device is administratively "off" we wont raise any
                  alerts about it down state

        +--------------------------+
        |  AdministrativeStatus    |
        +--------+--------+--------+
        | GDev01 | GDev02 | GDev03 |
        +--------+--------+--------+
        | on     | off    |  on    |
        +--------+--------+--------+
    """
    def create_tables(self, general_devices):
        """ Call create table for existing devices and call create table
            online status.
        """
        list_of_dev_names = [g_dev.name for g_dev in general_devices.values()]
        self._create_system_table(cfg.LAST_ONLINE_TABLE, list_of_dev_names,
                                  Boolean)
        self._create_system_table(cfg.ADMIN_STATUS_TABLE, list_of_dev_names,
                                  Text)
        admin_status = {gdev_name: "on" for gdev_name in general_devices}
        self.save_device_admin_status(admin_status);

    def save_device_admin_status(self, admin_status):
        """ Try selete and add new data to table. """
        table = Table(cfg.ADMIN_STATUS_TABLE, self.metadata, autoload=True)
        with self.sessionmaker() as session:
            session.commit()
            session.execute(table.delete())
            self._insert(cfg.ADMIN_STATUS_TABLE, admin_status)

    def _create_system_table(self, name, column_names, type_):
        """ Create special table for monitoring devices status. """
        columns = []
        self.alter_table(name, column_names, type_)
        for column_name in column_names:
            columns.append(Column(column_name, type_))
        Table(name, self.metadata, *columns, extend_existing=True)
        self.metadata.create_all(self.engine)

    def load_devices_online_status(self, general_devices):
        """ Set values of General devices according to table last_online."""
        last_online_table = Table(cfg.LAST_ONLINE_TABLE, self.metadata,
                                  autoload=True)
        for device_column in last_online_table.columns:
            device_name = device_column.name
            if device_name in general_devices.keys():
                columns = [last_online_table.columns[device_name]]
                with self.sessionmaker() as session:
                    is_online_responce = session.execute(select(columns))
                    general_devices[device_name].is_online =\
                    is_online_responce.first()[0]

    def save_devices_online_status(self, general_devices):
        """ Set values of table last_online. """
        column_values_dict = {}
        for g_dev in general_devices.values():
            column_values_dict[g_dev.name] = g_dev.is_online
        table = Table(cfg.LAST_ONLINE_TABLE, self.metadata, autoload=True)
        with self.sessionmaker() as session:
            session.commit()
            session.execute(table.delete())
            self._insert(cfg.LAST_ONLINE_TABLE, column_values_dict)

    def set_admin_status(self, device_name, status):
        """ By given general device name set its adm status. """
        # TODO: mock
        #return
        utility.log_color(
            'set adm status of {} to {}'.format(device_name, status),
            'yellow', 'warn')
        request = 'UPDATE {} SET {} = "{}";'.format(
            cfg.ADMIN_STATUS_TABLE, device_name, status)
        with self.sessionmaker() as session:
            session.execute(request)

    def get_admin_status(self, device_name):
        """ By given general device name get its adm status. """
        # TODO: tmp mock
        #return 'on'
        table = Table(cfg.ADMIN_STATUS_TABLE, self.metadata, autoload=True)
        columns = [table.columns[device_name]]
        with self.sessionmaker() as session:
            response = session.execute(select(columns))
            return response.first()[0]
