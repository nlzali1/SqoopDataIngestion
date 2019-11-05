import logging
from unixCommand import UnixCommand
from mapdatatypes import *

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)


def create_database(conf):
	cmd = ['hive', '-e', '"CREATE DATABASE IF NOT EXISTS {0}"'.format(conf["targetDBName"])]
	unix_command = UnixCommand()
	(ret, out, err) = unix_command.run_unix_cmd(cmd)
	print(ret, out, err)
	if ret == 0:
		logging.info('Success.')
	else:
		logging.info('Error.')
		return False


def alter_table_location(table,db,location,hdfspath):
	cmd = ['hive', '-e', '"alter table {2}.{0} SET LOCATION \'{4}{1}/{3}\';"'.format(table,location,db,table.upper(),hdfspath)]
	unix_command = UnixCommand()
	(ret, out, err) = unix_command.run_unix_cmd(cmd)
	print(ret, out, err)
	if ret == 0:
			logging.info('Success.')
	else:
			logging.info('Error.')
			return False


def create_external_avsc_tables(conf, tableName):
	cmd = ['hive', '-e', '"CREATE EXTERNAL TABLE {0}.{1} ROW FORMAT SERDE \'org.apache.hadoop.hive.serde2.avro.AvroSerDe\' '
						 'STORED AS INPUTFORMAT \'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat\' '
						 'OUTPUTFORMAT \'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat\' LOCATION \'{2}{3}/{4}\' '
						 'TBLPROPERTIES (\'avro.schema.url\'=\'{5}{6}/{7}/{8}_{9}.avsc\')"'.format(conf["targetDBName"],
																								   tableName,
																								   conf["hdfsAddress"],
																								   conf["targetDirectory"],
																								   tableName,
																								   conf["hdfsAddress"],
																								   conf["avscDir"],
																								   conf["targetDBName"],
																								   conf["subDBName"],
																								   tableName)]
	unix_command = UnixCommand()
	(ret, out, err) = unix_command.run_unix_cmd(cmd)
	print(ret, out, err)
	if ret == 0:
		logging.info('Success.')
	else:
		logging.info('Error.')
		return False
	return True


def create_external_orc_table_from_sqlserver(conf, tableName):
	cmd =['hive', '-e', '"{0}"'.format(createHiveQueryFromSQLServer(conf, tableName))]
	unix_command = UnixCommand()
	(ret, out, err) = unix_command.run_unix_cmd(cmd)
	print(ret, out, err)
	if ret == 0:
		logging.info('Success.')
	else:
		logging.info('Error.')
		return False
	return True


def create_external_orc_table_from_mysql(conf, tableName):
	cmd =['hive', '-e', '"{0}"'.format(createHiveQueryFromMySQL(conf, tableName))]
	unix_command = UnixCommand()
	(ret, out, err) = unix_command.run_unix_cmd(cmd)
	print(ret, out, err)
	if ret == 0:
		logging.info('Success.')
	else:
		logging.info('Error.')
		return False
	return True


def create_external_orc_table(conf, tableName):
	cmd =['hive', '-e','"{0}"'.format(createHiveTableQuery(conf, tableName))]
	unix_command = UnixCommand()
	(ret, out, err) = unix_command.run_unix_cmd(cmd)
	print(ret, out, err)
	if ret == 0:
		logging.info('Success.')
	else:
		logging.info('Error.')
		return False
	return True

import logging
from unixCommand import UnixCommand
from mapdatatypes import *

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)


def create_database(conf):
	cmd = ['hive', '-e', '"CREATE DATABASE IF NOT EXISTS {0}"'.format(conf["targetDBName"])]
	unix_command = UnixCommand()
	(ret, out, err) = unix_command.run_unix_cmd(cmd)
	print(ret, out, err)
	if ret == 0:
		logging.info('Success.')
	else:
		logging.info('Error.')
		return False


def alter_table_location(table,db,location,hdfspath):
	cmd = ['hive', '-e', '"alter table {2}.{0} SET LOCATION \'{4}{1}/{3}\';"'.format(table,location,db,table.upper(),hdfspath)]
	unix_command = UnixCommand()
	(ret, out, err) = unix_command.run_unix_cmd(cmd)
	print(ret, out, err)
	if ret == 0:
			logging.info('Success.')
	else:
			logging.info('Error.')
			return False


def create_external_avsc_tables(conf, tableName):
	cmd = ['hive', '-e', '"CREATE EXTERNAL TABLE {0}.{1} ROW FORMAT SERDE \'org.apache.hadoop.hive.serde2.avro.AvroSerDe\' '
						 'STORED AS INPUTFORMAT \'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat\' '
						 'OUTPUTFORMAT \'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat\' LOCATION \'{2}{3}/{4}\' '
						 'TBLPROPERTIES (\'avro.schema.url\'=\'{5}{6}/{7}/{8}_{9}.avsc\')"'.format(conf["targetDBName"],
																								   tableName,
																								   conf["hdfsAddress"],
																								   conf["targetDirectory"],
																								   tableName,
																								   conf["hdfsAddress"],
																								   conf["avscDir"],
																								   conf["targetDBName"],
																								   conf["subDBName"],
																								   tableName)]
	unix_command = UnixCommand()
	(ret, out, err) = unix_command.run_unix_cmd(cmd)
	print(ret, out, err)
	if ret == 0:
		logging.info('Success.')
	else:
		logging.info('Error.')
		return False
	return True


def create_external_orc_table_from_sqlserver(conf, tableName):
	cmd =['hive', '-e', '"{0}"'.format(createHiveQueryFromSQLServer(conf, tableName))]
	unix_command = UnixCommand()
	(ret, out, err) = unix_command.run_unix_cmd(cmd)
	print(ret, out, err)
	if ret == 0:
		logging.info('Success.')
	else:
		logging.info('Error.')
		return False
	return True


def create_external_orc_table_from_mysql(conf, tableName):
	cmd =['hive', '-e', '"{0}"'.format(createHiveQueryFromMySQL(conf, tableName))]
	unix_command = UnixCommand()
	(ret, out, err) = unix_command.run_unix_cmd(cmd)
	print(ret, out, err)
	if ret == 0:
		logging.info('Success.')
	else:
		logging.info('Error.')
		return False
	return True


def create_external_orc_table(conf, tableName):
	cmd =['hive', '-e','"{0}"'.format(createHiveTableQuery(conf, tableName))]
	unix_command = UnixCommand()
	(ret, out, err) = unix_command.run_unix_cmd(cmd)
	print(ret, out, err)
	if ret == 0:
		logging.info('Success.')
	else:
		logging.info('Error.')
		return False
	return True


