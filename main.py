import datetime
import argparse
from hdfs import *
from hive import *
from readConfig import ReadConfiguration
from sqoop import Sqoop

config_file_name="config.json"

readconfig = ReadConfiguration()


logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)


def main():
	conf = readconfig.read_json(config_file_name)
	parser = argparse.ArgumentParser()
	
	parser.add_argument("-idt","--importdbtype", help="Use --importdbtype to specify the database type such as ORACLE, SQLServer etc to import with the configurations set in configtest.json")
	parser.add_argument("-edt","--exportdbtype", help="Use --exportdbtype to specify the database type such as ORACLE, SQLServer etc to export with the configurations set in configtest.json")
	parser.add_argument("-atl","--altertableslocations", help="Use --altertableslocations to alter the locations of hive tables in hive metastore with the configurations in configtest.json", action="store_true")
	args = parser.parse_args()
	ts = datetime.datetime.now()
	print("Start time: {0}".format(ts))

	if args.importdbtype and args.importdbtype.lower() == "oracle":
		print("Import Oracle")
		load_data_from_oracle(conf)
	elif args.exportdbtype and args.exportdbtype.lower() == "oracle":
		print("Export Oracle")
		export_data_to_oracle(conf)
	elif args.importdbtype and args.importdbtype.lower() == "sqlserver":
		print("Import Sql Server")
		load_data_from_sqlserver(conf)
	elif args.importdbtype and args.importdbtype.lower() == "mysql":
		print("Import MySQL")
		load_data_from_mysql(conf)
	elif args.altertableslocations:
		print("Alter table locations")
		alter_table(conf)

	tf = datetime.datetime.now()
	print("End time: {0}".format(tf))
	totalTimeElapsed = tf - ts
	print("Total time elapsed in seconds: {0}".format(totalTimeElapsed.total_seconds()))


def alter_table(conf):
	for table in conf["Tables"]:
		alter_table_location(table, conf["Configs"]["targetDBName"],conf["Configs"]["targetDirectory"],conf["Configs"]["hdfsAddress"])


def export_data_to_oracle(conf):
	create_database(conf["Configs"])

	for table in conf["Tables"]:
		sqoop_obj = Sqoop(conf["Configs"])
		if sqoop_obj.run_sqoop_export_job(table):
			continue
		else:
			return


def load_data_from_oracle(conf):
	create_database(conf["Configs"])

	for table in conf["Tables"]:
		if create_external_orc_table(conf["Configs"], table):
			sqoop_obj = Sqoop(conf["Configs"])
			if sqoop_obj.run_sqoop_import_orc_job(table):
				continue
			else:
				return
		else:
			return


def load_data_from_mysql(conf):
	create_database(conf["Configs"])

	for table in conf["Tables"]:
		if create_external_orc_table_from_mysql(conf["Configs"], table):
			sqoop_obj = Sqoop(conf["Configs"])
			if sqoop_obj.run_sqoop_import_mysql_job(table):
				continue
			else:
				return
		else:
			return


def load_data_from_sqlserver(conf):
	create_database(conf["Configs"])

	for table in conf["Tables"]:
		if create_external_orc_table_from_sqlserver(conf["Configs"], table):
			sqoop_obj = Sqoop(conf["Configs"])
			if sqoop_obj.run_sqoop_import_sqlserver_job(table):
				continue
			else:
				return
		else:
			return


def load_data_as_avsc(conf):
	if not check_avsc_schema_dir_exists(conf["Configs"]):
		if not create_avsc_schema_dir(conf["Configs"]):
			return

	create_database(conf["Configs"])

	for table in conf["Tables"]:
		sqoop_obj = Sqoop(conf["Configs"])
		if sqoop_obj.run_sqoop_import_avro_job(table):
			if put_avsc_hdfs(conf["Configs"], table):
				if not create_external_avsc_tables(conf["Configs"], table):
					return
			else:
				return
		else:
			return


if __name__ == "__main__":
	main()



