import logging
import os
from unixCommand import UnixCommand

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)

def check_avsc_schema_dir_exists(conf):
	cmd = ['hdfs', 'dfs', '-test', '-d', '{0}{1}'.format(conf["avscDir"], conf["targetDBName"])]
	unix_command = UnixCommand()
	(ret, out, err) = unix_command.run_unix_cmd(cmd)
	if ret == 0:
		return True

	return False


def create_avsc_schema_dir(conf):
	cmd = ['hdfs', 'dfs', '-mkdir', '{0}{1}'.format(conf["avscDir"], conf["targetDBName"])]
	unix_command = UnixCommand()
	(ret, out, err) = unix_command.run_unix_cmd(cmd)
	print(ret, out, err)
	if ret == 0:
		logging.info('Success.')
	else:
		logging.info('Error.')
		return False
	return True


def put_avsc_hdfs(conf, tableName):
	mypath = os.path.dirname(os.path.abspath(__file__))
	dbName = conf["subDBName"]
	outdir = conf["outdir"]
	cmd = ['hdfs', 'dfs', '-put', '{0}/{1}/{2}_{3}.avsc'.format(mypath, outdir, dbName, tableName), '{0}{1}'.format(conf["avscDir"],
																										conf["targetDBName"])]
	unix_command = UnixCommand()
	(ret, out, err) = unix_command.run_unix_cmd(cmd)
	print(ret, out, err)
	if ret == 0:
		logging.info('Success.')
	else:
		logging.info('Error.')
		return False
	return True

