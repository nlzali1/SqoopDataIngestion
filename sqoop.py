import logging
from mapdatatypes import *
from unixCommand import UnixCommand

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)


class Sqoop:

    def __init__(self, conf):
        self.dbServerAddress = conf["dbServerAddress"]
        self.dbServerPort = conf["dbServerPort"]
        self.dbServerAdd = conf["dbServerAdd"]
        self.dbName = conf["dbName"]
        self.dbUser = conf["dbUser"]
        self.dbPassword = conf["dbPassword"]
        self.dbSecurityProviderPath = conf["dbSecurityProviderPath"]
        self.dbPasswordAlias = conf["dbPasswordAlias"]
        self.targetDirectory = conf["targetDirectory"]
        self.subDBName = conf["subDBName"]
        self.outdir = conf["outdir"]
        self.targetDBName = conf["targetDBName"]
        self.conf = conf

    def sqoop_export_job(self, tableName):
        """
        Create Sqoop job
        """
        cmd = ['sqoop', 'export', '--connect', '{0}:{1}/{2}'.format(self.dbServerAddress, self.dbServerPort, self.dbName),
               '--username', self.dbUser, '--password', self.dbPassword, '--table', tableName, '--hcatalog-table',
               tableName, '--hcatalog-database', self.subDBName, '--batch', '--outdir', self.outdir]

        mappings = getoraclemappings(self.conf, tableName)

        if mappings is not None:
            cmd.append(mappings)
        return cmd

    def sqoop_import_avro_job(self, tableName):
        """
        Create Sqoop import avro job
        :param tableName:
        :return:
        """

        cmd = ['sqoop', 'import', '-Dmapreduce.job.classloader=true','-Dorg.apache.sqoop.splitter.allow_text_splitter=true', '-Dhadoop.security.credential.provider.path={0}'.format(self.dbSecurityProviderPath), '-Doraoop.chunk.method=PARTITION', '--connect',
               '{0}:{1}/{2}'.format(self.dbServerAddress, self.dbServerPort, self.dbName), '--username', self.dbUser,
               '--password-alias', self.dbPasswordAlias, '--table', '{0}.{1}'.format(self.subDBName, tableName), '--as-avrodatafile',
               '--target-dir={0}/{1}'.format(self.targetDirectory, tableName), '--autoreset-to-one-mapper', '--outdir', self.outdir]

        mappings = getoraclemappings(self.conf, tableName)

        if mappings is not None:
            cmd.append(mappings)

        return cmd

    def sqoop_import_orc_job(self, tableName):
        """
        Create Sqoop import orc job
        :param tableName:
        :return:
        """

        cmd = ['sqoop', 'import', '-Dorg.apache.sqoop.splitter.allow_text_splitter=true', '-Dhadoop.security.credential.provider.path={0}'.format(self.dbSecurityProviderPath),
               '-Doraoop.chunk.method=PARTITION', '--connect',
               '{0}:{1}/{2}'.format(self.dbServerAddress, self.dbServerPort, self.dbName), '--username', self.dbUser,
               '--password-alias', self.dbPasswordAlias, '--table', '{0}.{1}'.format(self.subDBName, tableName),
               '--hcatalog-table', tableName, '--hcatalog-database', self.targetDBName, '--autoreset-to-one-mapper',
               '--outdir', self.outdir]

        mappings = getoraclemappingsfororc(self.conf, tableName)

        if mappings is not None:
            cmd.append(mappings)

        return cmd

    def sqoop_import_sqlserver_job(self, tableName):
        """
        Create Sqoop import orc job
        :param tableName:
        :return:
        """
        print(self.dbServerAddress)

        cmd = ['sqoop', 'import', '-Dorg.apache.sqoop.splitter.allow_text_splitter=true',
               '-Dhadoop.security.credential.provider.path={0}'.format(self.dbSecurityProviderPath),
               '--connect',
               '\'{0}{1}:{2};database={3}\''.format(self.dbServerAddress,self.dbServerAdd, self.dbServerPort, self.dbName), '--username', self.dbUser,
               '--password', self.dbPassword, '--table', tableName,
               '--hcatalog-table', tableName, '--hcatalog-database', self.targetDBName, '--autoreset-to-one-mapper',
               '--outdir', self.outdir]

        mappings = getsqlservermappings(self.conf, tableName)
        if mappings is not None:
            cmd.append(mappings)

        return cmd

    def sqoop_import_mysql_job(self, tableName):
        """
        Create Sqoop import orc job
        :param tableName:
        :return:
        """

        print(self.dbServerAddress)

        cmd = ['sqoop', 'import', '-Dorg.apache.sqoop.splitter.allow_text_splitter=true',
               '-Dhadoop.security.credential.provider.path={0}'.format(self.dbSecurityProviderPath),
               '--connect',
               '\'{0}/{3}\''.format(self.dbServerAddress, self.dbServerAdd, self.dbServerPort,
                                                    self.dbName), '--username', self.dbUser,
               '--password', self.dbPassword, '--table', tableName,
               '--hcatalog-table', tableName, '--hcatalog-database', self.targetDBName, '--autoreset-to-one-mapper',
               '--outdir', self.outdir]

        mappings = getmysqlmappings(self.conf, tableName)
        if mappings is not None:
            cmd.append(mappings)

        return cmd

    def run_sqoop_import_orc_job(self, tableName):
        job = self.sqoop_import_orc_job(tableName)
        unix_command = UnixCommand()
        (ret, out, err) = unix_command.run_unix_cmd(job)
        print(ret, out, err)
        if ret == 0:
            logging.info('Success.')
        else:
            logging.info('Error.')
            return False
        return True

    def run_sqoop_import_mysql_job(self, tableName):
        job = self.sqoop_import_mysql_job(tableName)
        unix_command = UnixCommand()
        (ret, out, err) = unix_command.run_unix_cmd(job)
        print(ret, out, err)
        if ret == 0:
            logging.info('Success.')
        else:
            logging.info('Error.')
            return False
        return True

    def run_sqoop_import_sqlserver_job(self, tableName):
        job = self.sqoop_import_sqlserver_job(tableName)
        unix_command = UnixCommand()
        (ret, out, err) = unix_command.run_unix_cmd(job)
        print(ret, out, err)
        if ret == 0:
            logging.info('Success.')
        else:
            logging.info('Error.')
            return False
        return True

    def run_sqoop_import_avro_job(self, table):
        job = self.sqoop_import_avro_job(table)
        unix_command = UnixCommand()
        (ret, out, err) = unix_command.run_unix_cmd(job)
        print(ret, out, err)
        if ret == 0:
            logging.info('Success.')
        else:
            logging.info('Error.')
            return False
        return True

    def run_sqoop_export_job(self, table):
        job = self.sqoop_export_job(table)
        unix_command = UnixCommand()
        (ret, out, err) = unix_command.run_unix_cmd(job)
        print(ret, out, err)
        if ret == 0:
            logging.info('Success.')
        else:
            logging.info('Error.')
            return False
        return True

