import cx_Oracle
import pyodbc
import mysql.connector


def getmysqlmappings(conf, tableName):
    cnx = mysql.connector.connect(
        host=conf["dbServerAdd"],
        user=conf["dbUser"],
        password=conf["dbPassword"],
        database=conf["dbName"]
    )

    getColValTypes = []
    getColNames = []

    mapColumn = "--map-column-java"
    mapColumnHive = "--map-column-hive"

    cursor = cnx.cursor()

    query = "SELECT table_schema, table_name, column_name, ordinal_position, data_type, numeric_precision, column_type," \
            "column_default, is_nullable, column_comment FROM information_schema.columns WHERE (table_schema='{table_schema}' " \
            "and table_name='{table}') order by ordinal_position".format(table_schema=conf["dbName"], table=tableName)

    cursor.execute(query)
    for row in cursor:
        columnName = str(row[2])
        columnType = str(row[4])
        if 'CLOB' in columnType:
            columnType="String"
            getColValTypes.append(columnName+'='+columnType)
            getColNames.append(columnName)
        elif 'BLOB' in columnType:
            columnType="String"
            getColValTypes.append(columnName+'='+columnType)
            getColNames.append(columnName)

    cursor.close()
    cnx.close()

    if len(getColValTypes):
        return mapColumn + ' ' + ','.join(getColValTypes) + ' ' + mapColumnHive + ' ' + ','.join(getColValTypes)
    else:
        return None


def getoraclemappings(conf, tableName):
    connstr='{0}/{1}@{2}:{3}/{4}'.format(conf["dbUser"], conf["dbUser"], conf["dbServerAdd"], conf["dbServerPort"],
                                         conf["dbName"])

    conn = cx_Oracle.connect(connstr)
    cur = conn.cursor()

    getColValTypes = []
    getColNames=[]

    mapColumn="--map-column-java"


    sqlStmt='SELECT * FROM {usertable} where rownum < 2'.format(usertable=tableName)
    print(tableName)
    a=cur.execute(sqlStmt)
    tableSchema=cur.description
    for i in tableSchema:
        columnName=str(i[0])
        columnType=str(i[1])
        #print('{0}={1}\n'.format(columnName,columnType))
        if 'CLOB' in columnType:
            columnType="String"
            getColValTypes.append(columnName+'='+columnType)
            getColNames.append(columnName)
        elif 'BLOB' in columnType:
            columnType="String"
            getColValTypes.append(columnName+'='+columnType)
            getColNames.append(columnName)
        elif 'OBJECT' in columnType:
            columnType="String"
            getColValTypes.append(columnName+'='+columnType)
            getColNames.append(columnName)

    cur.close()
    conn.close()

    if len(getColValTypes):
        return mapColumn + ' ' + ','.join(getColValTypes)
    else:
        return None


def getoraclemappingsfororc(conf, tableName):
    connstr = '{0}/{1}@{2}:{3}/{4}'.format(conf["dbUser"], conf["dbPassword"], conf["dbServerAdd"],
                                           conf["dbServerPort"],
                                           conf["dbName"])

    conn = cx_Oracle.connect(connstr)
    cur = conn.cursor()

    getColValTypes = []
    getColNames = []

    mapColumn = "--map-column-java"
    mapColumnHive = "--map-column-hive"

    sqlStmt = 'SELECT * FROM {dbname}.{usertable} where rownum < 2'.format(usertable=tableName, dbname=conf["subDBName"])
    print(tableName)
    a = cur.execute(sqlStmt)
    tableSchema = cur.description
    for i in tableSchema:
        columnName = str(i[0])
        columnType = str(i[1])
        #print('{0}={1}\n'.format(columnName,columnType))
        if 'OBJECT' in columnType:
            columnType = "String"
            getColValTypes.append(columnName + '=' + columnType)
            getColNames.append(columnName)
        if 'LONG_STRING' in columnType:
            columnType = "String"
            getColValTypes.append(columnName + '=' + columnType)
            getColNames.append(columnName)
        if 'ROWID' in columnType:
            columnType = "String"
            getColValTypes.append(columnName + '=' + columnType)
            getColNames.append(columnName)

    cur.close()
    conn.close()

    if len(getColValTypes):
        return mapColumn + ' ' + ','.join(getColValTypes) + ' ' + mapColumnHive + ' ' + ','.join(getColValTypes)
    else:
        return None


def getsqlservermappings(conf, tableName):
    server = '{0},{1}'.format(conf["dbServerAdd"], conf["dbServerPort"])
    database = conf["dbName"]
    username = conf["dbUser"]
    password = conf["dbPassword"]

    getColValTypes = []
    getColNames = []

    cnxn = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username +
        ';PWD=' + password)

    cursor = cnxn.cursor()
    query = 'SELECT COLUMN_NAME,* FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = \'{0}\';'.format(tableName)
    cursor.execute(query)

    mapColumn = "--map-column-java"
    mapColumnHive = "--map-column-hive"
    numberOfMappers = "-m 1"

    inlcudeMappers = False

    for row in cursor:
        # row = cursor.fetchone()
        columnName = row.COLUMN_NAME
        columnType = row.DATA_TYPE
        print("{}\t{}\n".format(columnName,columnType))
        if 'uniqueidentifier' in columnType:
            includeMappers = True
            columnType = "String"
            getColValTypes.append(columnName + '=' + columnType)
            getColNames.append(columnName)
        
    cursor.close()
    cnxn.close()

    if len(getColValTypes):
        cmd =  mapColumn + ' ' + ','.join(getColValTypes) + ' ' + mapColumnHive + ' ' + ','.join(getColValTypes)
        if includeMappers == True:
            cmd = cmd + '  ' + numberOfMappers
        
        return cmd
    else:
        return None


def createHiveTableQuery(conf, tableName):
    connstr='{0}/{1}@{2}:{3}/{4}'.format(conf["dbUser"], conf["dbPassword"], conf["dbServerAdd"], conf["dbServerPort"],
                                         conf["dbName"])

    conn = cx_Oracle.connect(connstr)
    cur = conn.cursor()

    getColValTypes = []
    getColNames = []

    tablecreation = ["CREATE EXTERNAL TABLE", " STORED AS ORC LOCATION '{0}/{1}'".format(conf["targetDirectory"], tableName)]

    tableName = "{0}.{1}".format(conf["subDBName"], tableName)
    sqlStmt = 'SELECT * FROM {usertable} where rownum < 2'.format(usertable=tableName)
    print(tableName)
    a = cur.execute(sqlStmt)
    tableSchema = cur.description
    for i in tableSchema:
        columnName = '\`{0}\`'.format(str(i[0]))
        columnType = str(i[1])
        if 'NUMBER' in columnType:
            columnType = "BIGINT"
            getColValTypes.append(columnName + ' ' + columnType)
            getColNames.append(columnName)
        elif 'STRING' in columnType:
            columnType = "STRING"
            getColValTypes.append(columnName + ' ' + columnType)
            getColNames.append(columnName)
        elif 'ROWID' in columnType:
            columnType = "STRING"
            getColValTypes.append(columnName + ' ' + columnType)
            getColNames.append(columnName)
        elif 'FIXED_CHAR' in columnType:
            columnType = "STRING"
            getColValTypes.append(columnName + ' ' + columnType)
            getColNames.append(columnName)
        elif 'TIMESTAMP' in columnType:
            columnType = "TIMESTAMP"
            getColValTypes.append(columnName + ' ' + columnType)
            getColNames.append(columnName)
        elif 'CLOB' in columnType:
            columnType = "STRING"
            getColValTypes.append(columnName + ' ' + columnType)
            getColNames.append(columnName)
        elif 'BLOB' in columnType:
            columnType = "STRING"
            getColValTypes.append(columnName + ' ' + columnType)
            getColNames.append(columnName)
        elif 'VARCHAR' in columnType:
            columnType = "STRING"
            getColValTypes.append(columnName + ' ' + columnType)
            getColNames.append(columnName)
        elif 'NVARCHAR' in columnType:
            columnType = "STRING"
            getColValTypes.append(columnName + ' ' + columnType)
            getColNames.append(columnName)
        elif 'VARCHAR2' in columnType:
            columnType = "STRING"
            getColValTypes.append(columnName + ' ' + columnType)
            getColNames.append(columnName)
        elif 'NVARCHAR2' in columnType:
            columnType = "STRING"
            getColValTypes.append(columnName + ' ' + columnType)
            getColNames.append(columnName)
        elif 'CHAR' in columnType:
            columnType = "STRING"
            getColValTypes.append(columnName + ' ' + columnType)
            getColNames.append(columnName)
        elif 'NCHAR' in columnType:
            columnType = "STRING"
            getColValTypes.append(columnName + ' ' + columnType)
            getColNames.append(columnName)
        elif 'DATE' in columnType:
            columnType = "DATE"
            getColValTypes.append(columnName + ' ' + columnType)
            getColNames.append(columnName)
        elif 'RAW' in columnType:
            columnType = "BINARY"
            getColValTypes.append(columnName + ' ' + columnType)
            getColNames.append(columnName)
        elif 'LONG RAW' in columnType:
            columnType = "BINARY"
            getColValTypes.append(columnName + ' ' + columnType)
            getColNames.append(columnName)
        elif 'FLOAT' in columnType:
            columnType = "FLOAT"
            getColValTypes.append(columnName + ' ' + columnType)
            getColNames.append(columnName)
        elif 'DOUBLE' in columnType:
            columnType = "DOUBLE"
            getColValTypes.append(columnName + ' ' + columnType)
            getColNames.append(columnName)
        elif 'BINARY_FLOAT' in columnType:
            columnType = "FLOAT"
            getColValTypes.append(columnName + ' ' + columnType)
            getColNames.append(columnName)
        elif 'BINARY_DOUBLE' in columnType:
            columnType = "DOUBLE"
            getColValTypes.append(columnName + ' ' + columnType)
            getColNames.append(columnName)
        elif 'LONG' in columnType:
            columnType = "BINARY"
            getColValTypes.append(columnName + ' ' + columnType)
            getColNames.append(columnName)
        elif 'CLOB' in columnType:
            columnType = "BINARY"
            getColValTypes.append(columnName + ' ' + columnType)
            getColNames.append(columnName)
        elif 'NCLOB' in columnType:
            columnType = "BINARY"
            getColValTypes.append(columnName + ' ' + columnType)
            getColNames.append(columnName)
        elif 'BLOB' in columnType:
            columnType = "BINARY"
            getColValTypes.append(columnName + ' ' + columnType)
            getColNames.append(columnName)
        elif 'TIMESTAMP' in columnType:
            columnType = "TIMESTAMP"
            getColValTypes.append(columnName + ' ' + columnType)
            getColNames.append(columnName)
        else:
            print("Error Data Type {0} Not Found in Mappings".format(columnType))
            sys.exit()

    tableName = tableName.split('.')
    cmd = tablecreation[0] + ' ' + conf["targetDBName"] + '.' + tableName[1] + '(' + ','.join(getColValTypes) + ', `last_changed` timestamp NULL DEFAULT CURRENT_TIMESTAMP)' + \
          tablecreation[1]

    cur.close()
    conn.close()

    return cmd


def createHiveQueryFromSQLServer(conf, tableName):
    server = '{0},{1}'.format(conf["dbServerAdd"], conf["dbServerPort"])
    database = conf["dbName"]
    username = conf["dbUser"]
    password = conf["dbPassword"]

    getColValTypes = []
    getColNames = []

    tablecreation = ["CREATE EXTERNAL TABLE",
                     " STORED AS ORC LOCATION '{0}/{1}'".format(conf["targetDirectory"], tableName)]

    cnxn = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username +
        ';PWD=' + password)
    
    cursor = cnxn.cursor()
    query = 'SELECT COLUMN_NAME,* FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = \'{0}\';'.format(tableName)
    cursor.execute(query)

    # rows = cursor.fetchall()
    for row in cursor:
        # row = cursor.fetchone()
        columnName = '\`{0}\`'.format(row.COLUMN_NAME)
        columnType = row.DATA_TYPE
        if 'bit' in columnType:
            columnType = "BOOLEAN"
            getColValTypes.append(columnName + ' ' + columnType)
            getColNames.append(columnName)
        elif 'datetime' in columnType:
            columnType = "DATE"
            getColValTypes.append(columnName + ' ' + columnType)
            getColNames.append(columnName)
        elif 'int' in columnType:
            columnType = "INT"
            getColValTypes.append(columnName + ' ' + columnType)
            getColNames.append(columnName)
        elif 'numeric' in columnType:
            columnType = "DOUBLE"
            getColValTypes.append(columnName + ' ' + columnType)
            getColNames.append(columnName)
        elif 'uniqueidentifier' in columnType:
            columnType = "STRING"
            getColValTypes.append(columnName + ' ' + columnType)
            getColNames.append(columnName)
        elif 'varchar' in columnType:
            columnType = "STRING"
            getColValTypes.append(columnName + ' ' + columnType)
            getColNames.append(columnName)
        else:
            getColValTypes.append(columnName + ' ' + "STRING")
            getColNames.append(columnName)

    # tableName = tableName.split('.')
    cmd = tablecreation[0] + ' ' + conf["targetDBName"] + '.' + tableName + '(' + ','.join(getColValTypes) + ')' + \
          tablecreation[1]

    cursor.close()
    cnxn.close()

    return cmd

def createHiveQueryFromMySQL(conf, tableName):
    cnx = mysql.connector.connect(
        host=conf["dbServerAdd"],
        user=conf["dbUser"],
        password=conf["dbPassword"],
        database=conf["dbName"]
    )

    getColValTypes = []
    getColNames = []

    tablecreation = ["CREATE EXTERNAL TABLE",
                     " STORED AS ORC LOCATION '{0}/{1}'".format(conf["targetDirectory"], tableName)]

    cursor = cnx.cursor()

    query = "SELECT table_schema, table_name, column_name, ordinal_position, data_type, numeric_precision, column_type," \
            "column_default, is_nullable, column_comment FROM information_schema.columns WHERE (table_schema='{table_schema}' " \
            "and table_name='{table}') order by ordinal_position".format(table_schema=conf["dbName"], table=tableName)

    cursor.execute(query)
    for row in cursor:
        columnName = str(row[2])
        columnType = str(row[4])
        if 'TINYINT(1)' in columnType:
            columnType = "BOOLEAN"
            getColValTypes.append('\`' + columnName + '\` ' + columnType)
            getColNames.append(columnName)
        elif 'datetime' in columnType:
            columnType = "DATE"
            getColValTypes.append('\`' + columnName + '\` ' + columnType)
            getColNames.append(columnName)
        elif 'int' in columnType:
            columnType = "INT"
            getColValTypes.append('\`' + columnName + '\` ' + columnType)
            getColNames.append(columnName)
        elif 'double' in columnType:
            columnType = "DOUBLE"
            getColValTypes.append('\`' + columnName + '\` ' + columnType)
            getColNames.append(columnName)
        elif 'varchar' in columnType:
            columnType = "STRING"
            getColValTypes.append('\`' + columnName + '\` ' + columnType)
            getColNames.append(columnName)
        else:
            getColValTypes.append('\`' + columnName + '\` ' + "STRING")
            getColNames.append(columnName)

    # tableName = tableName.split('.')
    cmd = tablecreation[0] + ' ' + conf["targetDBName"] + '.' + tableName + '(' + ','.join(getColValTypes) + ')' + \
          tablecreation[1]

    cursor.close()
    cnx.close()

    return cmd

