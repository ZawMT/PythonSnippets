import mysql.connector
import os.path
import sshtunnel

print("Testing DB Connection : Start")

_file_pem = 'path to pem file'

if not os.path.exists(_file_pem):
    print("PEM file missing. DB connection cannot be established!!!")

_ssh_host = 'SSH IP'
_ssh_port = 0 # SSH Port
_ssh_username = '' #SSH User

_sql_hostname = 'SQL HOST'
_sql_username = 'SQL User'
_sql_password = 'SQL Pwd'
_sql_main_database = 'DB Schema name'
_sql_port = 0 #DB Port

try:
    with sshtunnel.SSHTunnelForwarder(ssh_address=(_ssh_host, _ssh_port), ssh_username=_ssh_username,
                                      ssh_pkey=_file_pem,
                                      remote_bind_address=(_sql_hostname, _sql_port)) as tunnel:
        print("Tunneling is OK ... now getting DB connection")
        cnx = mysql.connector.MySQLConnection(host='127.0.0.1',
                                              port=tunnel.local_bind_port,
                                              user=_sql_username,
                                              password=_sql_password,
                                              database=_sql_main_database,
                                              connection_timeout=30)
        print("Getting DB connection Done!!!")
        if cnx.is_connected():
            print("DB connection is opened!")
            cursor = cnx.cursor()
            cursor.execute("select * from DBSchemaName.TableName")
            for(ColumnName) in cursor:
                print("{}".format(ColumnName))
        else:
            print("DB connection fails!!!")
except Exception as x:
    print("Error in connecting to DB")
    print(x)

print("Testing DB Connection : Done!!!")
