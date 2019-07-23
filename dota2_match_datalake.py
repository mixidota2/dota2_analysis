import mysql.connector
from mysql.connector import errorcode

DB_NAME = 'Dota2_match_datalake'

TABLES = {}
TABLES['whole_data'] = ("CREATE TABLE wholedata (matchid int(11) NOT NULL,allthedata int(11) NOT NULL);")


cnx = mysql.connector.connect(user='root',password='takuwan1',host='localhost')
cursor = cnx.cursor()

def create_database(cursor):
    try:
        cursor.execute("CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(DB_NAME))
    except mysql.connector.Error as err:
        print("Failed creating database: {}".format(err))
        exit(1)

try:
    cursor.execute("USE {}".format(DB_NAME))
except mysql.connector.Error as err:
    print("Database {} does not exists.".format(DB_NAME))
    if err.errno == errorcode.ER_BAD_DB_ERROR:
       #3-B.存在しなければデータベースを構築
        create_database(cursor)
        print("Database {} created successfully.".format(DB_NAME))
        cnx.database = DB_NAME
    #3-C.他のエラーであれば、アプリケーションを終了し、エラーメッセージを移す
    else:
        print(err)
        exit(1)


for table_name in TABLES:
    table_description = TABLES[table_name]
    try:
        print("Creating table {}: ".format(table_name), end='')
        cursor.execute(table_description)
    except mysql.connector.Error as err:
        #4-A.テーブルが存在するというエラーが出た場合は文字列を表示
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print("already exists.")
        #4-B.そうでない場合のエラーはエラーメッセージを表示
        else:
            print(err.msg)
    else:
        print("OK")
cursor.close()
cnx.close()