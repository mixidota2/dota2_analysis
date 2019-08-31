from dota2_config import *
import mysql.connector
from mysql.connector import errorcode
import numpy as np
import pandas as pd
from joblib import Parallel, delayed

api = API

def role_hantei(n):
    role_get_sql = (f"select `gold_per_min`,`player_slot` from `{TABLE_EACH_PLAYERDATA}` where `match_id` = %s order by `player_slot`")
    matchid = (n,)
    print(n)
    cnx = mysql.connector.connect(user=USER, password=PASSWORD, host=HOST)
    cursor = cnx.cursor(buffered=True)
    cursor.execute("USE dota2_match_datalake")
    cursor.execute(role_get_sql, matchid)
    result = cursor.fetchall()
    print(result)
    try:
        radiant = (result[0], result[1], result[2], result[3], result[4])
        dire = (result[5], result[6], result[7], result[8], result[9])
        print(radiant)
        print(dire)
        role_radiant = sorted(radiant, key=lambda x: x[0], reverse=True)
        role_dire = sorted(dire, key=lambda x: x[0], reverse=True)
        print(role_radiant)
        print(role_dire)
        role_list_radiant = []
        role_list_dire = []
        for i in range(5):
            role_list_radiant.append(role_radiant.index(radiant[i]))
            role_list_dire.append(role_dire.index(dire[i]))
        role_list_radiant = list(map(lambda x:x+1,role_list_radiant))
        role_list_dire = list(map(lambda x:x+1,role_list_dire))
        print(role_list_radiant)
        print(role_list_dire)
        role_insert_query = (f"UPDATE `{TABLE_EACH_PLAYERDATA}` SET `role`=%s WHERE `match_id`=%s AND `player_slot`=%s")
        role_insert_values = []
        for i, role in zip(range(0,5),role_list_radiant):
            each_tuple = (role,n,i)
            role_insert_values.append(each_tuple)
            print(role_insert_values)
        for i, role in zip(range(128,133),role_list_dire):
            each_tuple = (role,n,i)
            role_insert_values.append(each_tuple)
            print(role_insert_values)
        role_insert_values = tuple(role_insert_values)
        print("convert list to tuple")
        print(role_insert_values)
        cursor.executemany(role_insert_query,role_insert_values)
        print("inserted")
        cnx.commit()
        cursor.close()
        cnx.close()
    except:
        print("skipped")

def use_joblib(match_array):
    Parallel(n_jobs=4,verbose=10)([delayed(role_hantei)(n) for n in match_array])

conn = mysql.connector.connect(user=USER, password=PASSWORD, host=HOST,database=DATABASE)
get_matchid_sql = (f"SELECT `matchid` from `{TABLE_WHOLEDATA}`")
match_df = pd.read_sql(get_matchid_sql,conn)
match_arr = np.array(match_df['matchid'],dtype=np.int64)
match_arr = map(int,match_arr)
use_joblib(match_arr)
