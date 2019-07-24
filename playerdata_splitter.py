from dota2_config import *
import mysql.connector
from mysql.connector import errorcode
import json
import re
from joblib import Parallel, delayed
import pandas as pd
import numpy as np

def playerdata_splitter(n):
        getplayers_query = (r"""SELECT JSON_EXTRACT(`playerdata`,"$.players") FROM `wholedata_722d` WHERE matchid = %s""")
        getplayers_data = (n,)
        cnx = mysql.connector.connect(user=USER,password=PASSWORD,host=HOST, auth_plugin='caching_sha2_password')
        cursor = cnx.cursor(buffered=True)
        try:
            cursor.execute("USE dota2_match_datalake")
            cursor.execute(getplayers_query,getplayers_data)
            result = cursor.fetchone()
            try:
                result_list = json.loads(result[0])
                print(n)
                for i in range(10):
                        eachdata = result_list[i]
                        eachdata['matchid'] = n
                        eachdata['ability_upgrades'] = "none"
                        player_query = (
                                        """INSERT INTO `{}` 
                                        (`match_id`,`player_slot`,`hero_id`,`item_0`,`item_1`,`item_2`,`item_3`,
                                        `item_4`,`item_5`,`backpack_0`,`backpack_1`,`backpack_2`,`kills`,`deaths`,
                                        `assists`,`leaver_status`,`last_hits`,`denies`,`gold_per_min`,`xp_per_min`,
                                        `level`,`hero_damage`,`hero_healing`,`tower_damage`) 
                                        VALUES 
                                        (%(matchid)s,%(player_slot)s,%(hero_id)s,%(item_0)s,%(item_1)s,%(item_2)s,
                                        %(item_3)s,%(item_4)s,%(item_5)s,%(backpack_0)s,%(backpack_1)s,%(backpack_2)s,
                                         %(kills)s,%(deaths)s,%(assists)s,%(leaver_status)s,%(last_hits)s,%(denies)s,
                                        %(gold_per_min)s,%(xp_per_min)s,%(level)s,%(hero_damage)s,%(hero_healing)s,%(tower_damage)s)"""
                                        .format(TABLE_EACH_PLAYERDATA)
                                        )
                        cursor.execute(player_query,eachdata)
                cnx.commit()
                cursor.close()
                cnx.close()
            except:
                print("Match ID not found")

        except mysql.connector.Error as err:
            print("failed to insert data:{}".format(err))
            

def use_joblib(match_arr):
    Parallel(n_jobs=8,verbose=10)([delayed(playerdata_splitter)(n) for n in match_arr])


def match_id_getter():
    conn = mysql.connector.connect(user=USER, password=PASSWORD, host=HOST,database=DATABASE)
    get_matchid_sql = ("SELECT `matchid` from `{}`".format(TABLE_WHOLEDATA))
    match_df = pd.read_sql(get_matchid_sql,conn)
    match_df_arr = np.array(match_df['matchid'],dtype=np.int64)
    return map(int,match_df_arr)

match_arr = match_id_getter()
use_joblib(match_arr)
