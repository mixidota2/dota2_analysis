import dota2api
import mysql.connector
from mysql.connector import errorcode
import json
import re
from joblib import Parallel, delayed
import pandas as pd
import numpy as np

api = dota2api.Initialise("C03AFEE69BA4181B7B20988EFB41E6CD")

#cnx = mysql.connector.connect(user='root',password='takuwan1',host='localhost')
#cursor = cnx.cursor(buffered=True)

#matchid = 4317400992
#query = ("SELECT `playerdata` from `wholedata`" "where matchid = %s")


def playerdata_splitter(n):
        getplayers_query = (r"""SELECT JSON_EXTRACT(`playerdata`,"$.players") FROM `wholedata_722d` WHERE matchid = %s""")
        getplayers_data = (n,)
        cnx = mysql.connector.connect(user='root',password='takuwan1',host='localhost')
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
                        player_query = "INSERT INTO `each_playerdata_722d` (`match_id`,`player_slot`,`hero_id`,`item_0`,`item_1`,`item_2`,`item_3`,`item_4`,`item_5`,`backpack_0`,`backpack_1`,`backpack_2`,`kills`,`deaths`,`assists`,`leaver_status`,`last_hits`,`denies`,`gold_per_min`,`xp_per_min`,`level`,`hero_damage`,`hero_healing`,`tower_damage`) VALUES (%(matchid)s,%(player_slot)s,%(hero_id)s,%(item_0)s,%(item_1)s,%(item_2)s,%(item_3)s,%(item_4)s,%(item_5)s,%(backpack_0)s,%(backpack_1)s,%(backpack_2)s,%(kills)s,%(deaths)s,%(assists)s,%(leaver_status)s,%(last_hits)s,%(denies)s,%(gold_per_min)s,%(xp_per_min)s,%(level)s,%(hero_damage)s,%(hero_healing)s,%(tower_damage)s)"
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


conn = mysql.connector.connect(user='root', password='takuwan1', host='localhost',database='dota2_match_datalake')
get_matchid_sql = ("SELECT `matchid` from `wholedata_722d`")
match_df = pd.read_sql(get_matchid_sql,conn)
match_arr = np.array(match_df['matchid'],dtype=np.int64)
match_arr = map(int,match_arr)
use_joblib(match_arr)