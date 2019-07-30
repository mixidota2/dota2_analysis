from dota2_config import *
import mysql.connector
from mysql.connector import errorcode
import json
from joblib import Parallel, delayed

def insert_data(n):
    cnx = mysql.connector.connect(user=USER,password=PASSWORD,host=HOST)
    cursor = cnx.cursor(buffered=True)
    try:
        match = API.get_match_details(match_id=n)
        players_json = json.dumps({"players":match['players']})
        picks_bans_json = json.dumps({'picks_bans':match['picks_bans']})
        insert_query = (
                        """INSERT INTO `{}` 
                        (`matchid`,`playerdata`,`duration`,`lobby_name`,`picks_bans`,`radiant_win`) 
                        VALUES (%s,%s,%s,%s,%s,%s)"""
                        .format(TABLE_WHOLEDATA))
        insert_data = (match['match_id'],players_json,match['duration'],match['lobby_name'],picks_bans_json,match['radiant_win'])
        if match['lobby_name'] == 'Ranked':
            try:
                print(n)
                cursor.execute("USE {}".format(DATABASE))
                cursor.execute(insert_query,insert_data)
                cnx.commit()
                cursor.close()
                cnx.close()
            except mysql.connector.Error as err:
                print("failed to insert data:{}".format(err))
            
        else:
            print("It is not Ranked")
    except:
        print("Match ID not found")
       


def use_joblib(startid,upto_number):
    Parallel(n_jobs=4,verbose=10)([delayed(insert_data)(n) for n in range(startid,startid + upto_number)])

use_joblib(4878636745,10)




