import dota2api
import pprint
import mysql.connector
from mysql.connector import errorcode
import json
from joblib import Parallel, delayed

#api = dota2api.Initialise("C03AFEE69BA4181B7B20988EFB41E6CD")

#query = "INSERT INTO `wholedata` (`matchid`,`playerdata`,`duration`,`lobby_name`,`picks_bans`,`radiant_win`) VALUES (%s,%s,%s,%s,%s,%s)"

#cnx = mysql.connector.connect(user='root',password='takuwan1',host='localhost')
#cursor = cnx.cursor(buffered=True)

def insert_data(n):
    api = dota2api.Initialise("C03AFEE69BA4181B7B20988EFB41E6CD")
    cnx = mysql.connector.connect(user='root',password='takuwan1',host='localhost')
    #connection_wholedata = self.output().connect()
    cursor = cnx.cursor(buffered=True)
    try:
        match = api.get_match_details(match_id=n)
        players_json = json.dumps({"players":match['players']})
        picks_bans_json = json.dumps({'picks_bans':match['picks_bans']})
        insert_query = "INSERT INTO `wholedata_722d` (`matchid`,`playerdata`,`duration`,`lobby_name`,`picks_bans`,`radiant_win`) VALUES (%s,%s,%s,%s,%s,%s)"
        insert_data = (match['match_id'],players_json,match['duration'],match['lobby_name'],picks_bans_json,match['radiant_win'])
        if match['lobby_name'] == 'Ranked':
            try:
                print(n)
                cursor.execute("USE dota2_match_datalake")
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

use_joblib(4878336761,300000)




