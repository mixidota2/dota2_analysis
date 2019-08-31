from dota2_config import *
import mysql.connector
from mysql.connector import errorcode
import luigi
from luigi.contrib.mysqldb import MySqlTarget
from luigi.util import inherits, requires
import socket
import json
import re
socket.getaddrinfo('127.0.0.1', 3306)


class matchdata_mining(luigi.Task):
    
    matchid = luigi.IntParameter()
    api = API
    host = HOST
    database = DATABASE
    user = USER
    password = PASSWORD
    table = TABLE_WHOLEDATA

    def output(self):
        target_wholedata = luigi.contrib.mysqldb.MySqlTarget(host=self.host,database=self.database,user=self.user,password=self.password,table=self.table,update_id=("wholedata-" + str(self.matchid)))
        return target_wholedata
    
    def run(self):
        connection_wholedata = self.output().connect()
        cursor = connection_wholedata.cursor(buffered=True)
        match = self.api.get_match_details(match_id=self.matchid)
        players_json = json.dumps({"players":match['players']})
        picks_bans_json = json.dumps({'picks_bans':match['picks_bans']})
        insert_query = f"INSERT INTO `{self.table}` (`matchid`,`playerdata`,`duration`,`lobby_name`,`picks_bans`,`radiant_win`) VALUES (%s,%s,%s,%s,%s,%s)"
        insert_data = (match['match_id'],players_json,match['duration'],match['lobby_name'],picks_bans_json,match['radiant_win'])
        if match['lobby_name'] == 'Ranked':
            try:
                cursor.execute(insert_query,insert_data)
                connection_wholedata.commit()
                self.output().touch()
                cursor.close()
                connection_wholedata.close()
            except mysql.connector.Error as err:
                print("failed to insert data:{}".format(err))
                self.output().touch()
        else:
            print("skipped")
            self.output().touch()


@requires(matchdata_mining)
class data_splitter(luigi.Task):
    matchid = luigi.IntParameter()
    
    api = API
    host = HOST
    database = DATABASE
    user = USER
    password = PASSWORD
    table = TABLE_EACH_PLAYERDATA

    def output(self):
        target_eachplayerdata = luigi.contrib.mysqldb.MySqlTarget(host=self.host,database=self.database,user=self.user,password=self.password,table=self.table,update_id=("each_playerdata-" + str(self.matchid)))
        return target_eachplayerdata
    
    def run(self):
        getplayers_query = (r"""SELECT JSON_EXTRACT(`playerdata`,"$.players") FROM `wholedata` WHERE matchid = %s""")
        getplayers_data = (self.matchid,)
        target_eachplayerdata = luigi.contrib.mysqldb.MySqlTarget(host=self.host,database=self.database,user=self.user,password=self.password,table=self.table,update_id=str(self.matchid))
        connection_wholedata = self.output().connect()
        cursor = connection_wholedata.cursor(buffered=True)
        try:
            cursor.execute(getplayers_query,getplayers_data)
            result = cursor.fetchone()
            result_list = json.loads(result[0])
            for i in range(10):
                eachdata = result_list[i]
                eachdata['matchid'] = self.matchid
                eachdata['ability_upgrades'] = "none"
                player_query = "INSERT INTO `each_playerdata` (`match_id`,`player_slot`,`hero_id`,`item_0`,`item_1`,`item_2`,`item_3`,`item_4`,`item_5`,`backpack_0`,`backpack_1`,`backpack_2`,`kills`,`deaths`,`assists`,`leaver_status`,`last_hits`,`denies`,`gold_per_min`,`xp_per_min`,`level`) VALUES (%(matchid)s,%(player_slot)s,%(hero_id)s,%(item_0)s,%(item_1)s,%(item_2)s,%(item_3)s,%(item_4)s,%(item_5)s,%(backpack_0)s,%(backpack_1)s,%(backpack_2)s,%(kills)s,%(deaths)s,%(assists)s,%(leaver_status)s,%(last_hits)s,%(denies)s,%(gold_per_min)s,%(xp_per_min)s,%(level)s)"
                cursor.execute(player_query,eachdata)
            connection_wholedata.commit()
            self.output().touch()
            cursor.close()
            connection_wholedata.close()
        except mysql.connector.Error as err:
            print("failed to insert data:{}".format(err))
            self.output().touch()


class task_factory(luigi.WrapperTask):
    startid = luigi.IntParameter()
    
    def requires(self):
        for i in range(20000):
            workid = self.startid + i
            yield data_splitter(matchid=workid)




if __name__ == '__main__':
    luigi.build([task_factory(startid=4848640200)],local_scheduler=True)
