from dota2_config import *
import mysql.connector
import pandas as pd
import numpy as np

conn = mysql.connector.connect(user=USER, password=PASSWORD, host=HOST,database=DATABASE)
get_data_sql = (
                """SELECT 
                A.match_id,C.localized_name,A.player_slot 
                FROM `{}` AS A 
                LEFT JOIN `wholedata_722d` AS B 
                ON A.match_id = B.matchid 
                LEFT JOIN `heroes` AS C 
                ON A.hero_id = C.id 
                WHERE 
                case 
	                when A.player_slot in (0,1,2,3,4) and B.radiant_win = 0 then 0 
                	when A.player_slot in (0,1,2,3,4) and B.radiant_win = 1 then 1 
                	when A.player_slot in (128,129,130,131,132) and B.radiant_win = 0 then 1 
                	when A.player_slot in (128,129,130,131,132) and B.radiant_win = 1 then 0 
                	else NULL 
                end 
                = 1 
                ORDER BY A.match_id """
                .format(TABLE_EACH_PLAYERDATA))

df = pd.read_sql(get_data_sql,conn)

print(df.shape)
print(df.columns)
print(df.head(10))
print(df.tail(10))