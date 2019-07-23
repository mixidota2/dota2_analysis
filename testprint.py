import dota2api
import pprint
import mysql.connector
from mysql.connector import errorcode
import codecs
import re
import ast
import json
api = dota2api.Initialise("C03AFEE69BA4181B7B20988EFB41E6CD")
match = api.get_match_details(match_id=4317400000)
data = (str(match['match_id']),str(match['players']))

cnx = mysql.connector.connect(user='root',password='takuwan1',host='localhost')
cursor = cnx.cursor(buffered=True)

query = (r"""select JSON_EXTRACT(`json_data`,"$.players") from `json_test` where `id` = 3""")

cursor.execute("USE dota2_match_datalake")
cursor.execute(query)

result = cursor.fetchone()
something = json.loads(result[0])
pprint.pprint(something[0])

#newresult = result[0]
#m = re.findall(r'{.*?}',newresult)
#a = m[1]
#b = ast.literal_eval(a)
#print(a)
#print(b['hero_name'])

cursor.close()
cnx.close()

#query = "INSERT INTO `items` (`cost`,`id`,`localized_name`,`name`,`recipe`,`secret_shop`,`side_shop`,`url_image`) VALUES (%(cost)s,%(id)s,%(localized_name)s,%(name)s,%(recipe)s,%(secret_shop)s,%(side_shop)s,%(url_image)s)"