import luigi
from luigi.util import inherits, requires
from luigi.contrib.gcs import GCSClient, GCSTarget
import dota2api
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
import subprocess
from luigiconfig import *
from time import sleep


class get_ranked(luigi.Task):
    matchid = luigi.Parameter(default=1)
    dotapatch = luigi.Parameter()
    task_complete = False
    
    def complete(self):
        return self.task_complete

    
    def run(self):
        api = dota2api.Initialise(STEAM_KEY)
        match = api.get_match_details(match_id=self.matchid)
        print("match data ready")
        if match['lobby_name'] == 'Ranked':
            #some code to insert data into Firestore
            #try:
            db = firestore.client()
            doc_ref = db.collection('match_data').document('patch').collection(f'{self.dotapatch}').document(f"{match['match_id']}")
            doc_count_ref = db.collection('match_data').document('patch').collection(f'{self.dotapatch}').document('count_table')
            num_of_docs = doc_count_ref.get(['count']).get('count')
            doc_ref.set(match)
            doc_count_ref.update({'count':num_of_docs + 1})
            self.task_complete = True
            print(f"inserted match(id:{self.matchid})")
            #except:
            #    print("failed to insert match-data.")
            #    self.task_complete = True

        else:
            self.task_complete = True
            print(f"it is not ranked(id:{self.matchid})")

#@requires(get_ranked)
class pub_task(luigi.Task):
    startid = luigi.Parameter()
    dotapatch = luigi.Parameter()

    def requires(self):
        #cred = credentials.Certificate(CRED_PATH)
        #firebase_admin.initialize_app(cred)
        db = firestore.client()
        doc_count_ref = db.collection('match_data').document('patch').collection(f'{self.dotapatch}').document('count_table')
        if doc_count_ref.get().exists == False:
            doc_count_ref.set({'count':1})
        num_of_docs = doc_count_ref.get(['count']).get('count')
        first_id = int(self.startid)
        increment_num = 0
        print("start creating tasks")
        while num_of_docs < 1000:
            current_id = first_id + num_of_docs
            print(f"docs count :{num_of_docs}")
            search_id = current_id + increment_num
            increment_num += 1
            print(f"search id:{search_id}")
            sleep(5)
            yield get_ranked(matchid=str(search_id),dotapatch=self.dotapatch)
    
    def output(self):
        return GCSTarget(f"gs://dota2-analysis-247916.appspot.com/{self.dotapatch}")
    
    def run(self):
        #some code to export Firestore docs into Cloud Storage
        subprocess.call(["sudo","gcloud","beta","firestore","export",f"gs://dota2-analysis-247916.appspot.com/{self.dotapatch}",f"--collection-ids='match_data','{self.dotapatch}'"])


#@requires(pub_task)
#class load_to_bq(luigi.Task):

#    def __init__(self):
#        cred = credentials.Certificate(CRED_PATH)
#        firebase_admin.initialize_app(cred)

#    def output(self):
#        return #pass to BigQuery
    
#    def run(self):
        #some code to load data into Big Query from Cloud Storage


if __name__ == '__main__':
    #luigi.run()
    cred = credentials.Certificate(CRED_PATH)
    firebase_admin.initialize_app(cred)
    luigi.build([pub_task(startid=5051000136,dotapatch="7.22h")],local_scheduler=True)
    #luigi.build([get_ranked(matchid=5051000136,dotapatch="7.22h")],local_scheduler=True)