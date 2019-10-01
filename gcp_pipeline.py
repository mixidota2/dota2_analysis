import luigi
from luigi.util import inherits, requires
import dota2api


class get_ranked(luigi.Task):
    matchid = luigi.Parameter()
    def run(self):
        api = dota2api.Initialise(STEAM_KEY)
        match = api.get_match_details(match_id=self.matchid)
    
        if match['lobby_name'] == 'Ranked':
            #some code to insert data into Firestore
        else:
            pass


class pub_task(luigi.Task):
    startid = luigi.Parameter()
    def requires(self):
        while num_of_docs < 10000000:
            self.stratid += 1
            return get_ranked(matchid=self.startid)
    
    def output(self):
        return #pass to Cloud Storage
    
    def run(self):
        #some code to export Firestore docs into Cloud Storage


@requires(pub_task)
class load_to_bq(luigi.Task):

    def output(self):
        return #pass to BigQuery
    
    def run(self):
        #some code to load data into Big Query from Cloud Storage
