import boto3, json, datetime, tempfile, os
from snowline.analysis.snowmap import SnowMap



def boundaries_to_geo(boundaries):
    features = []
    for boundary in boundaries:
        features.append({'type': 'Feature',
           'properties': {},
           'geometry': {'type': 'Polygon',
            'coordinates':[b.tolist() for b in boundary]}})

    return {'type': 'FeatureCollection',
          'features':features}


class SnowlineDB(object):
    def __init__(self, dbbucketname='snowlines-database',
            snowlinebucketname='snowlines',
            dbname='snowline.json'):
        self._dbbucketname = dbbucketname
        self._snowlinebucketname = snowlinebucketname
        self._dbname = dbname
        self._s3_resource = boto3.resource('s3')

    def upload(self, boundaries, dry_run=False):
        now = datetime.datetime.now()
        timestamp = int(now.timestamp())

        dbobj = self._s3_resource.Object(self._dbbucketname, self._dbname)
        
        
        
        # Creating a temporary directory to working
        with tempfile.TemporaryDirectory() as tmpdirname:
            print("Working in temporary directory {}".format(tmpdirname))
            dbfilename = os.path.join(tmpdirname, 'snowlines.json')
            print("Downloading database...", end='')
            dbobj.download_file(dbfilename)
            with open(dbfilename) as f:
                database = json.load(f)

            new_sl_data = boundaries_to_geo(boundaries)
            new_sl_filename = 'snowline_{}.json'.format(datetime.datetime.strftime(now, "%Y%m%d_%H%M"))
            print(" Done\nWriting snowline boundaries to {}...",format(new_sl_filename))
            with open(os.path.join(tmpdirname,new_sl_filename), 'w') as f:
                json.dump(new_sl_data, f)


            database['updated'] = timestamp
            database['data'].append({'id':max([d['id'] for d in database['data']])+1,
                    'datetime':timestamp, 'url':new_sl_filename})
            print(" Done\nWriting new database... ", end='')
            with open(dbfilename, 'w') as f:
                json.dump(database, f)


            print("Done")
            # upload
            if not dry_run:
                print("Uploading database and new snowline to bucket... ", end="")
                slobj = self._s3_resource.Object(self._snowlinebucketname, new_sl_filename)
                slobj.upload_file(os.path.join(tmpdirname, new_sl_filename))
                dbobj.upload_file(dbfilename)
                print("Done")
            else:
                input("This is a dry run, enter to delete temp dir")
