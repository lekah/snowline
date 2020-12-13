import boto3, json, datetime, tempfile, os, shutil
from snowline.analysis.snowmap import SnowMap
from abc import ABCMeta


def boundaries_to_geo(boundaries):
    features = []
    for boundary in boundaries:
        features.append({'type': 'Feature',
           'properties': {},
           'geometry': {'type': 'Polygon',
            'coordinates':[b.tolist() for b in boundary]}})

    return {'type': 'FeatureCollection',
          'features':features}


class S3DB(object, metaclass=ABCMeta):
    def __init__(self, aws_access_key_id=None,
            aws_secret_access_key=None):
        self._s3_resource = boto3.resource('s3',
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key)

    def get_files(self):
        dbbucket = self._s3_resource.Bucket(name=self._dbbucketname)
        files = []
        for dbobj_ in dbbucket.objects.all():
            files.append(dbobj_.key)
        return files

    def download_files(self, filenames, directory, overwrite=False):
        os.makedirs(directory, exist_ok=True)
        dbbucket = self._s3_resource.Bucket(name=self._dbbucketname)
        for filename in filenames:
            full_path = os.path.join(directory, filename)
            if not(overwrite) and os.path.isfile(full_path):
                print("Not downloading {}".format(filename))
                continue
            # TODO Exception handling if files do not exist!
            print("Downloading {}".format(filename))
            dbobj = self._s3_resource.Object(self._dbbucketname, filename)
            dbobj.download_file(full_path)


class SatelliteDB(S3DB):
    def __init__(self, dbbucketname='snowlines-satellite', **kwargs):
        self._dbbucketname = dbbucketname
        super().__init__(**kwargs)


class SnowlineDB(S3DB):
    def __init__(self, dbbucketname='snowlines-database',
            snowlinebucketname='snowlines',
            dbname='snowline.json', **kwargs):
        self._dbbucketname = dbbucketname
        self._snowlinebucketname = snowlinebucketname
        self._dbname = dbname
        super().__init__(**kwargs)

    def upload(self, boundaries, dry_run=False, timestamp=None,
            verbose=True):
        if timestamp is None:
            timestamp = datetime.datetime.now().timestamp()

        dbobj = self._s3_resource.Object(self._dbbucketname, self._dbname)

        # Creating a temporary directory to work in:
        with tempfile.TemporaryDirectory() as tmpdirname:
            if verbose:
                print("Working in temporary directory {}".format(tmpdirname))
            dbfilename = os.path.join(tmpdirname, 'snowlines.json')
            if verbose:
                print("Downloading database...", end='')
            dbobj.download_file(dbfilename)
            with open(dbfilename) as f:
                database = json.load(f)

            new_sl_data = boundaries_to_geo(boundaries)
            new_sl_filename = 'snowline_{}.json'.format(
                    datetime.datetime.strftime(
                        datetime.datetime.fromtimestamp(timestamp), "%Y%m%d_%H%M"))
            if verbose:
                print(" Done\nWriting snowline boundaries to {}...",format(new_sl_filename))
            with open(os.path.join(tmpdirname,new_sl_filename), 'w') as f:
                json.dump(new_sl_data, f)


            database['updated'] = timestamp
            try:
                current_max_id = max([d['id'] for d in database['data']])
            except ValueError:
                # max raises Value Error if an empty sequence is passed
                current_max_id = 0
            database['data'].append({'id':current_max_id+1,
                    'datetime':timestamp, 'url':new_sl_filename})
            if verbose:
                print(" Done\nWriting new database... ", end='')
            with open(dbfilename, 'w') as f:
                json.dump(database, f)
            if verbose:
                print("Done")
            # upload
            if not dry_run:
                if verbose:
                    print("Uploading database and new snowline to "
                            "bucket... ", end="")
                slobj = self._s3_resource.Object(
                        self._snowlinebucketname, new_sl_filename)
                slobj.upload_file(os.path.join(tmpdirname, new_sl_filename))
                dbobj.upload_file(dbfilename)
                if verbose:
                    print("Done")
            else:
                for idx in range(1, 101):
                    dirpath = 'dry-run-{}'.format(idx)
                    if os.path.isdir(dirpath):
                        continue
                    elif idx == 100:
                        raise ValueError("Exceed number of directories"
                                " dry-run-[1..100]")
                    print("Files I would have sent are in  {}".format(
                            dirpath))
                    shutil.copytree(tmpdirname, dirpath)
                    break
