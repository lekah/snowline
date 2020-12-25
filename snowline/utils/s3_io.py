import boto3, json, datetime, tempfile, os, shutil
from snowline.analysis.snowmap import SnowMap
from snowline.utils.geo_utils import boundaries_to_geo, geo_to_boundaries
from abc import ABCMeta

DB_VERSION = 0.1
BUCKET_URL = "https://snowlines.s3.eu-central-1.amazonaws.com"


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

    def download_db(self):
        """
        Utility function, download db locally for inspection
        """
        dbobj = self._s3_resource.Object(self._dbbucketname, self._dbname)
        dbobj.download_file(self._dbname)

    def upload(self, boundaries, dry_run=False, timestamp=None,
            verbose=True, wipe_previous=False):
        """
        :param boundaries: The boundaries as calculated by snowmap.get_boundaries
        :param bool dry_run: Whether this is a dry_run, if True will not upload
            but write to a directory
        :param timestamp: The timestamp to write to the DB. If None, will chose
            now()
        :param bool verbose: Enables verbose output
        :param bool wipe_previous: Deletes all previous data in the DB.
        """
        if timestamp is None:
            timestamp = datetime.datetime.now().timestamp()

        dbobj = self._s3_resource.Object(self._dbbucketname, self._dbname)

        # Creating a temporary directory to work in:
        with tempfile.TemporaryDirectory() as tmpdirname:
            if verbose:
                print("Working in temporary directory {}".format(tmpdirname))

            new_sl_data = boundaries_to_geo(boundaries)
            new_sl_filename = 'snowline_{}.json'.format(
                    datetime.datetime.strftime(
                        datetime.datetime.fromtimestamp(timestamp), "%Y%m%d_%H%M"))
            if verbose:
                print(" Done\nWriting snowline boundaries to {}...".format(new_sl_filename))
            with open(os.path.join(tmpdirname,new_sl_filename), 'w') as f:
                json.dump(new_sl_data, f, separators=(',', ':'))

            dbfilename = os.path.join(tmpdirname, self._dbname)
            try:
                if verbose:
                    print("Downloading database...", end='')
                dbobj.download_file(dbfilename)
                with open(dbfilename) as f:
                    database = json.load(f)
            except Exception as e:
                print("An exception occured: {}".format(e))
                if wipe_previous:
                    # This is recoverable since I dont need the DB in that case
                    if verbose:
                        print("Deciding not to download database.")
                    # TODO Improve this hardcoded shit:
                    database = {
                        "version": DB_VERSION,
                        "bucket": BUCKET_URL}
                else:
                    raise e
            database['updated'] = timestamp
            if wipe_previous:
                if verbose:
                    print(" Deleting previous data in DB")
                # Before I do this, make a backup
                for idx in range(1, 101):
                    filepath = 'snowline-{}.json'.format(idx)
                    if os.path.isfile(filepath):
                        continue
                    elif idx == 100:
                        raise ValueError("Exceed number of filenames"
                                " snowline-[1..100]")
                    print("Keeping backup of previous database in {}".format(
                            filepath))
                    with open(filepath, 'w') as f:
                        json.dump(database, f)
                    break
                database['data'] = []

            current_max_id = max([0] + [d['id'] for d in database['data']])
            # THe [0] + [ is to avoid get 0 when database is empty
            # otherwise raises a ValueError.


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

if __name__ == '__main__':
    from argparse import ArgumentParser
    parser = ArgumentParser()
    parser.add_argument('-d', '--download-database', action='store_true',
            help=('Downloads the DB locally'))
    parsed = parser.parse_args()
    if parsed.download_database:
        sdb = SnowlineDB()
        sdb.download_db()
