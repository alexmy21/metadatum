# Import Multiplication from your library
import os
import time
import redis
from redis.commands.graph import Graph, Node, Edge

from metadatum.commands import Commands
from metadatum.utils import Utils as utl
from metadatum.vocabulary import Vocabulary as voc
from metadatum.bootstrap import Bootstrap

utl.importConfig()
import config as cnf

class File:    

    def __init__(self):

        self.time_start = time.perf_counter()
        boot = Bootstrap()
        boot.boot()

    def run(self):

        dir = cnf.settings.dot_meta
        pool = redis.ConnectionPool(host=cnf.settings.redis.host, port = cnf.settings.redis.port, db = 0)
        r = redis.Redis(connection_pool = pool)

        cmd = Commands()

        idx_file = os.path.join(dir, cnf.settings.indices.dir_user, 'schemas/file.yaml')

        '''
            createIndex command is idempotent. We can run it to get latest version of the index schema
            or create index if it doesn't exist.
        '''
        idx = cmd.createIndex(r, idx_file)
        key_list = idx.get(voc.KEYS)

        # list all files with ext in directory
        file_list = utl.listAllFiles('/home/alexmy/Downloads/POC/DATA/DEMO', '.csv')

        print(file_list)

        for file in file_list:
            print(file)

            dir, _file = os.path.split(os.path.abspath(file))

            map: dict = {
                'parent_id': dir,
                'url': _file,
                'file_type': 'csv',
                'size': os.path.getsize(file),
                'doc:': ' ',
                'commit_id': 'und',
                'commit_status': 'und'
            }

            sha_id = cmd.createRecordHash(r, idx.get(voc.PREFIX), key_list, map)

            # print('ID: ', sha_id)

            # redis, proc_ref:str, namespace:str, item_id:str, item_prefix:str, url:str, status: str
            hash = cmd.txCreate(r, 'main_file', idx.get(voc.NAMESPACE), sha_id, idx.get(voc.PREFIX), file, voc.WAITING)
            cmd.hllDoc(r, sha_id, file, batch=200000)
            # print()

        time_end = time.perf_counter()
        execution_time = time_end - self.time_start
        print('Execution time: ', execution_time)

if __name__ == '__main__':
    file = File()
    file.run()