import os
import logging
logging.basicConfig(filename='main_file.log', encoding='utf-8', level=logging.DEBUG)

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

    def run(self):
        '''
            Bootstrap ensures that the registry index and all core indices exist.
            boot() command is idempotent. It will create indices if they don't exist.
        '''
        boot = Bootstrap()
        b_reg, b_idx, b_sha_id = boot.boot()

        dir = cnf.settings.dot_meta
        pool = redis.ConnectionPool(host=cnf.settings.redis.host, port = cnf.settings.redis.port, db = 0)
        r = redis.Redis(connection_pool = pool)

        cmd = Commands()

        idx_file = os.path.join(dir, cnf.settings.indices.dir_user, 'schemas/file.yaml')

        '''
            createUserIndex command is idempotent. We can run it to get latest version of the index schema
            or create index if it doesn't exist.
        '''
        reg, idx, sha_id = cmd.createUserIndex(r, b_reg, idx_file, 'main_file')

        cmd.parseDocument(r, 'idx_reg' + sha_id, idx_file)

        key_list = idx.get(voc.KEYS)

        # list all files with ext in directory
        file_list = utl.listAllFiles('/home/alexmy/Downloads', '.pdf')

        # Process each file
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
            f_prefix = idx.get(voc.PREFIX)
            f_sha_id = cmd._createRecordHash(r, f_prefix, key_list, map)

            if cmd.parseDocument(r, utl.fullId(f_prefix, f_sha_id), file) > 0:
                cmd.txCreate(r, 'main_file', idx.get(voc.NAMESPACE), f_sha_id, f_prefix, file, voc.WAITING)
            else:
                print('Empty file: ', file)
                logging.error(f"Empty file: {file}; deleting {utl.underScore(utl.fullId(f_prefix, f_sha_id))} from 'file' redisearch index")
                r.delete(utl.underScore(utl.fullId(f_prefix, f_sha_id)))

        # Commit Processed files
        #=======================================================
        # 1. Create commit instance in 'commit' redisearch index
        c_sha_id, t_stamp = cmd.createCommit(r)

        # 2. Commit all processed files
        query = '(@processor_ref:main_file @status:{0})'.format(voc.WAITING)
        while(True):
            # redis, idx_name: str, query:str, limit: int = 100
            keys = cmd.selectBatch(r, 'transaction', query, 25)
            list = cmd.fldValuesFromSearchResult(keys, 'id')
            
            if len(list) > 0:  
                # print('\n\n', c_sha_id, t_stamp, list)             
                cmd.commit(r, c_sha_id, t_stamp, list)
            else:
                break


if __name__ == '__main__':
    time_start = time.perf_counter()
    file = File()
    file.run()
    time_end = time.perf_counter()
    print(f"Elapsed time: {time_end - time_start:0.4f} seconds")