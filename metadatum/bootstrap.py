import os
import redis
import logging

from metadatum.vocabulary import Vocabulary as voc
from metadatum.utils import Utils as utl
from metadatum.commands import Commands

utl.importConfig()
import config as cnf

class Bootstrap:

    def boot(self):
        pool = redis.ConnectionPool(host = cnf.settings.redis.host, port = cnf.settings.redis.port, db = 0)
        r = redis.Redis(connection_pool = pool)

        cmd = Commands()

        dir = cnf.settings.dot_meta
        idx_reg_file = os.path.join(dir, cnf.settings.indices.idx_reg_file)
        # logging.debug(f"idx_reg.yaml path {idx_reg_file}")

        reg, idx, sha_id = cmd.createRegistryIndex(r, idx_reg_file, 'bootstrap')

        dir_core = os.path.join(dir, cnf.settings.indices.dir_core)


        # load all libraries
        dir_func = os.path.join(dir, cnf.settings.scripts.dir_scripts)
        file_list = utl.listAllFiles(dir_func, '.lua')

        for lib_file in file_list:
            logging.debug(lib_file)
            cmd.loadLib(r, lib_file, True)


        # list all files with ext in directory
        file_list = utl.listAllFiles(dir_core, '.yaml')

        for schema_file in file_list:
            logging.debug(schema_file)
            reg, idx, sha_id = cmd.createUserIndex(r, reg, schema_file, 'bootstrap')
            logging.debug(cmd.parseDocument(r, reg.get(voc.PREFIX) + sha_id, schema_file))

        # Commit Processed indices
        #=======================================================
        # 1. Create commit instance in 'commit' redisearch index
        c_sha_id, t_stamp = cmd.createCommit(r)

        # 2. Commit all processed files
        query = '(@processor_ref:bootstrap @status:{0})'.format(voc.COMPLETE)
        while(True):
            # redis, idx_name: str, query:str, limit: int = 100
            keys = cmd.selectBatch(r, 'transaction', query, 25)
            list = cmd.fldValuesFromSearchResult(keys, 'id')
            
            if len(list) > 0:  
                # print('\n\n', c_sha_id, t_stamp, list)             
                cmd.commit(r, c_sha_id, t_stamp, list)
            else:
                break

        return reg, idx, sha_id

if __name__ == '__main__':
    boot = Bootstrap()
    boot.boot()