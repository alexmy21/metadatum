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

        # list all files with ext in directory
        file_list = utl.listAllFiles(dir_core, '.yaml')

        # print(file_list)

        for schema_file in file_list:
            print(schema_file)
            reg, idx, sha_id = cmd.createUserIndex(r, reg, schema_file, 'bootstrap')
            print(cmd.hllDoc(r, sha_id, schema_file))

    if __name__ == '__main__':
        boot()