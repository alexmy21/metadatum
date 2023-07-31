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
        registry = os.path.join(dir, cnf.settings.indices.registry)
        # logging.debug(f"idx_reg.yaml path {idx_reg_file}")

        reg, sha_id = cmd.createRegistryIndex(r, registry, 'bootstrap')

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

        # Submit processing results
        cmd.submit(r, 'bootstrap', voc.COMPLETE, 25)

        return reg, idx, sha_id

if __name__ == '__main__':
    boot = Bootstrap()
    boot.boot()