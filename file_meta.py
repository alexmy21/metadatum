import os
import logging
logging.basicConfig(filename='file_meta.log', encoding='utf-8', level=logging.DEBUG)

import time
import redis
from redis.commands.graph import Graph, Node, Edge
from pathlib import Path

from metadatum.commands import Commands
from metadatum.utils import Utils as utl
from metadatum.vocabulary import Vocabulary as voc
from metadatum.bootstrap import Bootstrap

utl.importConfig()
import config as cnf

def run(props: dict = None):        
        print('props: ===> ', props)              
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
        reg, idx, sha_id = cmd.createUserIndex(r, b_reg, idx_file, 'file_meta')

        cmd.parseDocument(r, 'registry' + sha_id, idx_file)

        key_list = idx.get(voc.KEYS)

        # list all files with ext in directory
        file_list = utl.listAllFiles(props.get('dir'), props.get('file_type'))
        # Process each file
        for file in file_list:
            print(file)
            dir, _file = os.path.split(os.path.abspath(file))
            # extract file extention from file name
            ext = Path(file).suffix
            map: dict = {
                'parent_id': dir,
                'url': _file,
                'file_type': ext,
                'size': os.path.getsize(file),
                'doc:': ' ',
                'commit_id': 'und',
                'commit_status': 'und'
            } 
            f_prefix = idx.get(voc.PREFIX)
            f_sha_id = cmd._updateRecordHash(r, f_prefix, key_list, map) 
            cmd.txCreate(r, 'file_meta', idx.get(voc.NAMESPACE), f_sha_id, f_prefix, file, voc.WAITING)           

        return props