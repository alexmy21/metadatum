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
       
        pool = redis.ConnectionPool(host=cnf.settings.redis.host, port = cnf.settings.redis.port, db = 0)
        rs = redis.Redis(connection_pool = pool)

        cmd = Commands()

        query = props.get('query')

        print(query)

        results = cmd.search(rs, voc.BIG_IDX, query, props.get('limit'))

        t_set = set()
        for doc in results.docs:            
            sha_id = utl.getIdShaPart(str(doc.id))
            bi_ref_id = utl.fullId(voc.BI_REF, sha_id)
            t_set= t_set.union(rs.smembers(bi_ref_id))

        if len(t_set) == 0:
            return props
        else:
            for t in t_set:
                t_str = t.decode('utf-8')
                item_prefix = utl.getIdPrefix(t_str)
                item_sha_id = utl.getIdShaPart(t_str)

                item_hash = rs.hgetall(utl.denormId(t_str))
                url = item_hash.get(b'url').decode('utf-8')
                schema_id = item_hash.get(b'schema_id').decode('utf-8')

                cmd.txCreate(rs, 'load_search', schema_id, item_sha_id, item_prefix, url, voc.WAITING) 

        return props