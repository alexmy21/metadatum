from metadatum.utils import Utils as utl
from metadatum.commands import Commands
from metadatum.vocabulary import Vocabulary as voc
import redis
from redis.commands.search.document import Document

import logging
logging.basicConfig(filename='file_meta.log', encoding='utf-8', level=logging.DEBUG)


utl.importConfig()
import config as cnf

'''
    This processor is taking record from transaction index with the reference to
    resources that it should process. In our case resource is a file that 
    file_ingest processor must process 
'''
def run(data: Document) -> dict|None:

    pool = redis.ConnectionPool(host=cnf.settings.redis.host, port = cnf.settings.redis.port, db = 0)
    rs = redis.Redis(connection_pool = pool)

    _map:dict = rs.hgetall(data.id)
    map = dict((k.decode('utf8'), v.decode('utf8')) for k, v in _map.items())
    file = map[voc.URL]    
    f_prefix = utl.underScore(map.get(voc.ITEM_PREFIX))
    f_sha_id = map.get(voc.ITEM_ID)

    cmd = Commands()

    hll = cmd.parseDocument(rs, utl.fullId(f_prefix, f_sha_id), file)

    if hll > 0:
        print('File parsing: ', file)        
    else:
        print('Empty file: ', file)
        logging.error(f"Empty file: {file}; deleting {utl.underScore(utl.fullId(f_prefix, f_sha_id))} from 'file' redisearch index")
        rs.delete(utl.underScore(utl.fullId(f_prefix, f_sha_id)))

    return {file:hll}    
