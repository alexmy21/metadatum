# import os
import time
import redis
# import logging

from metadatum.utils import Utils as utl
from metadatum.vocabulary import Vocabulary as voc
from metadatum.commands import Commands 


utl.importConfig()
import config as cnf

class Controller:    
    def __init__(self, path:str, data:dict):
        
        self.path = path
        self.data = data
        self.package = self.data.get(voc.PACKAGE)
        self.name = self.data.get(voc.NAME)
        self.props = self.data.get(voc.PROPS)

        self.keys = self.data.get(voc.KEYS) 
        self.processor = utl.importModule(self.name, self.package) 

        # dir = cnf.settings.dot_meta
        pool = redis.ConnectionPool(host=cnf.settings.redis.host, port = cnf.settings.redis.port, db = 0)
        self.rs = redis.Redis(connection_pool = pool)
        

    def run(self) -> dict|str|None:
        cmd = Commands()
        t1 = time.perf_counter()
        _data = {}
        _case = self.data.get(voc.LABEL)
        print(_case)
        match _case:
            case 'SOURCE':
                print('SOURCE')
                _data: dict = self.source()
            case 'TRANSFORM':
                print('TRANSFORM')
                _data: dict = self.process(voc.WAITING, False)
                print(_data)
            case 'COMPLETE':
                print('COMPLETE')
                _data: dict = self.process(voc.COMPLETE, False)
            case 'BATCH_TRANSFORM':
                print('TRANSFORM')
                _data: dict = self.process(voc.WAITING, True)
                print(_data)
            case 'BATCH_COMPLETE':
                print('COMPLETE')
                _data: dict = self.process(voc.COMPLETE, True)
            case 'TEST':
                print('TEST')
                _data: dict = self.test()
            case _:
                print('default case')

        ''' 
            Update processor index
            Each processor has its own index to record each run of the processor.
        '''
        cmd.updateLog(self.rs, self.data, t1, 'logging')

        # Submit processing results
        cmd.submit(self.rs, self.name, voc.COMPLETE, 25)

        return _data

    def source(self) -> dict|None:
        _data:dict = self.processor.run(self.props)
        return _data
    
    def process(self, status: str, batch:bool) -> dict|None:
        cmd = Commands()
        _data:dict = self.data.get(voc.PROPS)
        query = _data.get(voc.QUERY)
        # print('query', query)
        resources = cmd.selectBatch(self.rs, voc.TRANSACTION, query, _data.get(voc.LIMIT))
        # print('resources', resources.docs)
        ret = {}                    
        try: 
            if batch:
                processed = self.processor.run(resources.docs, self.props)
                ret.update(processed)
            else:  
                print(self.name)   
                for doc in resources.docs:
                    processed = self.processor.run(doc)        
                    ret.update(processed)
                    # redis, proc_id: str, proc_uuid: str, item_id: str, status: str
                    cmd.txStatus(self.rs, self.name, '', str(doc.id), status)
        except:
            ret = {'error': 'There is no data to process.'}

        return ret        
    
    def test(self) -> dict|None:
        _data:dict = self.processor.run()
        return _data