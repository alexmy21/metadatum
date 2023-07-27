import os
import redis
import logging

from metadatum.utils import Utils as utl
from metadatum.vocabulary import Vocabulary as voc
from metadatum.commands import Commands 
from metadatum.bootstrap import Bootstrap


utl.importConfig()
import config as cnf

class Controller:    
    def __init__(self, path:str, data:dict):
        '''
            Bootstrap ensures that the registry index and all core indices exist.
            boot() command is idempotent. It will create indices if they don't exist.
        '''
        boot = Bootstrap()
        boot.boot()

        self.path = path
        self.data = data
        self.package = self.data.get(voc.PACKAGE)
        self.name = self.data.get(voc.NAME)
        self.schema = self.data.get(voc.SCHEMA)
        self.schema_dir = self.data.get(voc.SCHEMA_DIR)
        self.props = self.data.get(voc.PROPS)

        print('\n\nContr_props_1: ===> ', self.props)

        self.keys = self.data.get(voc.KEYS) 
        self.processor = utl.importModule(self.schema, self.package) 

        # dir = cnf.settings.dot_meta
        pool = redis.ConnectionPool(host=cnf.settings.redis.host, port = cnf.settings.redis.port, db = 0)
        self.rs = redis.Redis(connection_pool = pool)
        

    def run(self) -> dict|str|None:
        _data = {}
        _case = self.data.get(voc.LABEL)
        print(_case)
        match _case:
            case 'SOURCE':
                print('SOURCE')
                _data: dict = self.source()
            case 'TRANSFORM':
                print('TRANSFORM')
                _data: dict = Controller(self.path, self.data).process(voc.WAITING)
                print(_data)
            case 'COMPLETE':
                print('COMPLETE')
                _data: dict = Controller(self.path, self.data).process(voc.COMPLETE)
            case 'TEST':
                print('TEST')
                _data: dict = Controller(self.path, self.data).test()
            case _:
                print('default case')

        ''' 
            Update processor index
            Each processor has its own index to record each run of the processor.
        '''
        cmd = Commands()
        cmd.updateRecordHash(self.rs, prefix=self.schema, key_list=self.keys, props=self.props)

        # Submit processing results
        cmd.submit(self.rs, self.schema, voc.COMPLETE, 25)

        return _data

    def source(self) -> dict|None:
        print('\n\nContr_props_2: ===> ', self.props)
        _data:dict = self.processor.run(self.props)
        return _data
    
    def process(self, status: str) -> dict|None:
        _data:dict = self.data.get(voc.PROPS)
        resources = self.cmd.selectBatch(self.rs, voc.TRANSACTION, _data.get(voc.QUERY), _data.get(voc.LIMIT))
        ret = {}                    
        try:            
            for doc in resources.docs:
                # print('in for loop')
                processed = self.processor.run(doc, _data.get('duckdb_name'))                
                ret.update(processed)
                self.cmd.txStatus(self.rs, self.schema, self.schema, doc.id, status) 
        except:
            ret = {'error', 'There is no data to process.'}

        return ret
        
    
    def test(self) -> dict|None:
        _data:dict = self.processor.run()
        return _data