import os
import redis

import utils as utl
from vocabulary import Vocabulary as voc
from commands import Commands

class Controller:    
    def __init__(self, path:str, data:dict, redis_host:str, redis_port:int):
        self.path = path
        self.data = data
        self.package = self.data.get(voc.PACKAGE)
        self.name = self.data.get(voc.NAME)
        self.schema = self.data.get(voc.SCHEMA)
        self.schema_dir = self.data.get(voc.SCHEMA_DIR)
        self.props = self.data.get(voc.PROPS)

        self.uri = os.path.normpath(os.path.join(self.path, self.package, self.name))        
        module = '.' + self.schema
        self.processor = utl.importModule(module, self.package) 
        self.rs = utl.getRedis(redis_host, redis_port)

        ''' 
            Update processor index
        '''
        cmd = Commands(self.rs)
        schema_path = os.path.join(self.schema_dir, self.schema + '.yaml')
        # print('Controller: ' + path)
        cmd.updateRecord(self.schema, schema_path, self.props, True)

    def run(self) -> dict|str|None:
        _data = {}
        _case = self.data.get(voc.LABEL)
        print(_case)
        match _case:
            case 'SOURCE':
                print('SOURCE')
                _data: dict = Controller(self.path, self.data).source()
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

        return _data

    def source(self) -> dict|None:
        _data:dict = self.processor.run(self.data.get(voc.PROPS))
        return _data
    
    def process(self, status: str) -> dict|None:
        rs = utl.getRedis(Client().config_props) 
        _data:dict = self.data.get(voc.PROPS)
        resources = cmd.selectBatch(rs, voc.TRANSACTION, _data.get(voc.QUERY), _data.get(voc.LIMIT))
        ret = {}                    
        try:            
            for doc in resources.docs:
                # print('in for loop')
                processed = self.processor.run(doc, _data.get('duckdb_name'))                
                ret.update(processed)
                cmd.txStatus(rs, self.schema, self.schema, doc.id, status) 
        except:
            ret = {'error', 'There is no data to process.'}

        return ret
        
    
    def test(self) -> dict|None:
        _data:dict = self.processor.run()
        return _data