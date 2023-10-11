from ast import Dict, List
import os
import re
# import string
import logging
import string
import time
from typing import Any

import redis
from redis.commands.graph.edge import Edge
from redis.commands.graph.node import Node
# from redis.commands.graph import Graph, Node, Edge
# from redis.commands.graph import GraphCommands as gcmd

from redis.commands.search.indexDefinition import IndexDefinition
from redis.commands.search.query import Query
from redis.commands.search.document import Document

from cerberus import Validator
from cymple.builder import QueryBuilder


from metadatum.vocabulary import Vocabulary as voc
from metadatum.utils import Utils as utl

import textract
# import nltk, string
# nltk.download('punkt')

utl.importConfig()
import config as cnf

class Commands: 
    '''
        This is list of commands to work with RedisGraph
    '''
    def mergeNodeQuery(self, node_labels: list|str, properties: dict|str|Any = None):
        return (QueryBuilder()
                .merge()
                .node(labels=node_labels, properties=properties)
                )

    def mergeEdgeQuery(self, e_label:str, e_label_left:str, e_left:dict, e_label_right:str, e_right:dict, e_props:dict):
        
        query = (QueryBuilder()
            .match()
            .node(labels=e_label_left, properties=e_left, ref_name='src')
            .match()
            .node(labels=e_label_right, properties=e_right, ref_name='dst')
            .merge()
            .node(ref_name='src')
            .related_to(label=e_label, properties=e_props)
            .node(ref_name='dst')
            )

        return query
    
    def buildIndex(self, r: redis.Redis, cnf_settings, schema_file:str, processor_ref:str):     

        dir = cnf.settings.dot_meta
        idx_file = os.path.join(dir, cnf_settings, schema_file)
        '''
            createUserIndex command is idempotent. We can run it to get latest version of the index schema
            or create index if it doesn't exist.
        '''
        reg, idx, schema_sha_id = self.createUserIndex(r, idx_file, processor_ref)

        self.parseDocument(r, 'registry' + schema_sha_id, idx_file)

        return reg, idx, schema_sha_id

       
    # Update Edge Index
    def updateEdgeIndex(self, redis, keys:list, args:list, commit:bool):        
        function = 'edge_index_update'
        k_len = len(keys)
        keys.extend(args)
        # pprint(keys)
        ret = redis.fcall(function, k_len, *keys)
        if commit:
            self.submit(redis, 'edge_index_update', 'waiting', 1000)

        return ret

    def getRedisHash(self, redis, key) -> dict|None:
        _map:dict = redis.hgetall(key)
        return dict((k.decode('utf8'), v.decode('utf8')) for k, v in _map.items())

    '''
        Redisearch support methods
    '''    
    # Create index
    def createIndex(self, redis, schema_path: str):
        sch = utl.getSchemaFromFile(schema_path) 
        # logging.debug(f'\n{sch}')

        p_dict, n_doc = utl.getProps(sch, schema_path)  

        if p_dict == None:
            return False

        ok = False
        try:
            index_name = n_doc.get(voc.NAME)
            # Create index
            schema = utl.ft_schema(p_dict)

            redis.ft(index_name).create_index(schema, definition=IndexDefinition(prefix=[utl.prefix(index_name)]))
            ok = True
        except:
            logging.debug(f'Index already exists, return: {ok}')

        return n_doc

    '''
        Creating hash for the instance in IDX_REG using attributes
        from IDX_REG (reg) and current index schema (idx)
    '''
    # Create index hash
    def createIndexHash(self, redis, idx: dict, schema_path):
        '''
            Gete registry schema reference
        '''
        dir = cnf.settings.dot_meta
        reg_file= os.path.join(dir, cnf.settings.indices.registry)
        reg_props, reg_doc = utl.getProps(utl.getSchemaFromFile(reg_file), reg_file)

        # print(reg_doc)

        sch = utl.getSchemaFromFile(schema_path)

        ''' Register index in idx_reg
            All attributes for the index are taken from the current index schema
            except for the 'prefix' that will attach this instance to IDX_REG index 
        '''
        map: dict = {
            voc.NAME: idx.get(voc.NAME),
            voc.NAMESPACE: idx.get(voc.NAMESPACE),
            voc.ITEM_PREFIX: 'registry',
            voc.LABEL: 'REGISTER', #reg.get(voc.LABEL),
            voc.KIND: reg_doc.get(voc.KIND), # reg
            voc.COMMIT_ID: reg_doc.get(voc.PROPS).get(voc.COMMIT_ID), # reg
            voc.COMMIT_STATUS: reg_doc.get(voc.PROPS).get(voc.COMMIT_STATUS), # reg
            voc.SOURCE: str(sch)
        }
        # logging.debug('IDX_REG record: {}'.format(idx_reg_dict))

        '''
            Create hash record in Redis, hash key is sha1 of the list of keys
            from the index schema. Prefis is underscored to indicate that this
            record is not a part of the index yet. It would be added to the index
            after commiting transaction.
        '''
        _pref = utl.underScore('registry')
        k_list: dict = reg_doc.get(voc.KEYS)

        sha_id = utl.sha1(k_list, map)
        # add item ID (__id) to map
        map[voc.ID] = sha_id

        full_id = utl.fullId(_pref, sha_id)

        redis.hset(full_id, mapping=map)

        return reg_doc, sha_id

    '''
        Aggregated functions that support index creation
    '''
    # Create Registry index (IDX_REG) from .yaml file
    def createRegistryIndex(self, redis, idx_reg_file, proc_name) -> dict|None: 

        logging.debug(f'{idx_reg_file}')

        reg = {}
        idx = {}
        reg = self.createIndex(redis, idx_reg_file)
        idx = reg

        # logging.debug(f'{reg}, \n, {idx}')

        reg_doc, sha_id = self.createIndexHash(redis, idx, idx_reg_file)
        hash = self.txCreate(redis, proc_name, reg.get(voc.NAMESPACE), sha_id, reg.get(voc.PREFIX), '.yaml', idx_reg_file, voc.COMPLETE)

        return reg, sha_id

    # Create user defined index from .yaml file
    def createUserIndex(self, redis, schema_file, proc_name) -> dict|None:
        idx = self.createIndex(redis, schema_file)
        reg, sha_id = self.createIndexHash(redis, idx, schema_file)
        # redis, proc_ref:str, namespace:str, item_id:str, item_prefix:str, url:str, status: str
        _hash = self.txCreate(redis, proc_name, idx.get(voc.NAMESPACE), sha_id, reg.get(voc.PREFIX), '.yaml', schema_file, voc.COMPLETE)

        return reg, idx, sha_id
    
    # Create record hash
    def _updateRecordHash(self, redis, prefix: str, key_list: list, props:dict) -> str|None:        
        _pref = utl.underScore(prefix)
        return self.updateRecordHash(redis, _pref, key_list, props)

        # sha_id = utl.sha1(key_list, props)
        # # add item ID (__id) to props
        # props[voc.ID] = sha_id

        # full_id = utl.fullId(_pref, sha_id)
        # redis.hset(full_id, mapping = props)
        # return sha_id
    
    def updateRecordHash(self, redis, prefix: str, key_list: list, props:dict) -> str|None:
        '''
            Create hash record in Redis, hash key is sha1 of the list of keys
            from the index schema. This record is not going to transaction 
            so prefix is not underscored.
        '''
        sha_id = utl.sha1(key_list, props)
        # add item ID (__id) to props
        props[voc.ID] = sha_id

        full_id = utl.fullId(prefix, sha_id)
        redis.hset(full_id, mapping = props)

        return sha_id
        

    def updateLog(self, redis, data:dict, t1, prefix:str='logging') -> str|None:

        key_list = ["label", "version", "package", "name", "language", "props"]

        map = {}
        map[voc.LABEL] = data.get(voc.LABEL)
        map[voc.VERSION] = data.get(voc.VERSION)
        map[voc.PACKAGE] = data.get(voc.PACKAGE)
        map[voc.NAME] = data.get(voc.NAME)
        map[voc.LANGUAGE] = data.get(voc.LANGUAGE)  
        map[voc.PROPS] = str(data.get(voc.PROPS))        

        sha_id = utl.sha1(key_list, map)
        # add item ID (__id) to map
        map[voc.ID] = sha_id
        map[voc.TIMESTAMP] = utl.timestamp()
        map['run_time'] = time.perf_counter() - t1

        full_id = utl.fullId(prefix, sha_id)
        redis.hset(full_id, mapping = map)

        return sha_id

    '''
        Create hash record for BIG_IDX index in Redis, hash key is sha1 of the term 
    '''
    def createBigIdxHash(self, redis, term, reference) -> str|None:
        
        sha_id = utl.sha1_str(term)
        props = {}
        full_id = utl.fullId(voc.BIG_IDX, sha_id)
        redis.hset(full_id, mapping = props)

        return id


    '''
        Managing transaction index - the list of available for processing records
        "TRANSACTION" index is a core of metadatum. It is a list of records that are
        ready for processing. Each record is a Redis hash that contains all the information
        about the resource including the URL reference to resource and its status.
    '''
    # This is one of the methods that populates 'transaction' index 
    def txCreate(self, redis, proc_ref: str, schema_id:str, sha_id: str, item_prefix: str, item_type:str, url:str, status: str) -> dict|None:
       
        try:
            schema_id = schema_id.decode('utf-8')
        except (UnicodeDecodeError, AttributeError):
            pass
        try:
            item_type = item_type.decode('utf-8')
        except (UnicodeDecodeError, AttributeError):
            pass

        full_id = utl.fullId(voc.TRANSACTION, sha_id)
        map:dict = redis.hgetall(full_id)

        _map = {}
        _map[voc.PROCESSOR_REF] = proc_ref
        _map[voc.SCHEMA_ID] = schema_id
        _map[voc.ITEM_ID] = sha_id
        _map[voc.ITEM_PREFIX] = item_prefix
        _map[voc.ITEM_TYPE] = item_type
        _map[voc.URL] = url
        _map[voc.PROCESSOR_UUID] = ' '
        _map[voc.STATUS] = status
        
        map.update(_map)
        
        return redis.hset(full_id, mapping=map)

        
    # Updates status of resource with the value that reflects the processing step 
    '''
        proc_id is a normilized (full_id 'prefix:sha_id' without ':') processor id
        proc_uuid is a temporary UUID for locking item in TRANSACTION index for current processor
        item_id is a normalized item id
    ''' 
    def txStatus(self, redis, proc_id: str, proc_uuid: str, item_id: str, status: str) -> dict|None:
        if voc.TRANSACTION in item_id:
            item_id = utl.denormId(item_id)
        else:
            return None
        
        _map:dict = redis.hgetall(item_id) 
        map = dict((k.decode('utf8'), v.decode('utf8')) for k, v in _map.items()) 

        if map == None:
            return None
        else:            
            map[voc.PROCESSOR_REF] = proc_id
            map[voc.PROCESSOR_UUID] = proc_uuid
            map[voc.STATUS] = status
            return redis.hset(item_id, mapping=map)

    
    '''
        This method is used to lock (set status to "locked") a batch of records in transaction index 
        for processing.
        Should be replaces with Redis function
    '''
    def txLock(self, redis, query:str, limit: int, uuid: str) -> str|None:
        resources = self.selectBatch(voc.TRANSACTION, query, limit)
        ret = {}                    
        try:            
            for doc in resources.docs:
                if voc.TRANSACTION in doc.id:
                    map:dict = redis.hgetall(doc.id)
                    map[voc.PROCESSOR_UUID] = uuid
                    map[voc.STATUS] = voc.LOCKED
                    redis.hset(doc.id, mapping=map)
        except:
            logging.error('ERROR: There is no data to process.')


    '''
        Running provided search query
    '''
    def search(self, redis, index: str, query: str, limit: int = 1000) -> dict|None:
        # _query: Query = Query(query).no_content(True).paging(0, limit)
        # if query_params == None:
        #     result = redis.ft(index).search(_query)
        #     doc: Document = result.docs[0]
        #     doc.id
        #     return result
        # else:
            return redis.ft(index).search(query)

    
    '''
        Select batch of records (list of full_id's) from TRANSACTION index for processing
    '''
    def selectBatch(self, redis, idx_name: str, query:str, limit: int = 10):
        _query = Query(query).no_content().paging(0, limit)
        return redis.ft(idx_name).search(_query)
    
    # extract field value from search result
    def docIdList(self, result) -> list|None:
        if result.docs == None or len(result.docs) == 0:
            return None
        else:
            _list = []
            for doc in result.docs:
                _list.append(doc.id)
                
            return _list

    '''
        Tokenizes provided file, updates BIG_IDX, and Creates HLL in Redis for the document
        by running Redis function "big_idx_update"

        id - is full or normilized id of the item
        file_name - is a name of the file that contains text to be processed
        bucket - is a number of chars from beginning of the sha1 part of the id that will be used as a bucket id
        batch - is a number of words to be processed in one transaction
    '''
    def parseDocument(self, redis, id, file_name, bucket:int = 2, batch:int = 7000) -> int|None:
        text = self.getText(file_name)        
        if text == None:
            logging.error(f'Cannot read file: {file_name}')
            return None
        else:
            return self.parseText(redis, id, text, bucket, batch)            
        

    def parseText(self, redis, id, text:str, bucket:int = 2, batch:int = 7000) -> int|None:
        if isinstance(text, bytes):
            text = str(text.decode())
        else:
            text = str(text)
        
        tokens = set(text.split())
        # set(nltk.word_tokenize(text))
        t_set = set()
        _digits = re.compile('\d')
        for word in tokens:
            if isinstance(word, bytes):
                word = re.sub("[\n\t\.:;\,'\"\[\]\{\}]", " ", str(word.decode()).lower())
            else:
                word = re.sub("[\n\t\.:;\,'\"\[\]\{\}]", " ", str(word).lower())

            for w in word.split():
            # set(nltk.word_tokenize(word)):
                if not bool(_digits.search(w)) and len(w) > 2:
                    t_set.add(w)

        norm_id = utl.normId(id)
        
        function = "big_index_update"
        t_list = list(t_set)
        for i in range(0, len(t_list), batch):
            keys = [norm_id, bucket]
            keys.extend(t_list[i:i + batch])            
            redis.fcall(function, 2, *keys)

        hll_pref = 'hll_' + utl.getIdPrefix(norm_id)
        _hll_id = utl.underScore(utl.fullId(hll_pref, utl.getIdShaPart(norm_id)))
        count = redis.pfcount(_hll_id) 
        print(_hll_id, ': ', count)  
        
        return count
    
    def getText(self, file_path:str) -> str|None:
        _file_name, file_extension = os.path.splitext(file_path)
        text = None
        if file_extension.lower() == '.yaml' or file_extension.lower() == '.yml':
            text = utl.getSchemaFromFile(file_path) 
        elif file_extension.lower() in voc.TEXTRACT_EXT:
            try:
                text = textract.process(file_path, encoding='utf-8')
            except:
                logging.error('Cannot process file: ' + file_path)
                return None
        else:
            # read file to string
            try:
                with open(file_path, mode = 'r', encoding="utf-8", errors="backslashreplace") as file:
                    text = file.read()
            except:
                logging.error('Cannot read as texr file: ' + file_path)
                return None
            
        return text

    # Create commit
    def createCommit(self, redis) -> str|None:
        t_stamp = time.time()
        c_map: dict = {
            'timestamp': t_stamp,
            'committer_name': cnf.settings.commit.committer_name,
            'committer_email': cnf.settings.commit.committer_email,
            'doc:': ' ',
            'commit_id': 'und',
            'commit_status': 'und'
        }
        c_prefix = voc.COMMIT
        c_key_list = [voc.TIMESTAMP, voc.COMMITTER_NAME, voc.COMMITTER_EMAIL]
        sha1_id = self.updateRecordHash(redis, c_prefix, c_key_list, c_map)

        return sha1_id, t_stamp

    # commit transaction
    def commit(self, redis, commit_id:str, timestamp:str, tx_keys:list) -> int|None:        
        function = "commit"
        keys = [commit_id, timestamp]
        keys.extend(tx_keys)

        return redis.fcall(function, 2, *keys)
    
    def submit(self, redis, proc:str, status:str, limit:int) -> int|None:
        # Commit Processed indices
        #=======================================================
        # 1. Create commit instance in 'commit' redisearch index
        c_sha_id, t_stamp = self.createCommit(redis)

        # 2. Commit all processed files 
        query = f'(@processor_ref:{proc} @status:{status})'       
        committed = False
        i = 0
        while(True):
            i += 1
            # redis, idx_name: str, query:str, limit: int = 100
            keys = self.selectBatch(redis, 'transaction', query, limit)
            _list = self.docIdList(keys) 
            if _list == None or len(_list) == 0 or i > limit:                    
                break
            else:
                # commit command returns True if some documents were committed
                committed = committed or eval(self.commit(redis, c_sha_id, t_stamp, _list).capitalize())
            keys = None
        
        if not committed:
            # Remove empty commit from commit index
            logging.error(f"Empty commit: {c_sha_id} {t_stamp}")
            redis.delete(utl.fullId('commit', c_sha_id))

        print('>>>>>>>', i)

        
    # convert list to set
    def listToSet(self, redis, list_name, list) -> int|None:
        return redis.sadd(list_name, *list)
    
    # convert list to HLL
    def listToHll(self, redis, list_name, list) -> int|None:
        return redis.pfadd(list_name, *list)
    
    # convert b'aaa' to 'aaa'
    def bytesToStr(self, bytes_list) -> list:
        return [bytes.decode('utf-8') for bytes in bytes_list]
    
    # tokenize bytelist
    def tokenize(self, bytes_list) -> list:
        return [self.bytesToStr(word).strip(string.punctuation) for word in bytes_list.split()]
    
    # check if word is not num
    def isNotNum(self, word) -> bool:
        return not word.isnumeric()
    
    # read python list by chunks
    def chunks(self, l, n) -> list:
        for i in range(0, len(l), n):
            yield l[i:i + n]

    # load redis library
    def loadLib(self, redis, lib_path, replace:bool) -> None:
        code = ''
        # load file to string
        with open(lib_path, 'r') as file:
            code = file.read()
        # load redis library
        redis.function_load(code, replace=replace)
