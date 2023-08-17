import csv
import datetime
import importlib
import re
import string
import logging
import uuid

import os
import yaml
import hashlib
import redis
from redis.commands.search.field import TextField, NumericField, TagField

from cerberus import Validator

from metadatum.vocabulary import Vocabulary as voc

import sys
import inspect

from dataclasses import dataclass, field

currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir) 

import config as cnf

# from .config import settings

class Utils:

    doc_0 = {'props': {}}

    def decodeList(list: list) -> list:
        return [item.decode('utf8') for item in list]
    
    def decodeDict(dict: dict) -> dict:
        return {k.decode('utf8'): v.decode('utf8') for k, v in dict.items()}

    def timestamp():
        return datetime.datetime.now().strftime("%Y-%m-%d-%H:%M:%S")

    def importConfig():
        import sys
        import inspect
        currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
        parentdir = os.path.dirname(currentdir)
        sys.path.insert(0, parentdir)
        # import config 

    def runModuleByName(module_name: str, package: str, class_name: str, *args, **kwargs):
        try:
            module = importlib.import_module(module_name, package=package)
            class_ = getattr(module, class_name)
            instance = class_(*args, **kwargs)
            return instance.run()
        except ImportError as err:
            logging.error(f'Error: {err}')
            return None

    # Import module from package
    def importModule(module: str, package: str):
        try:
            if package == None or len(package) == 0:
                return importlib.import_module(module, '.')
            else:
                return importlib.import_module(module, package=package) 
        except ImportError as err:
            logging.error(f'Error: {err}')
            return None

    '''
        This method lists all files in the directory skipping sub-directories.
    '''
    def listDirFiles(self, dir: str) -> list|None:
        return [f for f in os.listdir(dir) if os.isfile(os.join(dir, f))]

    def listAllFiles(dir: str, ext: str) -> list|None:
        _ext = ext.replace('.', '')
        _list: list = []
        for root, dirs, files in os.walk(dir):
            for file in files:
                if file.endswith(f".{_ext}"):
                    _list.append(os.path.join(root, file))

        return _list

    def getSchemaFromFile(file_name):     
        with open(file_name, 'r') as file:
            return yaml.safe_load(file)

    def schema_name(file_name: str) -> str|None:
        return file_name.split('.')[0]

    def idxFileWithExt(schema: str) -> str|None:
        if schema.endswith('.yaml'):
            return
        else:
            return schema + '.yaml' 

    def getConfig(file_name: str|None) -> dict|None:
        config: dict | None
        try:
            with open(file_name, 'r') as file:
                config = yaml.safe_load(file)
        except:
            raise RuntimeError(f"Error: Problem with '{file_name}' file.")            
        return config

    def getDefaultDoc(schema_path: str) -> dict|None:
        v = Validator()
        v.schema = Utils.getSchemaFromFile(schema_path)

        return v.normalized(Utils.doc_0)
    
    def getProps(doc, schema_path):        
        v = Validator()
        
        p_dict: dict = {}
        if v.validate(Utils.doc_0, doc):
            n_doc = v.normalized(Utils.doc_0, doc)
            p_dict = n_doc.get(voc.PROPS).items() 
            return p_dict, n_doc
        else:
            logging.debug(f'Inavalid: {schema_path}')
            return None, None
    

    def ft_schema(schema: dict) -> tuple|None:
        dictlist = []
        tmp: str
        for key, value in schema:
            if value == 'tag':
                temp = TagField(key)
                dictlist.append(temp)
            elif str(value).isnumeric():
                temp = NumericField(key)
                dictlist.append(temp)
            else:
                temp = TextField(key)
                dictlist.append(temp) 
        return tuple(i for i in dictlist)

    # Generates SHA1 hash code from key fields of props 
    # dictionary
    def sha1(keys: list, props: dict) -> str|None:
        '''
            Append values from props dictionary with keys from "keys" list
        '''
        sha = ''
        if len(keys) == 0:
            sha+= str(uuid.uuid1).lower().replace(' ', '')
        else:
            for key in keys:
                # Normalize strings by turning to low case
                # and removing spaces
                if props.get(str(key)) != None:
                    sha+= str(props.get(str(key))).lower().replace(' ', '')

        m = hashlib.sha1()
        m.update(sha.encode())
        return m.hexdigest()
    
    # calculate SHA1 hash code from single string
    def sha1_str(string: str) -> str|None:
        if string == None:
            return None
        
        m = hashlib.sha1()
        m.update(string.lower().replace(' ', '').encode())
        return m.hexdigest()
   
    #    Entity ID manipulation support 
    '''
        Creating a full id in format: <prefix>:<sha_id>
            sha_id is SHA1 hash (20 characters hash code) generated from the key properties of the entity
        Extracting prefix from the full id
        Extracting sha_id from the full id
        Normalizing id by removing ':' from full id
        Denormalizing id by adding ':' to the normalized id
    '''
    def fullId(prefix:str, sha_id) -> str|None:
        if len(prefix) > 0 and len(sha_id) == 40:
            _prefix = Utils.prefix(prefix)
            return _prefix + sha_id
        else:
            return None

    '''
        Creates a normilized id from full id by removing ':'
    '''
    def normId(full_id:str) -> str|None:
        if full_id == None:
            return None
        
        if len(full_id.replace(':', '')) > 40:
            return full_id.replace(':', '')
        else:
            return None

    '''
        Restores full_id from normilized id 
    '''
    def denormId(norm_id:str) -> str|None:
        if norm_id == None:
            return None
        
        if ':' not in norm_id and len(norm_id) > 40:
            return norm_id[:-40] + ':' + norm_id[-40:]
        else:
            return None

    '''
        Extract prefix from full id, it works with full and normalized id
    '''
    def getIdPrefix(full_id:str) -> str|None:
        if full_id == None:
            return None
        
        _full_id = full_id.replace(':', '')
        if len(_full_id) > 40:
            return _full_id[:-40]
        else:
            return None

    '''
        Extract SHA1 part from the full id, it works with full and normalized id
    '''
    def getIdShaPart(full_id:str) -> str|None:
        if full_id == None:
            return None
        
        _full_id = full_id.replace(':', '')
        if len(_full_id) > 40:
            return _full_id[-40:]
        else:
            return None

    '''
        Misc methods
    '''
    def flat2dList(list: list) -> list:
        return [item for sublist in list for item in sublist]
    
    def prefix(term: str) -> str:
        if term.endswith(':'):
            return term
        else:
            return term + ':'

    def underScore(term: str) -> str|None:
        if term.startswith('_'):
            return term
        else:
            return '_' + term

    def csvHeader(file_path: str) -> bool|None:
        try:
            with open(file_path, mode = 'r', encoding="utf-8", errors="backslashreplace") as csvfile:
                return csv.Sniffer().has_header(csvfile.read(4096))
        except:
            logging.error('Error reading file')
            return None

    def replaceChars(text:str, replacement:str) -> str|None:
        chars = re.escape(string.punctuation)
        return re.sub(r'['+chars+']', replacement, text)

    def quotes(text: str) -> str:
        return "\'" + text + "\'"

    # get redis client
    def getRedisClient(db: int=0) -> redis.Redis|None:
        try:
            host = cnf.settings.redis.host
            port = cnf.settings.redis.port
            return redis.Redis(host=host, port=port, db=db)
        except:
            logging.error('Error: Redis connection failed.')
            return None
        
    # get item from dict by condition and concatenate values into string
    def dropAndJoin(dict: dict, skip_keys: str, separator: str) -> str:
        return separator.join([v for k, v in dict.items() if k not in skip_keys])