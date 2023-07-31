class Vocabulary:
    # Scripts and queries templates
    tx_batch = "@processor_id: '{parent_id}' @status: '{status}' @item_prefix: '{item_prefix}' @item_type: '{item_type}'"
    
    OK = {'status': "OK"}
    
    MDS_PY = 'MDS_PY'
    CORE_SCHEMAS = 'core_schemas'
    CORE_PROCESSORS = 'core_processors'
    CONFIG = 'config'
    PROCESSORS = 'processors'
    SCHEMAS = 'schemas'
    SCRIPTS = 'scripts'
    SQLITE_FILES = 'sqlite_files'

    # Core schemas
    IDX_REG = 'idx_reg'
    CONFIG_FILE = 'config'
    TRANSACTION = 'transaction'
    COMMIT = 'commit'
    COMMIT_TAIL = 'commit_tail'
    COMMIT_ID = 'commit_id'
    COMMIT_STATUS = 'commit_status'
    COMMITTER_NAME = 'committer_name'
    COMMITTER_EMAIL = 'committer_email'
    TIMESTAMP = 'timestamp'
    PROC_REG = 'proc_reg'
    BIG_IDX = 'big_idx'
    HLL = 'hll'

    # Schema top properties
    ID = '__id'
    PARENT_ID = 'parent_id'
    NAME = 'name'
    URL = 'url'
    NAMESPACE = 'namespace'
    PREFIX = 'prefix'
    LABEL = 'label'
    KIND = 'kind'
    KEYS = 'keys'
    PROPS = 'props'
    SOURCE = 'source'
    DOC = 'doc'
    SIZE = 'size'
    PACKAGE = 'package'
    SCHEMA = 'schema'
    SCHEMA_DIR = 'schema_dir'
    VERSION = 'version'
    LANGUAGE = 'language'
    LIMIT = 'limit'
    QUERY = 'query'
    TYPE = 'type'

    # TRANSACTION
    ITEM_NAMESPACE = 'item_namespace'
    ITEM_ID = 'item_id'
    ITEM_PREFIX = 'item_prefix'
    ITEM_TYPE = 'item_type'
    PROCESSOR_REF ='processor_ref'
    PROCESSOR_UUID = 'processor_uuid'
    PROCESSOR_PREFIX = 'processor_prefix'

    # Processing status    
    STATUS = 'status'
    WAITING = 'waiting'
    IN_PROCESS = 'in_process'
    LOCKED = 'locked'
    COMPLETE = 'complete'
    FAILED = 'failed'

    #  redis params
    REDIS = 'redis'
    REDIS_HOST = 'host'
    REDIS_PORT = 'port'

    # Data sources
    DIR = 'dir'
    FILE = 'file'
    FILE_TYPE = 'file_type'
    DB = 'db'
    DB_TYPE = 'db_type'