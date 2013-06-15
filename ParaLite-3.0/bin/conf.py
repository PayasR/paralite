# information that master send to each worker
CQID = 'cqid'
INPUT = 'input'
OUTPUT = 'output'
SOURCE = 'source'
DEST = 'dest'
EXPRESSION = 'expression'
HOST = 'master_hostname'
PORT = 'port'
NAME = 'name'
OPID = 'opid'
WORKERID = 'id'
ATTRS = 'attrs'
SPLIT_KEY = 'split_key'
ORDER_KEY = 'order_key'
ORDER_TYPE = 'order_type'
FUNCTION = 'function'
CLIENTS = 'clients'
MY_PORT = 'my_port'
P_NODE = 'p_node'
NODE = 'node'          
DATABASE = 'database'
NUM_OF_CHILDREN = 'num_of_children'
IS_CHILD_SQL = 'is_child_sql'
KEY = 'key'
GROUP_KEY = 'group_key'
TABLE = 'table'
MASTER_NAME = "master_name"
MASTER_PORT = "master_port"
DEST_CLIENT_NAME = "dest_client_name"
DEST_CLIENT_PORT = "dest_client_port"
UDX = "udxes"
OUTPUT_DIR = "output_dir"
DEST_TABLE = "dest_table"
CACHE_SIZE = "cache_size"
TEMP_STORE = "temp_store"
DB_SIZE = "db_size"
ADDED_RECORD = "added record"
ADDED_SIZE = "added size"
STATUS = "status"
CLIENT_SOCK = "client_sock"
DB_COL_SEP = "db_col_sep"
DB_ROW_SEP = "db_row_sep"
DEST_DB = "dest_db"
LOCAL_ADDR = "local socket addr"
BLOCK_SIZE = "block_size"
PARTITION_NUM = "partition_num"
IS_CHECKPOINT = "is_checkpoint"
PREDICTION = "prediction"
FAILED_NODE = "failed_node"
LIMIT = "limit"

# Signal among master, worker node and data node
ACK = 'ACK'
REG = 'REG'
KAL = 'KAL' # keep alive
DATA_NODE = 'datanode'
WORKER_NODE = 'workernode'
DLOADER_SERVER = "the data load server"
CLIENT = 'client'
PARALITE_QUIT = 'quit' or 'QUIT'
REQ = 'request'
DBM = 'db modified'
QUE = "db query"
INFO = "metadata_info"
DB_INFO = "the output option of the database"
DETECT_INFO = "detect the master started"
KILL = "kill the master"
MASTER_READY = "the master is ready for message"
EXIT = "the master exits"
JOB_ARGUMENT = "the argument for each job"
JOB = "a single job"
JOB_END = "job end"
RS = "the result info"
DATA = "DATA"
DATA_END = "DATA END"
DATA_PERSIST = "DATA PERSIST DECISION"
DATA_REPLICA = "DATA REPLICA"
DATA_DISTRIBUTE = "DATA DISTRIBUTION"
DATA_DISTRIBUTE_UDX = "DATA DISTRIBUTION UDX"
NODE_FAIL = "NODE FAILIURE"
DLOAD_REPLY = "DLOAD REPLY"
METADATA_INFO = "METADATA INFO"

# job or operator status
NOT_READY = 'not ready'
READY = 'ready'
RUNNING = 'running'
WAITING = "waiting"
PENDING = "pending"
FINISH = 'finish'
FAIL = 'fail'
ABNORMAL = 'node abnormal'
NORMAL = 'node normal'
DONE = 'done'
IDLE = 'idle'
BUSY = 'busy'
WAIT = 'wait for data'

# message info
MESSAGE_TYPE = 'msg_type'
REQUEST_INFO = 'request_info'
JOB_STATUS_INFO = 'job_status_info'

# data load type
LOAD_FROM_CMD = "load from cmd"
LOAD_FROM_API = "load from api"

# columns type
STRING = 'VARCHAR'
INT = 'INTEGER'
FLOAT = 'FLOAT'
REAL = "REAL"
TEXT = 'TEXT'

# file seperator
SEPERATOR = '|'
DB_DEFAULT_ROW_SEP = "==#=="
DB_DEFAULT_COL_SEP = "|"
SEP_IN_MSG = "==%"

# metadata tables
SETTING_INFO = "setting_info"
DATA_NODE_INFO = "data_node_info"
TABLE_SIZE_INFO = "table_size_info"
TABLE_ATTR_INFO = "table_attr_info"
TABLE_POS_INFO = "table_pos_info"
TABLE_PARTITION_INFO = "table_partition_info"
SUB_DB_INFO = "sub_db_info"
DB_REPLICA_INFO = "db_replica_info"

# result data's destination
DATA_TO_ANO_OP = "piplining data"
DATA_TO_CLIENTS = "distribute data to clients"
DATA_TO_LOCAL = "send reply to client"
DATA_TO_ONE_CLIENT = " data to one client"
DATA_TO_DB = "load data into database"
NO_DATA = "no data is produced"

# keyword in udx
STDIN = "STDIN"
STDOUT = "STDOUT"
NEW_LINE = "\n"
EMPTY_LINE = "\n\n"
NULL = ""
INPUT_ROW_DELIMITER = "INPUT_ROW_DELIMITER"
INPUT_COL_DELIMITER = "INPUT_COL_DELIMITER"
INPUT_RECORD_DELIMITER = "INPUT_RECORD_DELIMITER"
OUTPUT_ROW_DELIMITER = "OUTPUT_ROW_DELIMITER"
OUTPUT_COL_DELIMITER = "OUTPUT_COL_DELIMITER"
OUTPUT_RECORD_DELIMITER = "OUTPUT_RECORD_DELIMITER"
AS = "AS"

# SQL related 
GENERAL_FUNC = ["SUM", "sum", "TOTAL", "total", "AVG", "avg", "COUNT", "count", "MAX", "max", "MIN", "min", "mul"] 

# TAGS
END_TAG = "end_tag"
DLOAD_END_TAG = "dload_end_tag"

# Data partition fashion
HASH_FASHION = "hash"
RANGE_FASHION = "range"
REPLICATE_FASHION = "replicate"
ROUND_ROBIN_FASHION = "round robin"
FASHION = "fashion"
HASH_KEY = "hash_key"
HASH_KEY_POS = "hash_key_pos"

# dir
TEMP_DIR = "temp_dir"
LOG_DIR = "log_dir"
PORT_RANGE = "port_range"

# information about Fault Tolerance
CHECKPOINT = "True"
NOT_CHECKPOINT = "False"
SINGLE_BUFFER = "single buffer"
MULTI_BUFFER = "multi buffers"
MULTI_FILE = "multi files"

# output from process or socket
SOCKET_OUT = "output from socket"
PROCESS_STDOUT = "standard output from process"
PROCESS_STDERR = "standard error from process"

# join type
JOIN = "join"
LEFT_JOIN = "left join"

# sorting type
ASC = "ascending"
DESC = "descending"

# set task number for some operators (JOIN and GROUP)
task_num_for_join = 10
task_num_for_group_by = 10


