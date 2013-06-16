"""The master of ParaLite
@version: 3.0
@author: Ting Chen
"""
# import python standard modules 
import sys
import string
import random
import os
import math
import time
import traceback
import cPickle
import base64
import sqlite3
import re
import Queue
import shlex
import cStringIO
import logging
from socket import *
from collections import deque
from threading import Thread

# set the home direcotry of paraLite into system's path
sys.path.append(os.path.abspath(os.path.join(sys.path[0], os.path.pardir)))

# import modules from ParaLite
from lib import newparser
from lib import ioman, ioman_base
from lib.logger import ParaLiteLog
import conf

def ws(s):
    sys.stdout.write(s)
    
def es(s):
    sys.stderr.write(s)
    sys.stderr.close()

try:
    client_num = 0
    NODE_CONF = os.getcwd() + os.sep + "node.conf"
    MAIN_CONF = os.getcwd() + os.sep + "paraLite.conf"
    WORKING_DIR = sys.path[0] + os.sep
    master_port = 0
    master_node = gethostname()
    
    assert len(sys.argv) == 6
    public_node = sys.argv[1]
    public_port = string.atoi(sys.argv[2])
    metadata_db = sys.argv[3]
    master_port = string.atoi(sys.argv[4])
except Exception, e:
    es("ERROR: %s" % " ".join(str(s) for s in e.args))

class ParaLiteProcess:
    def __init__(self, cqid, pid, tag, opid, node, r_chans, w_chans, err):
        self.cqid = cqid
        self.pid = pid
        self.tag = tag
        self.opid = opid
        self.node = node
        self.status = -1  # -1: no status   0: success   1: failed   2:running
        self.r_chans = r_chans
        self.w_chans = w_chans
        self.err = err

class UserConfig:
    """Some configurations specified by user
    """
    def __init__(self):
        self.block_size = 0
        self.worker_num = []        # {opid, worker_num}
        self.cache_size = 0
        self.temp_store = 0
        self.temp_dir = None
        self.log_dir = None
        self.port_range = []
        self.node_port = {}

class BasicOp:
    def __init__(self):
        self.name = None
        self.id = None
        self.node = {}  # <port, node> --> port is unique and node may not be unique
        self.p_node = {} # <port, node>  for parent operator
        self.dest = conf.DATA_TO_ANO_OP
        self.expression = None # orginal string in sql
        self.input = []
        self.output = []
        self.children = []
        self.status = None
        self.split_key = []
        self.table_split_key = {} # the partition key for a table may be changed 
                                  # during the construction of logical plan
        self.keys = []   # key is the columns involved by this operator: table.attr
        self.tables = [] # the tables involved by this operator
        self.is_split = False  # the output of the operator is splitted or not
        self.is_merge = False  # the input of the operator is merged or not
        self.is_sql = None    # mark this operator should be merged into SQL or not
        self.is_visited = 0
        self.worker_num = 0
        self.related_col = []
        self.is_first_op = False
        self.is_sub_plan = False
        self.limit = 0
        self.distinct = False
        self.map = {}
        
        # for checkpointing
        self.sibling = None
        self.ftproperty = FTProperty()
        self.data_size = -1
        self.execution_time = 0.0
        self.is_checkpoint = None
        self.is_checkpoint_est = False
        self.ect = -1.0
        self.is_fail = False # this operator is not the first time to be started
        self.is_first_job = True # to sign the first job that returned for failed op
        self.finish_child = []  # store the id of child whose data has already send 
        
    def show_rec(self, indent):
        spaces = "-" * indent
        ParaLiteLog.info(("\n%s (- %s %s expression=%s input=%s output=%s key=%s split_key=%s tables=%s dest=%s)\n" % (spaces, self.name, self.id, self.expression, self.input, self.output, self.keys, self.split_key, self.tables, self.dest)))
        if self.children != []:
            for c in self.children:
                c.show_rec(indent + 1)

    def show(self):
        ParaLiteLog.info(self.get())

    def show_ftp_rec(self, indent, buf):
        spaces = "-" * indent
        ftp = self.ftproperty
        buf.write("\n%s (- %s %s ect=%s process=%s recovery=%s checkpoint=%s input=%s output=%s node=%s task=%s decision=%s)\n" % (spaces, self.name, self.id, ftp.ect, ftp.processing_time, ftp.recovery_time, ftp.checkpoint_time, ftp.input_tuple, ftp.output_tuple, ftp.node_num, ftp.task_num, ftp.checkpoint_value))
        if self.children != []:
            for c in self.children:
                c.show_ftp_rec(indent + 1, buf)

    
    def show_ftp(self):
        buf = cStringIO.StringIO()
        self.show_ftp_rec(0, buf)
        return buf.getvalue()
    
    def get_rec(self, indent, buf):
        spaces = "-" * indent
        if self.name == Operator.ORDER_BY:
            buf.write("\n%s (- %s %s expression=%s input=%s output=%s order_key=%s split_key=%s tables=%s dest=%s is_sql=%s node=%s p_node=%s)\n" % (spaces, self.name, self.id, self.expression, self.input, self.output, self.order_key, self.split_key, self.tables, self.dest, self.is_sql, self.node, self.p_node))
        elif self.name == Operator.GROUP_BY:
            buf.write("\n%s (- %s %s expression=%s input=%s output=%s group_key=%s key=%s split_key=%s tables=%s func=%s dest=%s is_sql=%s)\n" % (spaces, self.name, self.id, self.expression, self.input, self.output, self.group_key, self.keys, self.split_key, self.tables, self.function, self.dest, self.is_sql))
        elif self.name == Operator.JOIN:
            buf.write("\n%s (- %s %s expression=%s input=%s output=%s key=%s split_key=%s tables=%s dest=%s is_sql=%s is_sub_plan=%s)\n" % (spaces, self.name, self.id, self.prediction, self.input, self.output, self.keys, self.split_key, self.tables, self.dest, self.is_sql, self.is_sub_plan))
        elif self.name == Operator.UDX:
            buf.write("\n%s (- %s %s expression=%s input=%s output=%s key=%s split_key=%s tables=%s dest=%s udxes=%s)\n" % (spaces, self.name, self.id, self.expression, self.input, self.output, self.keys, self.split_key, self.tables, self.dest, str([udx.cmd_line for udx in self.udxes])))
            
        else:
            buf.write("\n%s (- %s %s expression=%s input=%s output=%s key=%s split_key=%s tables=%s dest=%s is_sql=%s)\n" % (spaces, self.name, self.id, self.expression, self.input, self.output, self.keys, self.split_key, self.tables, self.dest,self.is_sql))
        if self.children != []:
            for c in self.children:
                c.get_rec(indent + 1, buf)
                
    def get(self):
        buf = cStringIO.StringIO()
        self.get_rec(0, buf)
        return buf.getvalue()

    # copy the self tree to another tree
    def copy(self, tree1):
        tree1.name = self.name
        tree1.node = self.node
        tree1.dest = self.dest
        tree1.expression = self.expression
        tree1.input = []
        for i in self.input: tree1.input.append(i)
        tree1.output = []
        for o in self.output: tree1.output.append(o)
        tree1.children = []
        for c in self.children:
            child = BasicOp()
            c.copy(child)
            tree1.children.append(child)
        tree1.status = self.status
        tree1.keys = []
        for k in self.keys: tree1.keys.append(k)
        tree1.tables = []
        for t in self.tables: tree1.tables.append(t)
        tree1.is_split = self.is_split
        tree1.is_merge = self.is_merge
        tree1.is_sql = self.is_sql
        tree1.is_visited = self.is_visited
        tree1.p_node = {}
        tree1.split_key = self.split_key
        tree1.is_first_op = self.is_first_op
        tree1.is_sub_plan = self.is_sub_plan
    
class GroupbyOp(BasicOp):
    def __init__(self):
        BasicOp.__init__(self)
        self.name = Operator().GROUP_BY
        self.function = [] # store all aggregation functions
        """
        # if the child of this operation is sql, then a local aggregation is done
        # Of course, for avg(), we have to convert it into sum() and count()
        """
        self.is_child_sql = False 
        # either one column or several columns connected by db_col_sep
        self.group_key = []

    def copy(self, tree1):
        BasicOp.copy(self, tree1)        
        tree1.function = self.function
        tree1.is_child_sql = self.is_child_sql
        tree1.group_key = self.group_key
        
class JoinOp(BasicOp):
    def __init__(self):
        BasicOp.__init__(self)
        self.name = Operator().JOIN
        self.map = {} # {child_id : output}
        # the expression of join, in most cases, it only has one prediction which is
        # the expression of this node. But sometimes, we have more than one prediction
        # for one join node.
        self.prediction = []
        # the input and output for different predictions
        self.pred_input = []
        self.pred_output = [] 
        self.join_type = None # LEFT_JOIN, RIGHT_JOIN ... 
        self.operator = None  # =, < , > ...

    def copy(self, tree1):
        BasicOp.copy(self, tree1)        
        tree1.map = self.map
        
class OrderbyOp(BasicOp):
    def __init__(self):
        BasicOp.__init__(self)
        self.name = Operator().ORDER_BY
        self.order_key = []  
        self.order_type = []
        self.is_child_sql = False 

    def copy(self, tree1):
        BasicOp.copy(self, tree1)        
        tree1.order_key = self.order_key
        tree1.order_type = self.order_type
        tree1.is_child_sql = self.is_child_sql

class UDXOp(BasicOp):
    def __init__(self):
        BasicOp.__init__(self)
        self.name = Operator().UDX
        self.udxes = [] # store all udxes

    def copy(self, tree1):
        BasicOp.copy(self, tree1)
        tree1.udxes = self.udxes

class AggrOp(BasicOp):
    def __init__(self):
        BasicOp.__init__(self)
        self.name = Operator().AGGREGATE
        self.function = [] # store all udxes
        self.is_child_sql = False
        
    def copy(self, tree1):
        BasicOp.copy(self, tree1)
        tree1.function = self.function

class Operator:
    JOIN = 'join'
    PROJ = 'proj'
    SCAN = 'from'
    GROUP_BY = 'group_by'
    SUM = 'sum'
    COUNT = 'count'
    LIMIT = 'limit'
    UNION = 'union'
    WHERE = 'where'
    ORDER_BY = 'order_by'
    DISTINCT = 'distinct'
    AND = 'and'
    OR = 'or'
    VIRTUAL = 'VIRTUAL'
    SQL = 'sql'
    UDX = 'udx'
    AGGREGATE = "aggregate"
    # Compound Operator
    CO_UNION = 'union'
    CO_ALL = 'all'
    CO_INTERSECT = 'intersect'
    CO_EXCEPT = 'except'
        
class UDXDef:
    def __init__(self):
        self.cmd_line = None
        self.input = conf.STDIN
        self.output = conf.STDOUT
        self.input_row_delimiter = conf.NEW_LINE
        self.output_row_delimiter = conf.NEW_LINE
        self.input_col_delimiter = conf.NULL
        self.output_col_delimiter = conf.NULL
        self.input_record_delimiter = conf.NULL
        self.output_record_delimiter = conf.NULL
        
class FTProperty:
    def __init__(self):
        self.ect = -1
        self.processing_time = 0
        self.recovery_time = 0
        self.checkpoint_time = 0
        self.checkpoint_value = -1
        self.failure_probability = -1
        self.input_tuple = 0
        self.output_tuple = 0
        self.node_num = 0
        self.task_num = 0
        

class LogicalPlanMaker:
    def __init__(self, task):
        self.task = task
        self.sql = task.query
        self.parse_result = None

    def get_expression(self, stmt):
        r = random.randint(1, 1000)
        return str(r)
    """
    make() is to create a logical plan for each kind of SQL
    1. parse sql query using pyparsing and obtain parsed result--semantic_proc
    2. make the logical plan
    """
    def make(self):
        try:
            ParaLiteLog.info(self.sql)
            parse_result = newparser.parse(self.sql)
            if parse_result is None:
                return 1, "The syntax of the query may be wrong, please check it!"
            ParaLiteLog.info(parse_result.dump())
            
            if self.sql.lower().startswith('select'):
                plan, err_type, err_info = self.mk_select_plan(parse_result, None)
                if plan is None:
                    return err_type, err_info
                self.task.plan = plan
                
            elif self.sql.lower().startswith('create'):
                err_type, err_info = self.mk_create_plan(parse_result)

            elif self.sql.lower().startswith('drop'):
                err_type, err_info = self.mk_drop_plan(parse_result)
                
            else:
                es('please enter right sql\n')
            self.task.plan.show()
            return err_type, err_info
        except Exception, e:
            ParaLiteLog.info(traceback.format_exc())
            ex = "ERROR: %s\n" % (traceback.format_exc())
            return 1, ex

    def mk_create_plan(self, stmt):
        if stmt.index != '':
            return self.mk_create_index_plan(stmt)
        else:
            return self.mk_create_table_plan(stmt)

    """
    ['CREATE', 'INDEX', 'ina', 'ON', 'y', ['a', 'b']]
    - column: ['a', 'b']
    - index: ['ina']
    - table: ['y']
    """
    def mk_create_index_plan(self, stmt):
        column = stmt.column
        index_name = stmt.index
        table = stmt.table
        tree = BasicOp()
        node = BasicOp()
        node.name = Operator.SQL
        node.expression = self.sql
        node.tables = table
        node.id = str(0)
        tree = node
        self.task.plan = tree
        InfoCenter().update_table_info(self.task.database, table[0], column,
                                        index_name[0])
        return -1, None
    
    """
    table = ['t1']
    column = [['a','int'], ['PRIMARY','KEY'], ['b','int'], ['c', 'varchar', '20']]
    select = select statemate
    """
    def mk_create_table_plan(self, stmt):
        err_type, err_info = self.make_create(stmt)
        if err_type != -1:
            return err_type, err_info
        if self.task.id == None:
            # this is .analyze command
            return
        if stmt.column:
            table_name = stmt.table[0]
            if stmt.table_type != "" and stmt.fts[1].lower() == "fts4aux":
                column = [["term", "TEXT"],["col", "TEXT"],["documents", "TEXT"],
                          ["occurrences", "int"]]
            else:
                column = stmt.column
        elif stmt.select:
            table_name = stmt.table[0]
            column = []
            column_for_table_create = []
            for col in self.task.plan.output:
                # first check if the col has alias
                c = []
                if col in self.task.column_alias:
                    alias = self.task.column_alias[col]
                    for a in alias:
                        if a.find(".") != -1:
                            a = a[a.find(".") +1:]
                        column_for_table_create.append(a)
                        c = []
                        c.append(a)
                        c.append("TEXT")
                        column.append(c)
                    continue
                new_col = col
                if col.find("(") != -1:
                    new_col = col[col.find("(")+1:col.find(")")]
                if new_col.find(".") != -1:
                    c.append(new_col.split(".")[1])
                    column_for_table_create += (new_col.split(".")[1], )
                else:
                    c.append(new_col)
                    column_for_table_create += (new_col,)
                c.append("TEXT")
                column.append(c)
            if stmt.table_type != '':
                sql_create = "create %s table %s %s (%s)" % (stmt.table_type,
                                                             table_name,
                                                             " ".join(stmt.fts),
                                                             ",".join(column_for_table_create))
                if stmt.fts[1].lower() == "fts4aux":
                    column = [["term", "TEXT"],["col", "TEXT"],["documents", "TEXT"],
                              ["occurrences", "int"]]
            else:
                if self.sql.lower().find("if not exists") != -1:
                    sql_create = "create table if not exists %s (%s)" % (
                        table_name, ",".join(column_for_table_create))
                else:
                    sql_create = "create table %s (%s)" % (
                        table_name, ",".join(column_for_table_create))

            queue = deque([])
            queue.append(self.task.plan)
            #find the last op
            while list(queue) != []:
                op = queue.popleft()
                for child in op.children:
                    queue.append(child)
            assert op.name == Operator.SQL
            new_sql_op = BasicOp()
            new_sql_op.name = Operator.SQL
            new_sql_op.expression = sql_create
            new_sql_op.tables = [table_name]
            # set the dest for this operator to be DATA_TO_ANO_OP to avoid sending
            # ACK to the master. Because the master checks if the task is finished
            # or not by the successful operators in the plan. If this operator sends
            # ACK to the master, it will cause errors.
            new_sql_op.dest = conf.DATA_TO_ANO_OP
            new_sql_op.id = str(string.atoi(op.id) + 1)
            #op.children = [new_sql_op]
            new_sql_op.children = [self.task.plan]
            self.task.plan = new_sql_op
        else:
            assert(0), stmt

        if stmt.node != '':
            N = stmt.node
            nodes = []
            for node in N:
                Nn = self.parse_data_node(node)
                for n in Nn:
                    nodes.append(n)
        elif stmt.file != '':
            f = stmt.file[0]
            nodes = self.process_data_node_file(f)
        else:
            nodes = [gethostname()]
        """
        first check if all nodes are available 
        """
        self.get_available_node(nodes)
        key = ''
        if stmt.hash_key != []:
            key = ",".join(stmt.hash_key)
        """
        all tables should have the same replicas and chunks
        """
        chunk_in_db = InfoCenter().get_chunk_num(metadata_db)
        replica_in_db = InfoCenter().get_replica_num(metadata_db)
        if stmt.chunk_num != '':
            chunk_num = string.atoi(stmt.chunk_num)
            if chunk_in_db != 1 and chunk_num != chunk_in_db:
                raise Exception("""You have already set the chunk n
                                   umber and you are not allowed to
                                   set a different one again!""")
        else: chunk_num = chunk_in_db
        if stmt.replica_num != '':
            replica_num = string.atoi(stmt.replica_num)
            if replica_in_db != 1 and replica_num != replica_in_db:
                raise Exception("""You have already set the replica
                                   number and you are not allowed to
                                   set a different one again!""")
            if replica_in_db == 1 and replica_num > len(nodes):
                raise Exception("""The replica number is larger than
                                   the total number of data nodes,
                                   please use a smaller number!""")
        else: replica_num = replica_in_db

        # update the metadata info of the table if it is necessary
        if self.sql.lower().find("if not exists") != -1 and InfoCenter().get_table_columns(self.task.database, table_name) != []:
            return -1, None
        InfoCenter().insert_table_info(self.task.database, table_name, column,
                                       nodes, chunk_num, replica_num, key)
        return -1, None
    
    def get_available_node(self, nodes):
        return nodes
            
    def process_data_node_file(self, f):
        df = open(f, "rb")
        nodes = []
        for line in df:
            N = self.parse_data_node(line.strip())
            for n in N:
                nodes.append(n)
        return nodes

    def parse_data_node(self, node):
        # look for istbs[[xxxx]]
        m = re.match("(?P<prefix>.*)\[\[(?P<set>.*)\]\](?P<suffix>.*)", node)
        if m is None: return [ node ]
        prefix = m.group("prefix")
        suffix = m.group("suffix")
        fields = re.split("(,|;|:|!)",  m.group("set"))
        S = []
        sign = 1
        for f in fields:
            if f == ",":
                sign = 1
            elif f == ";" or f == ":":
                sign = -1
            else:
                # "10-20" or "10"
                m = re.match("(?P<a>\d+)((?P<m>-+)(?P<b>\d+))?", f)
                if m is None:
                    return None         # parse error
                else:
                    # "10-20" -> a = 10, b = 20
                    # "10"    -> a = 10, b = 10
                    m_str = m.group("m") # -, --, ...
                    a_str = m.group("a")
                    b_str = m.group("b")
                    if b_str is None: b_str = a_str
                    if m_str is None: m_str = "-"
                    a = string.atoi(a_str)
                    if len(m_str) > 1:
                        b = string.atoi(b_str) - 1
                    else:
                        b = string.atoi(b_str)
                    # a-b represents [a,b]. 
                    for x in range(a, b + 1):
                        x_str = "%d" % x
                        if b_str is not None and len(a_str) == len(b_str):
                            x_str = string.zfill(x_str, len(b_str))
                        if sign == 1:
                            S.append(x_str)
                        else:
                            if x_str in S:
                                S.remove(x_str)
        H = []
        for x_str in S:
            for y in self.parse_data_node("%s%s%s" % (prefix, x_str, suffix)):
                H.append(y)
        return H
 
    def make_create(self, stmt):
        if stmt.select == '':
            tree = BasicOp()
            node = BasicOp()
            node.name = Operator.SQL
            sql = self.sql
            if stmt.node != '' or stmt.file != '':
                sql = sql[0:self.sql.find(" on ")]
            if stmt.hash_key != '':
                sql = sql[0:self.sql.find(" partition by")]
            if stmt.chunk_num != '':
                sql = sql[0:self.sql.find(" chunk ")]
            if stmt.replica_num != '':
                sql = sql[0:self.sql.find(" replica ")]
            node.expression = sql
            node.tables = stmt.table
            node.id = str(0)
            tree = node
            tree.dest = None
        else:
            self.task.create_table = stmt.table[0]
            tree, err_type, err_info = self.mk_select_plan(stmt.select, None)
            ParaLiteLog.debug(err_info)
            if not tree:
                es(err_info)
                return 1, err_info
            tree.dest = conf.DATA_TO_DB
            tree.map[conf.DEST_TABLE] = stmt.table[0]            
        self.task.plan = tree
        return -1, None
    
    def mk_drop_plan(self, stmt):
        node = BasicOp()
        node.name = Operator.SQL
        node.expression = self.sql
        node.id = str(0)
        node.dest = conf.NO_DATA
        node.tables = stmt.table
        data_node = InfoCenter().get_data_node_info(metadata_db, stmt.table[0])
        self.task.datanode = data_node
        if data_node == []:
            if self.sql.lower().find("if exists") == -1:
                return 1, "Error: table %s is not exists\n" % (stmt.table[0])
            return 0, "Error: table %s is not exists\n" % (stmt.table[0])
        self.task.plan = node
        self.task.table = stmt.table[0]
        self.task.tag = "DROP"
        return -1, None

    def mk_udx_node(self, stmt, udx_node, arg_list, udx_stmt):
        # five cases:
        # (1): F(a) (2): F(a, b, ...) (3): F(x.a) (4): F(G(x.a)) (5): F(G(a))
        # ['F', 'G', ['a'], ['b']], ['F', ['x', '.', 'a']]
        for ele in stmt:
            if isinstance(ele, str):
                # this is a command
                one_udx_stmt = None
                for u in udx_stmt:
                    if u[0] == ele:
                        one_udx_stmt = u
                        break
                if one_udx_stmt is None:
                    return (1, ParaLiteException().NO_UDX_DEF)
                udx = self.parse_udx(one_udx_stmt)
                udx_node.udxes.append(udx)
            else:
                # this is a column name
                col_name = "".join(ele)
                arg_list.append(col_name)
        return -1, None
                
    def mk_groupby_node(self, aggr_node, group_node, group_stmt, subcol, col):
        # subcol: sum(a*b); avg(a)
        # col: sum(a*b) + 1; avg(a) + sum(c)
        # firstly, parse subcol to get related columns
        related_cols = []
        arg = subcol[subcol.find("(") + 1 : subcol.rfind(")")]
        table = self.task.table
        if arg != "*":
            args = newparser.parse_column_expr(arg)
            for subarg in args:
                if re.search("^[a-zA-Z][a-zA-Z0-9_.]*$", subarg) is not None:
                    if subarg.find(".") == -1:
                        #subarg = "%s.%s" % (self.task.table, subarg)
                        table = self.task.table
                    else:
                        table = subarg.split(".")[0]
                    if subarg not in related_cols:
                        related_cols.append(subarg)
        if group_stmt != "":
            if group_node.tables == []:
                group_node.related_col = related_cols
                for key in group_stmt:
                    temp_col = "".join(key)
                    """
                    if temp_col.find(".") == -1:
                        temp_col = table + "." + temp_col
                    """
                    group_node.group_key.append(temp_col)
                    if temp_col.find(".") != -1:
                        group_node.tables.append(temp_col.split(".")[0])
                    group_node.expression = ",".join(group_node.group_key)
                if group_node.tables == [] and self.task.table is not None:
                    group_node.tables.append(self.task.table)
                    
            if "".join(col) not in group_node.function:
                group_node.function.append("".join(col))
        else:
            aggr_node.function.append("".join(col))
            aggr_node.output.append("".join(col))
            aggr_node.related_col = related_cols
            aggr_node.tables.append(table)
            aggr_node.expression = "".join(col)
            #aggr_node.is_sql = 0

    def mk_select_plan(self, stmt, stmt_dic):

        if stmt is not None and stmt_dic is None:
            stmt_dic = {}
            stmt_dic["select"] = stmt.select.asList()
            #stmt_dic["select"] = stmt.select
            stmt_dic["select_type"] = stmt.select_type
            stmt_dic["from"] = stmt.fromList.asList()
            #stmt_dic["from"] = stmt.fromList
            if stmt.where != "": stmt_dic["where"] = stmt.where.asList()
            #if stmt.where != "": stmt_dic["where"] = stmt.where
            if stmt.group_by != "": stmt_dic["groupby"] = stmt.group_by.asList()
            #if stmt.group_by != "": stmt_dic["groupby"] = stmt.group_by
            if stmt.order_by != "": stmt_dic["orderby"] = stmt.order_by.asList()
            #if stmt.order_by != "": stmt_dic["orderby"] = stmt.order_by
            if stmt.udx != "": stmt_dic["udx"] = stmt.udx.asList()
            #if stmt.udx != "": stmt_dic["udx"] = stmt.udx
            if stmt.limit != "": stmt_dic["limit"] = stmt.limit

        try:
            plan, error_type, error_info = self.make_select(stmt_dic)
            if not plan:
                return plan, error_type, error_info
            
            plan, error_type, error_info = self.optimize(plan)
            if not plan:
                return plan, error_type, error_info
            
            plan.dest = conf.DATA_TO_ONE_CLIENT
            return plan, -1, None
        except:
            ParaLiteLog.info("ERROR: %s" % traceback.format_exc())
            return None, 1, traceback.format_exc()

    """
    make the execution plan for select statement
    arguments: stmt -- the parseResult from pyparsing
               stmt_dic -- the dictionary that stores all related parsed result; this is
                           useful when stmt is None.
    """
    def make_select(self, stmt_dic):
        tree = BasicOp()
        srclist = stmt_dic["from"]
        first_node = None
        if srclist != '':
            """
            srclist: [ele1, ele2 ..., elen]
            ---------------------------------------
            ele1: 'FROM', [['at', 'A'], [['bt', 'B']]]
            --------------------------------------
            ele2: 'FROM', [[['SELECT', [['b']], 'FROM', [['x']]], 'A']]
            --------------------------------------
            ele3: 'FROM', [['at', 'A'], ['JOIN', ['bt', 'B'], 'ON', [['A', '.', 'a'], '=', ['B', '.', 'a']]]]
            """
            num = len(srclist)
            """
            join sql or nested sql
            """
            last_table = None
            for ele in srclist:
                if isinstance(ele[0], str):
                    # the first table or join
                    if ele[0].lower() == "join" or ele[0].lower().find(" join") != -1:
                        # join a table
                        if first_node is None:
                            first_node = BasicOp()
                            first_node.name = Operator.AND
                        if len(ele[1]) == 2:
                            self.task.table_alias[ele[1][1]] = ele[1][0]
                        if len(ele) == 2:
                            cur_table = ele[1][-1]
                            node_join = JoinOp()
                            node_join.tables = [last_table, cur_table]
                            child1 = BasicOp()
                            child1.name = Operator.SCAN
                            child1.expression = last_table
                            child1.tables = [last_table]
                            child2 = BasicOp()
                            child2.name = Operator.SCAN
                            child2.expression = cur_table
                            child2.tables = [cur_table]
                            node_join.children = [child1, child2]
                        else:
                            node_join = self.get_WHERE_CLAUSE_node(ele[3])

                        if node_join.operator != "=" and node_join.operator != "==":
                            return None, 1, ParaLiteException().NON_EQUI_JOIN_ERROR
                        if ele[0].lower() == "join":
                            node_join.join_type = conf.JOIN
                        else:
                            return None, 1, ParaLiteException().NON_EQUI_JOIN_ERROR
                            
                        """
                        adding a AND node to connect the explicit join 
                        conditions and where clause
                        """
                        first_node.children.append(node_join)
                        first_node.tables += node_join.tables
                        self.task.table = srclist[0][0]
                    else:
                        if len(ele) > 1:
                            self.task.table_alias[ele[-1]] = ele[0]
                        last_table = ele[-1]
                else:
                    # second table or select sub-query
                    if ele[0][0] == "SELECT":
                        # sub-query
                        cond = ele[0]
                        sub_stmt_dic = {}
                        for i in range(len(cond)):
                            subcond = cond[i]
                            if not isinstance(subcond, str):
                                continue
                            if subcond == "SELECT":
                                if isinstance(cond[i+1], str) and cond[i+1] in ["DISTINCT", "ALL"]:
                                    sub_stmt_dic["select_type"] = cond[i+1]
                                    sub_stmt_dic["select"] = cond[i+2]
                                else: sub_stmt_dic["select"] = cond[i+1]
                            elif subcond == "FROM": sub_stmt_dic["from"] = cond[i+1]
                            elif subcond == "WHERE": sub_stmt_dic["where"] = cond[i+1]
                            elif subcond == "GROUP": sub_stmt_dic["groupby"] = cond[i+2]
                            elif subcond == "WITH": sub_stmt_dic["udx"] = cond[i+1]
                            elif subcond == "ORDER": sub_stmt_dic["orderby"] = cond[i+2]
                            elif subcond == "LIMIT": sub_stmt_dic["limit"] = cond[i+1]
                        sub_plan, err_type, err_info = self.mk_select_plan(None, sub_stmt_dic)
                        sub_plan.is_sub_plan = True
                        sub_plan.dest = conf.DATA_TO_ANO_OP
                        tree = sub_plan
                        if len(ele) > 1:
                            self.task.table = ele[-1]
                            tmp_table = ele[-1]
                        else:
                            if self.task.create_table is not None:
                                self.task.table = self.task.create_table
                            else:
                                self.task.table = "_t1"
                            tmp_table = "_T_"
                        self.task.tmp_table[tmp_table] = []
                        for tmp_out in sub_plan.output:
                            if tmp_out in self.task.column_alias:
                                r_t_o = self.task.column_alias[tmp_out][0]
                            else:
                                r_t_o = tmp_out
                            self.task.tmp_table[tmp_table].append(r_t_o)
                    else:
                        if len(ele[0]) > 1:
                            self.task.table_alias[ele[0][-1]] = ele[0][0]

            if first_node is not None:
                tree = first_node
            if num == 1 and isinstance(srclist[0][0], str):
                self.task.table = srclist[0][0]
                self.task.datanode = InfoCenter().get_data_node_info(
                    metadata_db, self.task.table)
                self.task.table_column[self.task.table] = InfoCenter().get_table_columns(metadata_db, self.task.table)
                if self.task.table not in InfoCenter().get_table_info(metadata_db).split("\n"):
                    ParaLiteLog.info("ERROR: %s is not exist" % (self.task.table))
                    for addr in self.task.reply_sock:
                        sock = socket(AF_INET,SOCK_STREAM)
                        sock.connect(addr)
                        msg = "ERROR: %s is not exist" % (self.task.table)
                        sock.send("%10s%s" % (len(msg), msg))
                        sock.close()
                    sys.exit(1)
                if "where" not in stmt_dic:
                    scan_node = BasicOp()
                    scan_node.name = Operator.SCAN
                    scan_node.expression = " ".join(srclist[0])
                    scan_node.keys.append(" ".join(srclist[0]))
                    scan_node.tables += [srclist[0][0]]
                    if tree.name == None:
                        tree = scan_node
                    else:
                        scan_node.children.append(tree)
                        tree = scan_node
        if "where" in stmt_dic:
            node = self.get_WHERE_CLAUSE_node(stmt_dic["where"])
            if tree.name is not None:
                node_temp = BasicOp()
                node_temp.name = Operator.AND
                node_temp.children.append(tree)
                node_temp.children.append(node)
                for child in node_temp.children:
                    for table in child.tables:
                        node_temp.tables.append(table)
                tree = node_temp
            else:
                tree = node
        """
        # the form of select result is:
        select distinct x.a as aa, G(K(b)) as (bb, cc) from x with G= , K = 
        --------------------------
     - select: ['DISTINCT', ['x', '.', 'a'], 'AS', 'aa', ['G', 'K', 'b'] 'AS' ['bb', 'cc']]
     - func: ['G', 'K', 'b']
     - udx: [['G', 'test2'], ['K', 'test3']]
        --------------------------
        """
        assert "select" in stmt_dic
        select_type = ""
        if "select_type" in stmt_dic:
            select_type = stmt_dic["select_type"] # DISTINCT or ALL
        aggr_node = AggrOp()
        group_node = GroupbyOp()
        udx_node = None
        proj_node = BasicOp()
        proj_node.name = Operator.PROJ

        columns = stmt_dic["select"]
        i = 0
        while i < len(columns):
            col = columns[i]
            for subcol in col:
                if subcol.find("(") != -1 and subcol.find(")") != -1:
                    # a function
                    func = subcol.split("(")[0]
                    if func in conf.GENERAL_FUNC:
                        # general aggregation
                        group_stmt = ""
                        if "groupby" in stmt_dic:
                            group_stmt = stmt_dic["groupby"]
                        self.mk_groupby_node(aggr_node, group_node, group_stmt, subcol, col)
                        if proj_node.expression == None:
                            proj_node.expression = "".join(col)
                        else:
                            proj_node.expression += (','+ "".join(col))

                    else:
                        # UDX
                        if "udx" not in stmt_dic:
                            return None, 1, ParaLiteException().NO_UDX_DEF
                        arg_list = []
                        udx_node = UDXOp()

                        udx_expr = newparser.parse_udx_expr(subcol)

                        err_type, err_info = self.mk_udx_node(udx_expr,
                                                              udx_node,
                                                              arg_list,
                                                              stmt_dic["udx"])
                        if err_type == 1:
                            return err_type, err_info
                        for arg in arg_list:
                            udx_node.keys.append(arg)
                        for udx in udx_node.udxes:
                            ParaLiteLog.info("CMD: ----%s" % udx.cmd_line)

                    if udx_node is not None:
                        udx_node.children.append(tree)
                        udx_node.is_sql = 0
                        for argnum in range(1,len(col)):
                            c = "".join(col)
                            udx_node.keys.append(c)
                            if c.find(".") != -1:
                                udx_node.tables.append(c.split(".")[0])
                        if udx_node.tables == []:
                            udx_node.tables = [self.task.table]
                        tree = udx_node
                      
                elif re.search("^[a-zA-Z][a-zA-Z0-9_.]*$", subcol) is not None:
                    """
                    if subcol.find(".") == -1:
                        subcol = self.task.table + "." + subcol
                    """
                    if self.task.table in self.task.table_column:
                        col_temp = subcol
                        if subcol.find(".") != -1:
                            col_temp = subcol.split(".")[1]
                        if col_temp != "*" and col_temp not in self.task.table_column[self.task.table]:
                            return None, 1, "ERROR: There is no column %s in table %s" % (subcol, self.task.table)
                    if proj_node.expression == None:
                        proj_node.expression = subcol
                    else:
                        proj_node.expression += (',' + subcol)
                    proj_node.keys.append(subcol)
                else:
                    continue
            column = "".join(col)
            # if column == "*" or column == "%s.*" % (self.task.table):
            #     all_cols = InfoCenter().get_table_columns(metadata_db,
            #                                            self.task.table)
            #     for cc in all_cols:
            #         proj_node.output.append(str(cc))
            #         proj_node.keys.append(str(cc))
            #if column.find("*") != -1:
            if column == "*":
                if len(column.split(".")) == 2:
                    curtbl = column.split(".")[0]
                else:
                    curtbl = self.task.table
                if curtbl is None:
                    return None, 1, "Please specify the table for the columns"
                all_cols = InfoCenter().get_table_columns(metadata_db, curtbl)
                for cc in all_cols:
                    proj_node.output.append("%s.%s" % (curtbl, cc))
                    proj_node.keys.append("%s.%s" % (curtbl, cc))
            else:
                proj_node.output.append("".join(col))
            # check if the column has alias
            if i + 1 < len(columns) and columns[i + 1] == conf.AS:
                alias = columns[i + 2]
                if isinstance(alias, str):
                    #if alias.find(".") == -1:
                    #    alias = "%s.%s" % (self.task.create_table, alias)
                    self.task.column_alias["".join(col)] = (alias,)
                else:
                    a = tuple()
                    for al in alias:
                        
                        #if al.find(".") == -1:
                        #    al = "%s.%s" % (self.task.create_table, al)
                        a += (al,)
                    self.task.column_alias["".join(col)] = a
                i += 3
            else:
                i += 1

        if group_node is not None and group_node.function != []:
            groupby = group_node
            groupby.children.append(tree)
            for t in tree.tables:
                if t not in groupby.tables:
                    groupby.tables.append(t)
            tree = groupby
        
            
        if aggr_node is not None and aggr_node.function != []:
            aggr_node.children = [tree]
            tree = aggr_node
        if tree.name != None:
            proj_node.children.append(tree)
            newt = []
            for t in proj_node.tables:
                if t not in newt:
                    newt.append(t)
            for t in tree.tables:
                if t not in newt:
                    newt.append(t)
            proj_node.tables = newt

        if select_type != "" and select_type.lower() == Operator.DISTINCT:
            dis_node = BasicOp()
            dis_node.name = Operator.DISTINCT
            dis_node.expression = Operator.DISTINCT
            dis_node.tables = tree.tables
            dis_node.children.append(proj_node.children[0])
            proj_node.children = []
            proj_node.children.append(dis_node)
            
        tree = proj_node
        
        if "orderby" in stmt_dic:
            # [['revenue'], 'DESC', ['O', '.', 'o_orderdate']]
            node = OrderbyOp()
            expr = ""
            s = stmt_dic["orderby"]
            for i in range(len(s)):
                ele = s[i]
                if isinstance(ele, str):
                    # order type
                    continue
                column = "".join(ele)
                #if self.task.table is None:
                    #if column.find(".") == -1:
                    #    column = "%s.%s" % (self.task.create_table, column)
                node.order_key.append(column)
                expr += column
                if i + 1 < len(s):
                    if isinstance(s[i + 1], str):
                        if s[i+1] == "DESC":
                            node.order_type.append(conf.DESC)
                        else:
                            node.order_type.append(conf.ASC)
                        expr += " " + s[i+1]
                    else:
                        node.order_type.append(conf.ASC)
                expr += ","
            # set the order type for the last column
            if not isinstance(s[len(s)-1], str):
                node.order_type.append(conf.ASC)
            node.expression = expr
            node.children = tree.children
            temp_col = expr
            """
            if temp_col.find(".") == -1:
                temp_col = self.task.table + "." + temp_col
            """
            node.tables += tree.tables
            node.is_sql = 0
            tree.children = [node]
        if "limit" in stmt_dic:
            node = BasicOp()
            node.name = Operator.LIMIT
            node.children = tree.children
            node.tables += tree.tables
            node.expression = stmt_dic["limit"]
            tree.children = [node]
        return tree, -1, None
        
    # the unit of where expression without 'and' or 'or'
    # the form is [['x', '.', 'a'], '>', '10'] or [['x', '.', 'a'], 'MATCH', 'protein'] or ['a', '>', '10']
 
    def get_WHERE_node(self,whereexpression):
        exp = ''
        """
        for i in range(len(whereexpression)):
            exp += "".join(whereexpression[i]) + " "
        """
        tables = []
        attrs = []
        is_in_exp = False # special for prediction: col in (a, b, c)
        operator = None   # to denote if this is a non-Equi-Join prediction
        for m in range (len(whereexpression)):
            cond = whereexpression[m]
            # cond should be one of
            # (1) column name (2) operator, e.g. '<='
            # (3) constant (4) sub-query (5) airthematic operation
            if isinstance(cond, str):
                # case (2) and (3)
                is_sub_query = 0
                if is_in_exp:
                    exp += cond + ","
                else:
                    exp += cond + " "
                if cond.lower() == "in":
                    exp += "("
                    is_in_exp = True
                if cond in ["<", ">", "!=", "<>", "<=", ">=", "=", "=="]:
                    operator = cond
                continue
            elif len(cond) == 1 or len(cond) == 3 and cond[1] == ".":
                # case (1)
                is_sub_query = 0                
                cond3 = "".join(cond)
                if is_in_exp:
                    exp += cond3 + ","
                else:
                    exp += cond3 + " "
                if cond3.find('.') != -1:
                    table = cond3.split('.')[0]
                else:
                    table = self.task.table
                attr = cond3
                flag = 0
                for element in tables:
                    if table == element:
                        flag = 1
                        break
                if flag == 0:
                    tables.append(table)
                    attrs.append(attr)
            elif len(cond) >= 3 and cond[1] in ["+", "-", "*", "/", "^"]:
                # case (5). Now we only support simple operation without (), e.g. a + 1
                for subcond in cond:
                    exp += "".join(subcond) + " "
                is_sub_query = 0
                continue

            elif cond[0].lower() == "date":
                exp += "%s(%s) " % (cond[0], ",".join(cond[1:]))
                is_sub_query = 0
            
            elif cond[0] == "SELECT":
                # case (4)
                stmt_dic = {}
                is_sub_query = 1
                for i in range(len(cond)):
                    subcond = cond[i]
                    if isinstance(subcond, str):
                        if subcond == "SELECT":
                            if isinstance(cond[i+1], str) and cond[i+1] in ["DISTINCT", "ALL"]:
                                stmt_dic["select_type"] = cond[i+1]
                                stmt_dic["select"] = cond[i+2]
                            else:
                                stmt_dic["select"] = cond[i+1]
                        elif subcond == "FROM":
                            stmt_dic["from"] = cond[i+1]
                        elif subcond == "WHERE":
                            stmt_dic["where"] = cond[i+1]
                        elif subcond == "GROUP":
                            stmt_dic["groupby"] = cond[i+2]
                        elif subcond == "WITH":
                            stmt_dic["udx"] = cond[i+1]
                        elif subcond == "ORDER":
                            stmt_dic["orderby"] = cond[i+2]
                        elif subcond == "LIMIT":
                            stmt_dic["limit"] = cond[i+1]
                self.task.pre_table = self.task.table
                subplan, error_type, error_info = self.mk_select_plan(None, stmt_dic)
                self.task.table = self.task.pre_table
                subplan.is_sub_plan = True
                exp += subplan.output[0] + " "
                
        if is_in_exp:
            if exp.endswith(","):
                exp = exp[:exp.rfind(",")]
            exp = exp.strip() + ")"
            
        if is_sub_query == 0:
            if len(tables) == 2:
                node = JoinOp()
                node.keys = attrs
                node.operator = operator
                child1 = BasicOp()
                child1.name = Operator.SCAN
                child1.expression = tables[0]
                child1.keys.append(attrs[0])
                child1.split_key = [attrs[0]]
                child1.tables.append(tables[0])
                child2 = BasicOp()
                child2.name = Operator.SCAN
                child2.keys.append(attrs[1])
                child2.split_key = [attrs[1]]
                child2.expression = tables[1]
                child2.tables.append(tables[1])
                node.children.append(child1)
                node.children.append(child2)
                node.tables += child1.tables
                node.tables += child2.tables
                node.expression = exp
                node.prediction.append(exp)
            else:
                node = BasicOp()
                node.name = Operator.WHERE
                node.keys = attrs
                child = BasicOp()
                child.name = Operator.SCAN
                child.expression = tables[0]
                if tables[0] not in child.tables:
                    child.tables.append(tables[0])
                for a in attrs:
                    child.keys.append(a)
                node.children.append(child)
                node.tables += child.tables
                node.expression = exp
        else:
            node = BasicOp()
            node.name = Operator.WHERE
            if tables != []: node.tables = tables
            node.keys = attrs
            node.expression = exp
            node.children = [subplan]
            
        return node

    def get_AND_node(self,andexpression):
        nodeAND = BasicOp()
        if andexpression[1].lower() != 'and' :
            nodeAND = self.get_WHERE_node(andexpression)
        else:
            nodeAND.name = Operator.AND
            for j in range (len(andexpression)):
                if j % 2 == 0:
                    cond2 = andexpression[j]
                    where_node = self.get_WHERE_node(cond2)
                    """
                    for key in where_node.keys:
                        nodeAND.keys.append(key)
                    """
                    nodeAND.children.append(where_node)
                    nodeAND.tables += where_node.tables
        return nodeAND

    def get_WHERE_CLAUSE_node(self, where):
        node = BasicOp()
        if len(where) > 1:
            if where[1].lower() == 'or':
                node.name = Operator.OR
                for i in range (len(where)):
                    if i % 2 == 0:
                        cond1 = where[i]
                        and_node = self.get_AND_node(cond1)
                        for key in and_node.keys:
                            node.keys.append(key)
                        node.children.append(and_node)

                        for t in and_node.tables:
                            if t not in node.tables:
                                node.tables.append(t)
            else:
                node = self.get_AND_node(where)
        else:
            node = self.get_WHERE_node(where)
        return node

    """
    udx: ['test', 'INPUT', 'STDIN', ...]
    """
    def parse_udx(self, udx):
        try:
            u = UDXDef()
            u.name = string.strip(udx[0])
            u.cmd_line = string.strip(udx[1])
            i = 2
            while i < len(udx) - 1:
                if udx[i] == conf.INPUT:
                    u.input = udx[i + 1]
                elif udx[i] == conf.OUTPUT:
                    u.output = udx[i + 1]
                elif udx[i] == conf.INPUT_ROW_DELIMITER:
                    if hasattr(conf, udx[i+1]):
                        u.input_row_delimiter = getattr(conf, udx[i+1])
                    else:
                        u.input_row_delimiter = udx[i+1].decode("string-escape")
                elif udx[i] == conf.INPUT_COL_DELIMITER:
                    if hasattr(conf, udx[i+1]):
                        u.input_col_delimiter = getattr(conf, udx[i+1])
                    else: u.input_col_delimiter = udx[i+1].decode("string-escape")
                elif udx[i] == conf.INPUT_RECORD_DELIMITER:
                    if hasattr(conf, udx[i+1]):
                        u.input_record_delimiter = getattr(conf, udx[i+1])
                    else: u.input_record_delimiter = udx[i+1].decode("string-escape")
                elif udx[i] == conf.OUTPUT_ROW_DELIMITER:
                    if hasattr(conf, udx[i+1]):
                        u.output_row_delimiter = getattr(conf, udx[i+1])
                    else: u.output_row_delimiter = udx[i+1].decode("string-escape")
                elif udx[i] == conf.OUTPUT_COL_DELIMITER:
                    if hasattr(conf, udx[i+1]):
                        u.output_col_delimiter = getattr(conf, udx[i+1])
                    else: u.output_col_delimiter = udx[i+1]
                elif udx[i] == conf.OUTPUT_RECORD_DELIMITER:
                    if hasattr(conf, udx[i+1]):
                        u.output_record_delimiter = getattr(conf, udx[i+1])
                    else: u.output_record_delimiter = udx[i+1].decode("string-escape")
                i += 2
            return u
        except:
            traceback.print_exc()

    def optimize(self, plan):
        try:
            ParaLiteLog.info("original ....")
            ParaLiteLog.info(plan.get())
            self.set_output_attrs(plan)
            ParaLiteLog.info("after set output")
            ParaLiteLog.info(plan.get())
            self.and_rule(plan)
            self.or_rule(plan)
            ParaLiteLog.info("after and/or rule")
            ParaLiteLog.info(plan.get())
            plan = self.split_rule(plan)
            ParaLiteLog.info("after split rule")
            ParaLiteLog.info(plan.get())
            plan = self.make_SQL(plan)
            ParaLiteLog.info("after make rule")
            ParaLiteLog.info(plan.get())
            plan = self.set_others(plan)
            ParaLiteLog.info("after set others")
            ParaLiteLog.info(plan.get())
            return plan, -1, None
        except:
            ParaLiteLog.info(traceback.format_exc())

    """
    AND rule: 
    (1) if and_node only has one child, delete and_node
    (2) if and_node does not have JOIN child, merge all children into one operator
    (3) if and_node has JOIN child(ren), move WHERE child to according JOIN child and merge all JOINs 
    """
    def and_rule(self, plan):
        queue = deque([])
        queue.append(plan)
        while list(queue) != []:
            operator = queue.popleft()
            for child in operator.children:
                if child.name == Operator.AND:
                    node = self.and_rule_op(child)
                    queue.append(operator)
                    operator.children.remove(child)
                    operator.children.append(node)
                else:
                    queue.append(child)

    def and_rule_op(self, and_node):
        for child in and_node.children:
            if child.name == Operator.AND:
                node = self.and_rule_op(child)
                pos = and_node.children.index(child)
                and_node.children[pos] = node

        if len(and_node.children) == 1:
            temp = and_node.children[0]
            and_node = temp
        else:
            flag = 0
            # store the where node [table_name, where_node]
            table_dic = {}
            join_node = []
            good_join_node_dic = {} # {table : join_node join_key = partition_key}
            good_join_node = []   # good join node
            l = len(and_node.children)
            k = 0
            spe_where = None            
            for i in range(l):
                i -= k
                if i > l - k:
                    break
                child = and_node.children[i]
                if child.name == Operator.WHERE:
                    if child.children[0].is_sub_plan:
                        spe_where = child
                        continue
                    #table = child.keys[0].split('.')[0]
                    table = child.tables[0]
                    if table not in table_dic:
                        table_dic[table] = child
                    else:
                        node = table_dic[table]
                        node.expression += (and_node.name + ' ' + child.expression)
                        and_node.children.remove(child)
                        k += 1
                elif child.name == Operator.JOIN:
                    join_node.append(child)
                    is_good = True
                    for i in range(len(child.tables)):
                        t = child.tables[i]
                        if t in self.task.table_alias:
                            t = self.task.table_alias[t]
                        p_key = InfoCenter().get_table_partition_key(metadata_db, t)
                        if child.keys[i].split(".")[1] not in p_key:
                            is_good = False
                            break
                    if is_good:
                        good_join_node.append(child)
                        #child.is_sql = 1
                        for t in child.tables:
                            good_join_node_dic[t] = child
                    flag = 1

            # add where prediction to each join node
            # if good_join_node != []:
            #     for join in good_join_node:
            #         for scan in join.children:
            #             scan_table = scan.keys[0].split(".")[0]
            #             if table_dic.has_key(scan_table) == True:
            #                 where_node = table_dic.pop(scan_table)
            #                 where_node.split_key = scan.split_key
            #                 for o in scan.output:
            #                     if o not in where_node.output:
            #                         where_node.output.append(o)
            #                 for i in scan.input:
            #                     if i not in where_node.input:
            #                         where_node.input.append(i)
            #                 join.children[join.children.index(scan)] = where_node
            #                 and_node.children.remove(where_node)

            # # connect related good join nodes
            # tmp_dic = {}
            # if good_join_node != [] and len(good_join_node_dic)<2*len(good_join_node):
            #     for join in good_join_node:
            #         for t in join.tables:
            #             if t not in tmp_dic:
            #                 tmp_dic[t] = join
            #             else:
            #                 pre_join = tmp_dic[t]
            #                 join.
                        
            #             for t in table_join_node:
            #                 if table_join_node[t] == join_pres:
            #                     table_join_node[t] = join
            #             join.children[join.children.index(s_scan)] = join_pres
            #             join_pres.split_key = s_scan.split_key
            #             for t in join_pres.tables:
            #                 if t not in join.tables:
            #                     join.tables.append(t)
            #             presjoinout = join_pres.output
            #             otherout = []
            #             for o in join.output:
            #                 t = o.split(".")[0]
            #                 if t == scan_table:
            #                     if o not in presjoinout:
            #                         presjoinout.append(o)
            #                 else:
            #                     otherout.append(o)
            #             join.output = presjoinout + otherout
            #             join.input = []
            #             self.set_output_attrs(join)                        
            #             tmp_join = join
                
            if spe_where is not None and spe_where.tables == []:
                # type = EXISTS or "NOT EXISTS", to deal with this kind of WHERE NODE,
                # we consider the node as another pre-plan which should be first
                # executed and if the reuslt of the node is satisfied.
                self.task.pre_plan = spe_where
                
            if spe_where is not None and spe_where.tables != []:

                table = spe_where.tables[0]
                if table_dic.has_key(table) == False:
                    table_dic[table] = spe_where
                    # add a new SCAN node to get data from database
                    new_node = BasicOp()
                    new_node.name = Operator.SCAN
                    new_node.expression = table
                    for inp in spe_where.input:
                        if inp not in spe_where.children[0].output:
                            new_node.input.append(inp)
                    new_node.output = new_node.input
                    new_node.tables = [table]
                    spe_where.children.append(new_node)
                else:
                    node = table_dic[table]
                    node.output += spe_where.keys
                    spe_where.children.append(node)
                    and_node.children.remove(node)
                    table_dic[table] = spe_where
                    
            if flag == 1:
                # first decide the order of JOIN
                # A JOIN B: if the partition keys of A and B equal to the joined keys,
                # it has high priority.
                new_join_node = []
                num_of_best_joins = 0
                for jn in join_node:
                    score = 0
                    for i in range(len(jn.tables)):
                        t = jn.tables[i]
                        if t in self.task.table_alias:
                            t = self.task.table_alias[t]
                        p_key = InfoCenter().get_table_partition_key(metadata_db, t)
                        if jn.keys[i].split(".")[1] in p_key:
                            score += 1
                    if score == 2:
                        new_join_node.insert(0, jn)
                    else:
                        new_join_node.append(jn)

                # # store join node [table_name, join_node]
                # table_join_node = {}
                # for t in good_join_node_dic:
                #     table_join_node[t] = good_join_node_dic[t]

                table_join_node = {}
                tmp_join = None
                for join in new_join_node:
                    pos = -1 # use to identify the special case of s = 2
                    pres_joins = []
                    pres_scans = []
                    #if join in good_join_node:
                    #    continue
                    s = 0
                    s_scan = None
                    for scan in join.children:
                        if scan.name != Operator.SCAN:
                            continue
                        scan_table = scan.keys[0].split('.')[0]
                        if table_join_node.has_key(scan_table) == False :
                            table_join_node[scan_table] = join
                        else:
                            if s == 1 and join_pres != table_join_node[scan_table]:
                                pos = 1
                            join_pres = table_join_node[scan_table]
                            pres_joins.append(join_pres)
                            pres_scans.append(scan)
                            
                            # else:
                            #     join_pres = table_join_node[scan_table]
                            #     if join_pres in good_join_node:
                            #         if pos != -2:
                            #             tmppos = good_join_node.index(join_pres)
                            #             ParaLiteLog.debug("tmppos: %s" % tmppos)
                            #             if tmppos != pos:
                            #                 pos = tmppos
                            #                 pres_joins.append(join_pres)
                            #                 pres_scans.append(scan)
                            #             else:
                            #                 pos = -2
                            #     else:
                            #         pos = -2
                            s_scan = scan
                            s += 1
                            continue
                        if table_dic.has_key(scan_table) == True:
                            where_node = table_dic[scan_table]
                            where_node.split_key = scan.split_key
                            for o in scan.output:
                                if o not in where_node.output:
                                    where_node.output.append(o)
                            for i in scan.input:
                                if i not in where_node.input:
                                    where_node.input.append(i)
                            join.children[join.children.index(scan)] = where_node
                            and_node.children.remove(where_node)
                
                    """
                    (1) s = 1: only one table (A) has already joined, then replace the
                               scan node of A by the previous join node
                    (2) s = 2: two tables have already joined, then we just add the
                               predication of current join into the last join node
                    """
                    if s == 0:
                        tmp_join = join
                        continue
                    elif s == 1:
                        # map all tables whose join node = join_pres to the new join
                        for t in table_join_node:
                            if table_join_node[t] == join_pres:
                                table_join_node[t] = join
                        join.children[join.children.index(s_scan)] = join_pres
                        join_pres.split_key = s_scan.split_key
                        for t in join_pres.tables:
                            if t not in join.tables:
                                join.tables.append(t)
                        presjoinout = join_pres.output
                        otherout = []
                        for o in join.output:
                            t = o.split(".")[0]
                            if t == scan_table:
                                if o not in presjoinout:
                                    presjoinout.append(o)
                            else:
                                otherout.append(o)
                        join.output = presjoinout + otherout
                        join.input = []
                        self.set_output_attrs(join)                        
                        tmp_join = join

                    elif s == 2:
                        # a special case: the joined two tables come from two
                        # different join nodes who are good join nodes, then we
                        # should use another join node to connect them
                        if pos != -1:
                            for t in table_join_node:
                                if table_join_node[t] in pres_joins:
                                    table_join_node[t] = join
                            for scanpos in range(len(pres_scans)):
                                tscan = pres_scans[scanpos]
                                tjoin = pres_joins[scanpos]
                                join.children[scanpos] = tjoin
                                tjoin.split_key = tscan.split_key
                                for t in tjoin.tables:
                                    if t not in join.tables:
                                        join.tables.append(t)

                            scan1 = pres_scans[0]
                            scan2 = pres_scans[1]
                            for o in join.output:
                                t = o.split(".")[0]
                                if t == scan1.tables[0]:
                                    if o not in scan1.output:
                                        scan1.append(o)
                                elif t == scan2.tables[0]:
                                    if o not in scan2.output:
                                        scan2.append(o)
                            join.output = scan1.output + scan2.output
                            join.input = []
                            self.set_output_attrs(join)                        
                            tmp_join = join
                            continue
                        
                        join_pres.prediction += join.prediction
                        for key in join.keys:
                            if key not in join_pres.keys: join_pres.keys.append(key)
                        for t in join.tables:
                            if t not in join_pres.tables: join_pres.tables.append(t)
                            
                        # set the output and input for the new predications
                        if join_pres.pred_input == []:
                            join_pres.pred_input.append(join_pres.input)
                        if join_pres.pred_output == []:
                            join_pres.pred_output.append(join_pres.output)
                        join_pres.pred_input.append(join.input)
                        join_pres.pred_output.append(join.output)
                        self.set_output_attrs(join_pres)                        
                        tmp_join = join_pres
                and_node = tmp_join
            else:
                one_node = BasicOp()
                one_node.name = Operator.WHERE
                for k in table_dic:
                    one_node.keys.append(table_dic[k].keys[0])
                    if one_node.expression == None:
                        one_node.expression = table_dic[k].expression
                    else:
                        one_node.expression += table_dic[k].expression 
                    for o in table_dic[k].output:
                        if o not in one_node.output:
                            one_node.output.append(o)
                    for i in table_dic[k].input:
                        if i not in one_node.input:
                            one_node.input.append(i)
                            
                    one_node.children.append(table_dic[k].children[0])
                and_node = one_node
        return and_node
    
    """
    OR rule:
    (1) all children of "or" are "where" operators, integrate them into one
    (2) some children of "or" are "join" operators, if is_split = True, do nothing
        if is_split = False, integrate them into one
        TODO: if there are more than two JOIN with is_split=True, do nothing
    (3) if or_node has OR child, delete OR child (all children of OR become the 
        children of or_node)
    """
    def or_rule(self, plan):
        queue = deque([])
        queue.append(plan)
        while list(queue) != []:
            operator = queue.popleft()
            for child in operator.children:
                if child.name == Operator.OR and child.is_visited != 1:
                    nodes = self.or_rule_op(child)
                    operator.children.remove(child)
                    operator.children.append(nodes)
                    nodes.is_visited = 1
                else:
                    queue.append(child)
    
    def or_rule_op(self, or_node):
        # where_node_dic (wnd) stores the where nodes
        # join_node_dic (jnd) stores the join nodes
        wnd = {}
        jnd = {}
        join_node = []
        flag = 0
        k = 0
        l = len(or_node.children)
        for i in range(l):
            i -= k
            if i > l - k:
                break
            child = or_node.children[i]
            if child.name == Operator.WHERE:
                table = child.keys[0].split('.')[0]
                if wnd.has_key(table) == False:
                    wnd[table] = child
                else:
                    node = wnd[table]
                    node.expression += (or_node.name + ' ' + child.expression) 
                    or_node.children.remove(child)
                    k += 1
            elif child.name == Operator.JOIN:
                join_node.append(child)
                flag = 1
        
        if flag == 1:
            for join in join_node:
                for scan in join.children:
                    if scan.name != Operator.SCAN:
                        continue
                    scan_table = scan.keys[0].split('.')[0]
                    if jnd.has_key(scan_table) == False :
                        jnd[scan_table] = join
                    if wnd.has_key(scan_table) == True:
                        where_node = wnd.pop(scan_table)
                        join.expression += or_node.name + ' ' + where_node.expression
                        scan.keys.append(where_node.keys[0])
                        or_node.children.remove(where_node)
        else:
            one_node = BasicOp()
            one_node.name = Operator.WHERE
            for k in wnd:
                one_node.keys.append(wnd[k].keys[0])
                if one_node.expression == None:
                    one_node.expression = wnd[k].expression
                else:
                    one_node.expression += wnd[k].expression 
                one_node.children.append(wnd[k].children[0])
            or_node = one_node
        return or_node

    def set_others(self, plan):
        # delete PROJ node
        if len(plan.children) > 0 and plan.name == Operator.PROJ:
            output = plan.output
            plan = plan.children[0]
            plan.output = output
        # set is_child_sql and group_by's input (sum(a.b)-->a.b)
        # set another input format of join to 'map' attr
        # set id for each operator
        # set the dest of children of udx node be DATA_TO_CLIENTS
        # set the sub-query in nested query: select stmt only contains the attr in 
        # # the nested query without the necessary attr in the outside query
        queue = deque([])
        queue.append(plan)
        count = 0
        while list(queue) != []:
            op = queue.popleft()
            if op.is_sub_plan:
                continue

            if op.name == Operator.UDX:
                for child in op.children:
                    child.dest = conf.DATA_TO_CLIENTS
            elif op.name == Operator.GROUP_BY or op.name == Operator.AGGREGATE:
                op_child = op.children[0]
                if op_child.name == Operator.SQL and not op_child.is_sub_plan:

                    op.is_child_sql = True
                    ## if there is avg() function, avg() -- > sum() and count()
                    has_count = False
                    
                    # split the function into single functions: 
                    # avg(a) + 0.2*sum(b)--> avg(a), sum(b)
                    newfunc = []
                    for fun in op.function:
                        singlefunc = []
                        has_avg = []
                        if fun.find("avg(") != -1:
                            has_avg.append(fun)
                        if fun.find("count(*)") != -1:
                            has_count = True
                        expr = newparser.parse_column_expr(fun)
                        for ele in expr:
                            if re.match("(.*)\((.*)\)", ele):
                                # this is a single func
                                singlefunc.append(ele)
                        newfunc.append(singlefunc)
                        
                    for i in range(len(newfunc)):
                        singlefunc = newfunc[i]
                        if len(singlefunc) == 1 and singlefunc[0] == op.function[i] and singlefunc[0].find("avg(") == -1:
                            continue
                        pos = op.input.index(op.function[i])
                        op.input.remove(op.input[pos])
                        count = 0
                        for func in singlefunc:
                            if func.lower().startswith("avg("):
                                op.input.insert(pos+count, func.replace("avg", "sum"))
                                if func.replace("avg", "sum") in singlefunc:
                                    singlefunc.remove(func)
                                else:
                                    singlefunc[singlefunc.index(func)] = func.replace(
                                        "avg", "sum")
                            else:
                                op.input.insert(pos+count, func)
                            count += 1
                        if has_count:
                            op_child.expression = op_child.expression.replace(
                                op.function[i], ",".join(singlefunc))
                        else:
                            op.input.insert(pos+count, "count(*)")
                            op_child.expression = op_child.expression.replace(
                                op.function[i], ",".join(singlefunc + ["count(*)"]))
                            has_count = True
                        
                    # if has_avg != []:
                    #     for avg in has_avg:
                    #         pos = op.input.index(avg)
                    #         if has_count:
                    #             op.input[pos] = avg.replace("avg", "sum")
                    #             op_child.expression = op_child.expression.replace("avg", "sum")
                    #         else:
                    #             op.input[pos] = avg.replace("avg", "sum")
                    #             op.input.insert(pos+1, "count(*)")
                    #             op_child.expression = op_child.expression.replace(avg, "%s,count(*)" % (avg)).replace("avg", "sum")
                    #             has_count = True

                        if not op_child.is_sub_plan:
                            op_child.output = op.input
                else:
                    if op_child.is_sub_plan:
                        input_new = []
                        for outt in op_child.output:
                            if outt in self.task.column_alias:
                                input_new.append(self.task.column_alias[outt][0])
                            else:
                                input_new.append(outt)
                        op.input = input_new
                    else:
                        input_new = []
                        for i in op.related_col:
                            if i not in input_new:
                                input_new.append(i)
                        for i in op.input:
                            if i.find("(") == -1:
                                if i not in input_new:
                                    input_new.append(i)

                        op.input = input_new
                        op_child.output = op.input
                    self.set_output_attrs(op_child)
                    
            elif op.name == Operator.SQL:
                ex = op.expression
                if ex.find(" %s " % Operator.DISTINCT) != -1:
                    attr = ex[ex.find(
                        "%s" % Operator.DISTINCT) + len(Operator.DISTINCT) : ex.index("from")]
                else:
                    attr = ex[6 : ex.index("from")]
                attrs = attr.split()
                if len(attrs) != len(op.output):
                    newattr = ""
                    for out in op.output:
                        if newattr == "":
                            newattr += out
                        else:
                            newattr += "," + out
                    newattr = ' ' + newattr + ' '
                    op.expression = op.expression.replace(attr, newattr)
            elif op.name == Operator.ORDER_BY:
                # the key of order_by operator may be a alias of column, we have to
                # keep the consistence between key, input and output
                new_input = []
                for inp in op.input:
                    if inp in self.task.column_alias:
                        for alias in self.task.column_alias[inp]:
                            new_input.append(alias)
                    else:
                        new_input.append(inp)
                op.input = new_input

                new_output = []
                for out in op.output:
                    if out in self.task.column_alias:
                        for alias in self.task.column_alias[out]:
                            new_output.append(alias)
                    else:
                        new_output.append(out)
                op.output = new_output
                    
            for child in op.children:
                queue.append(child)

        # set the id for each operator
        # change the order of tables if the first table is a temp table
        count = 0
        queue = deque([])
        queue.append(plan)
        while list(queue) != []:
            op = queue.popleft()
            op.id = str(count)
            count += 1
            if len(op.tables) > 0 and op.tables[0] in self.task.tmp_table:
                t = op.tables[0]
                op.tables.remove(t)
                op.tables.append(t)
            for child in op.children:
                queue.append(child)

        # set the input for JOIN op
        queue = deque([])
        queue.append(plan)
        while list(queue) != []:
            op = queue.popleft()
            if op.name == Operator.JOIN:
                if len(op.children) == 2:
                    op.map = {}
                    op.map[op.children[0].id] = op.children[0].output
                    op.map[op.children[1].id] = op.children[1].output
            for child in op.children:
                queue.append(child)


        """
        TODO: (1), reorder the input of each operator according to the output of their children.
        (2), sometimes different join nodes will take the same sql child, we should delete the redundent data selection from the same table.
        """
        # set the limit attribute
        if plan.name == Operator.LIMIT:
            plan.children[0].limit = string.atoi(plan.expression)
            plan = plan.children[0]

        # set the distinct attribute
        if plan.name == Operator.DISTINCT:
            if plan.children[0].name in [Operator.ORDER_BY, Operator.AGGREGATE]:
                plan.children[0].distinct = True
                plan = plan.children[0]
            
        # set is_first_op
        plan.is_first_op = True
        return plan
    """
    The input for each operator is the output plus other involved keys.
    The output of each child is a part of the input of the father operator.
    """
    def set_output_attrs(self, plan):
        queue = deque([])
        queue.append(plan)
        while list(queue) != []:
            operator = queue.popleft()
            """
            For join operator, if some outputs are not belong to tables 
            this operator involved, delete them
            """
            if operator.is_sub_plan:
                continue
            if operator.name == Operator.JOIN:
                output = []
                for out in operator.output:
                    #sometimes, out is a function e.g. sum(a.b)...
                    m = re.search("(.*)\((.*)\)", out) 
                    if m:
                        out = m.group(2)
                    for t in operator.tables:
                        if t == out.split('.')[0] and out not in output:
                            output.append(out)
                operator.output = output
            """
            for udx, output_columns -->  arg in input
            F(a, b) outputs ["aa", "bb"], then the input should be "a" , "b"
            """
            if operator.name == Operator.UDX:
                for out in operator.output:
                    new_out = out
                    # check if this output is an alias or not
                    for col in self.task.column_alias:
                        alias = self.task.column_alias[col]
                        for al in alias:
                            if al == out:
                                new_out = col
                                break
                    
                    if new_out.find("(") != -1:
                        o = new_out[new_out.rfind("(") + 1:new_out.find(")")]
                        for oo in o.split(","):
                            if not (string.strip(oo) in operator.input):
                                operator.input.append(string.strip(oo))
                    else:
                        if not (new_out in operator.input):
                            operator.input.append(new_out)
            else:
                """
                use table.column way
                """
                for out in operator.output:
                    if out not in operator.input:
                        operator.input.append(out)

            """
            add required columns which are not outputed but needed by comparsion
            """
            flag = 0
            for key in operator.keys:
                flag = 0
                for out in operator.output:
                    if out == key:
                        flag = 1
                        break
                if flag == 0:
                    if key not in operator.input:
                        operator.input.append(key)
            """
            For some nodes, the output may not be the input of their parent if
            their father is JOIN node.
            """

            if operator.name == Operator.JOIN:
                for child in operator.children:
                    for inp in operator.input:
                        t = inp.split(".")[0]
                        if t in child.tables and inp not in child.output:
                            child.output.append(inp)
                    queue.append(child)
            else:
                for child in operator.children:
                    if child.is_sub_plan:
                        # reset the output of the parent node
                        new_output = []
                        for out in operator.output:
                            if out.find(".") != -1:
                                if out.split(".")[0] in operator.tables and out not in new_output:
                                       new_output.append(out)
                            else:
                                if out not in new_output: new_output.append(out)
                        operator.output = new_output
                        operator.input = []
                        for ii in child.output:
                            if ii in self.task.column_alias and len(self.task.column_alias[ii]) == 1:
                                ii = self.task.column_alias[ii][0]
                            if ii not in operator.input: operator.input.append(ii)
                        
                        for ii in new_output:
                            if ii not in operator.input: operator.input.append(ii)
                        for ii in operator.keys:
                            if ii not in operator.input: operator.input.append(ii)

                        continue
                    child.output = operator.input
                    queue.append(child)

            """
            # for each child operator, its output should be related to its table
            flag = False # if True: only related to one table
            for child in operator.children:
                for inp in operator.input:
                    if inp.find(".") != -1:
                        t = inp.split(".")[0]
                        if t in child.tables and inp not in child.output:
                            child.output.append(inp)
                    else:
                        flag = True
                        break
                queue.append(child)
            if flag:
                for child in operator.children:
                    child.output = operator.input
                    queue.append(child)
            """
           
    def split_rule(self, plan):
        """
        use hierarchical retiveal to visit each operator in
        logical_plan. If the operator is JOIN or GROUP_BY, check
        if the join key and group key are the same with the table
        partition key, if not, set the is_split attr of related
        operator be true.
        """
        if self.task.table is not None:
            partition_num = len(self.task.datanode)
            if partition_num == 1:
                # delete aggregation node
                if plan.name == Operator.AGGREGATE and len(plan.children) > 0:
                    plan = plan.children[0]
                return plan
        queue = deque([])
        queue.append(plan)
        while list(queue) != []:
            op = queue.popleft()
            if op.table_split_key == {}:
                self.set_split_key_op(op)
            for child in op.children:
                if child.is_sub_plan:
                    continue
                queue.append(child)
        return plan

    def set_split_key_op(self, op):
        if len(op.children) == 0:
            for k in op.keys:
                if k.find(".") != -1:
                    table_name = k.split('.')[0]
                else: 
                    table_name = self.task.table
                if table_name in self.task.table_alias:                    
                    tt = self.task.table_alias[table_name]
                else: tt = table_name
                table_split_key = InfoCenter().get_table_partition_key(metadata_db,tt)
                op.table_split_key[table_name] = table_split_key
        else:
            if op.is_sub_plan:
                return

            for child in op.children:
                self.set_split_key_op(child)
                for key in child.table_split_key.keys():
                    op.table_split_key[key] = child.table_split_key[key]
            if op.name == Operator.JOIN:
                """
                For join operator without prediction
                """
                if op.prediction == []:
                    op.is_sql = 0
                    return 
                """
                check if two involved tables are partitioned on same nodes,
                if so, check the partition key;
                if not, op.is_sql = 0 in any case
                """
                p1 = InfoCenter().get_data_node_info(metadata_db, op.tables[0])
                p2 = InfoCenter().get_data_node_info(metadata_db, op.tables[1])
                if p1 == p2:
                    if len(p1) == 1:
                        op.is_sql = 1
                    else:
                        for k in op.keys:
                            table_name = k.split('.')[0]
                            attr_name = k.split('.')[1]
                            if (table_name not in op.table_split_key) or op.table_split_key[table_name] == [] or attr_name != op.table_split_key[table_name][0]:
                                for child in op.children:
                                    child.is_split = True
                                    op.is_merge = True
                                    op.is_sql = 0
                                    
                                    # if child.keys[0].split('.')[0] == table_name:
                                    #     child.is_split = True
                                    #     #child.split_key = child.keys[0]
                                    #     op.is_merge = True
                                    #     op.is_sql = 0
                                op.table_split_key[table_name] = attr_name

                else:
                    for child in op.children:
                        if child.keys[0].split('.')[0] == table_name:
                            child.is_split = True
                            #child.split_key = child.keys[0]
                            op.is_merge = True
                            op.is_sql = 0
                    op.table_split_key[table_name] = attr_name
            elif op.name == Operator.GROUP_BY:
                """
                GROUP_BY: 
                (1) group by x.a : check the partition key of table x 
                (2) group by x.a, x.b : check the partition key of table x
                (3) group by x.a, y.c : child.is_split = True
                """
                related_table = []
                for key in op.group_key:
                    if key.find(".") != -1:
                        table = key.split(".")[0]
                    else:
                        table = self.task.table
                    if table not in related_table:
                        related_table.append(table)
                if len(related_table) > 1 or related_table[0] not in op.table_split_key or op.group_key != op.table_split_key[related_table[0]]:
                    child = op.children[0]
                    child.is_split = True
                    if child.is_sub_plan:
                        # use the orginal column as group_key not the alias
                        for gkey in op.group_key:
                            if gkey in child.output:
                                child.split_key.append(gkey)
                            else:
                                for ori in self.task.column_alias:
                                    if self.task.column_alias[ori][0] == gkey:
                                        child.split_key.append(ori)
                                        break
                    else:
                        child.split_key = op.group_key
                    op.is_merge = True
                    op.is_sql = 0

    def make_SQL(self, plan):
        self.mark_SQL(plan)
        """
        if the plan contains a AGGR op and its is_sql = 1, e.g.
        proj
          - limit is_sql=1
            -- aggr is_sql=1
              --- where is_sql=1
        -->
        proj
          - limit is_sql=0
            --aggr is_sql=0
              --- limit is_sql=1
                ---- aggr is_sql=1
                  ----- where is_sql=1
        """
        queue = deque([])
        queue.append(plan)
        first_op_is_sql = BasicOp()
        first_op_pos = 100
        count = 0
        flag = 0
        while list(queue) != []:
            op = queue.popleft()
            if op.name == Operator.AGGREGATE:
                if op.is_sql == 1:
                    flag = 1
                    break
            if op.is_sql == 1:
                if count < first_op_pos:
                    first_op_is_sql = op
                    first_op_pos = count
            count += 1
            for child in op.children:
                queue.append(child)

        new_first = BasicOp()
        first_op_is_sql.copy(new_first)
        if flag == 1:
            op.children = [new_first]
            op.is_sql = 0
            queue = deque([])
            queue.append(plan)
            while list(queue) != []:
                op = queue.popleft()
                if op.name == Operator.AGGREGATE:
                    break
                op.is_sql = 0
                for child in op.children:
                    queue.append(child)
            
        ParaLiteLog.info("after mark sql")
        ParaLiteLog.info(plan.get())
        sql_node = BasicOp()
        if plan.is_sql == 1:
            sql_node = self.make_SQL_op(plan)
            plan = sql_node
        else:
            queue = []
            queue.append(plan)
            while queue!= []:
                operator = queue.pop()
                for child in operator.children:
                    if child.is_sql == 1 and child.name != Operator.SQL:
                        if operator.name == Operator.GROUP_BY:
                            sql_node = self.make_SQL_op(operator)
                        else:
                            sql_node = self.make_SQL_op(child)
                        pos = operator.children.index(child)
                        operator.children[pos] = sql_node
                    else:
                        queue.append(child)
        return plan

    def make_SQL_op(self, operator):
        """
        TODO: if operator is PROJ and it has a child of AGGREGATE, the logical plan
              should be a SQL puls another AGGREGATE
        """
        sql = BasicOp()
        sql.name = Operator.SQL
        if operator.name == Operator.GROUP_BY:
            sql.is_split = operator.children[0].is_split
            sql.split_key = operator.children[0].split_key
            sql.is_merge = operator.children[0].is_merge
        else:
            sql.is_split = operator.is_split
            sql.is_merge = operator.is_merge
            sql.split_key = operator.split_key
        sql.tables = operator.tables
        if operator.name == Operator.GROUP_BY:
            sql.output = operator.input
        elif operator.name == Operator.PROJ:
            sql.output = operator.output
        else:
            flag = 0
            for out in operator.output:
                flag = 0
                if out.find("(") != -1:
                    # this is a function column
                    args = newparser.parse_column_expr(out)
                    for subarg in args:
                        if re.search("^[a-zA-Z][a-zA-Z0-9_.]*$", subarg) is not None:
                            if subarg.find(".") == -1:
                                t = self.task.table
                            else:
                                t = subarg.split(".")[0]
                            if t in sql.tables:
                                if subarg not in sql.output:
                                    sql.output.append(out)
                else:
                     for table in sql.tables:
                         if out.find(".") == -1 or out.split('.')[0] == table:
                             flag = 1
                             break
                     if flag == 1:
                         if out not in sql.output:
                             sql.output.append(out)
        """
        a special case:
        If a child of operator is sub_plan, then create a new sql which contains the sub
        query:
        -where1: C.a in _col
          -- where2: C.b > 1 and C.d = 0..
          -- sql3: select c from C where ...    output=[_col]
      -->
        -sql: select output_of_where1 from C where prediction_of_where2 and prediction_of_where1.replace(_col, sql3)
        """
        """
        is_special = False
        for child in operator.children:
            if child.is_sub_plan:
                is_special = True
                break
        if is_special:
            # remove SCAN node
            for child in operator.children:
                if child.name == Operator.SCAN:
                    operator.children.remove(child)
                    break

            if len(operator.children) > 1:
                if operator.children[0].name == Operator.WHERE:
                    a = operator.children[0]
                    b = operator.children[1]
                else:
                    a = operator.children[1]
                    b = operator.children[0]
                sql.expression = "select %s from %s where %s and %s" % (",".join(operator.output), operator.tables[0], a.expression, operator.expression.replace(b.output[0], "(%s)" % b.expression))
            else:
                b = operator.children[0]
                sql.expression = "select %s from %s where %s" % (",".join(operator.output), operator.tables[0], operator.expression.replace(" %s " % b.output[0], "(%s)" % b.expression))
            return sql
        """ 
        expression = ''
        dic = {}
        queue = []
        queue.append(operator)
        while queue != []:
            op = queue.pop()
            if dic.has_key(op.name) == 0:
                if op.name == Operator.SCAN:
                    exp = ""
                    t = op.expression.strip()
                    if t in self.task.table_alias:
                        exp += (self.task.table_alias[t] + " " + t)
                    else:
                        exp += op.expression
                    dic[op.name] = exp
                else:
                    dic[op.name] = op.expression
            else:
                exp = dic[op.name]
                if op.name == Operator.SCAN:
                    t = op.expression.strip()
                    if t in self.task.table_alias:
                        exp += (',' + self.task.table_alias[t] + " " + t)
                    else:
                        exp += (',' + op.expression)
                else:
                    exp += (Operator.AND + ' ' + op.expression + ' ')
                dic[op.name] = exp
            for child in op.children:
                queue.append(child)

        while dic != {}:
            for out in sql.output:
                if expression == '':
                    if Operator.DISTINCT in dic:
                        expression = ('select %s %s' % (Operator.DISTINCT, out))
                        dic.pop(Operator.DISTINCT)
                    else:
                        expression = ('select %s' % (out))
                else:
                    expression += (',' + out)
            if dic.has_key(Operator.PROJ) == 1:
                dic.pop(Operator.PROJ)
            expression += (' ')
            if dic.has_key(Operator.SCAN) == 1:
                temp = Operator.SCAN
                expression += (temp + ' ' + dic.pop(temp) + ' ')
            if dic.has_key(Operator.WHERE) == 1:
                temp = Operator.WHERE
                expression += (temp + ' ' + dic.pop(temp) + ' ')
                if dic.has_key(Operator.JOIN) == 1:
                    temp = Operator.JOIN
                    expression += (Operator.AND + ' ' + dic.pop(temp) + ' ')
            else:
                if dic.has_key(Operator.JOIN) == 1:
                    temp = Operator.JOIN
                    expression += (Operator.WHERE + ' ' + dic.pop(temp) + ' ')
                        
            if dic.has_key(Operator.AGGREGATE) == 1:
                temp = Operator.AGGREGATE
                #expression += (' ' + dic.pop(temp) + ' ')
                dic.pop(temp)
            if dic.has_key(Operator.GROUP_BY) == 1:
                temp = Operator.GROUP_BY
                name = temp.replace('_', ' ')
                expression += (name + ' ' + dic.pop(temp) + ' ')
            if dic.has_key(Operator.ORDER_BY) == 1:
                temp = Operator.ORDER_BY
                name = temp.replace('_', ' ')
                expression += (name + ' ' + dic.pop(temp) + ' ')
            if dic.has_key(Operator.LIMIT) == 1:
                temp = Operator.LIMIT
                expression += (temp + ' ' + dic.pop(temp) + ' ')
        sql.expression = expression
        return sql
            
    def mark_SQL(self, plan):
        queue = []
        queue.append(plan)
        while queue!=[]:
            operator = queue.pop()
            if operator.is_sql == None:
                self.mark_SQL_op(operator)
            for child in operator.children:
                queue.append(child)
        
    def mark_SQL_op(self, operator):
        flag = 0
        for child in operator.children:
            if child.is_sql == None:
                self.mark_SQL_op(child)
            if child.name == Operator.DISTINCT:
                child.is_sql = 0
            if child.is_sql == 0:
                operator.is_sql = 0
                flag = 1
                break
            if child.is_sub_plan:
                operator.is_sql = 0
                flag = 1
                break
        if flag == 0:
            operator.is_sql = 1

class DataParalleler:

    FINISH = 'finish'
    FAIL = 'fail'
    DONE = 'done'
    RUNNING = 'running'
    READY = 'ready'

    def __init__(self, task):
        self.worker_nodes = task.worker_nodes
        self.data_nodes = task.data_nodes 
        self.t_size = 0
        self.f_size = 0 # the size of data which has been finished
        self.status = self.READY
        self.std_cost_time = 0
        self.bk = 0
        self.start_time = 0
        self.end_time = 0
        self.t_total = 0
        self.is_notify = False
        self.hostname = gethostname()
        self.used_worker = {}
        self.task = task
        self.partition_num = 0
        self.max_node = None

    def set_bk_size(self):
        for key in self.data_nodes.keys():
            data_node = self.data_nodes[key]
            num_of_worker_node = len(self.worker_nodes)
            nn = num_of_worker_node * 4
            if data_node.t_size % nn:
                data_node.b_size = data_node.t_size/nn + 1
            else:
                data_node.b_size = data_node.t_size/nn
            bk = data_node.b_size
        self.bk = bk

    def update_ECT(self):
        max_ECT = 0
        max_node = None
        for nid in self.data_nodes:
            dn = self.data_nodes[nid]
            if dn.l_size == 0:
                dn.ECT = 0
                continue
            if dn.ECT == sys.maxint:
                max_node = dn
                max_ECT = sys.maxint
                continue
            bsize = dn.b_size
            m = 0  # the max of left time 
            t = time.time()
            total_speed = 0
            for client in dn.clients:
                if client.status == conf.IDLE:
                    l_time = 0
                else:
                    l_time = bsize/client.speed - (t - client.s_time)
                if l_time> m:
                    m = l_time
                total_speed += client.speed
            dn.ECT = dn.l_size/total_speed + m
            if dn.ECT > max_ECT:
                max_ECT = dn.ECT
                max_node = dn
        self.max_node = max_node

    def get_avaliable_worker_node(self, data_node):
        ava_nodes = []
        for key in self.worker_nodes.keys():
            w_node = self.worker_nodes[key]
            if w_node.status == conf.READY or w_node.status == conf.IDLE:
                if w_node.name == data_node.name:
                    return w_node
                else:
                    ava_nodes.append(w_node)
        if len(ava_nodes) > 0:
            return ava_nodes[random.randint(0, len(ava_nodes) - 1)]
        return None
    
    def get_best_data_node(self, worker_node):
        for key in self.data_nodes.keys():
            data_node = self.data_nodes[key]
            if data_node.name == worker_node.name and data_node.l_size > 0:
                return data_node
        self.update_ECT()
        return self.max_node

    def all_data_node_done(self):
        queue = deque([])
        queue.append(self.task.plan)
        if self.partition_num == 0:
            while list(queue) != []:
                op = queue.popleft()
                if op.name == Operator.UDX:
                    self.partition_num = len(op.children[0].node)
                    break
                for child in op.children:
                    queue.append(child)
        if len(self.data_nodes) == self.partition_num:
            for key in self.data_nodes.keys():
                data_node = self.data_nodes[key]
                if data_node.l_size > 0:
                    return False
            return True
        else:
            return False
    
    def send_cmd(self, data_node, worker_node):
        if data_node.l_size == 0:
            return None, None

        # calculate the data size to be sent
        # s_size = int(data_node.b_size + data_node.b_size * (worker_node.score - 1))

        cur_job_l_size = data_node.job_l_size[data_node.cur_job]
        s_size = min(cur_job_l_size, data_node.b_size)
            
        # msg = worker_id:job_id:(worker_node:worker_port):block_size
        sep = conf.SEP_IN_MSG
        if data_node.name == worker_node.name:
            addr = worker_node.local_addr
        else:
            addr = "%s%s%s" % (worker_node.name, sep, worker_node.port)
        msg = sep.join(
            [conf.DATA_DISTRIBUTE_UDX, worker_node.id,
             data_node.cur_job, addr, str(s_size)])

        # send msg to the worker
        sock = socket(AF_INET, SOCK_STREAM)
        addr = (data_node.name, data_node.port)
        try:
            sock.connect(addr)
            sock.send('%10s%s' % (len(msg), msg))
            sock.close()
        except Exception, e:
            if e.errno == 4:
                ParaLiteLog.info("ERROR: %s : %s --> %s " % (e.errno, data_node.name,
                                                             worker_node.id))
                return None, None

        ParaLiteLog.info('CMD: %s --> %s id=%s' % (
            data_node.name, msg, worker_node.id))

        # set the left size for the target job
        target_job = data_node.cur_job
        block_id = data_node.cur_bk
        data_node.job_l_size[data_node.cur_job] -= s_size
        data_node.cur_bk += 1
        if cur_job_l_size == s_size:
            cur_job_pos = data_node.job_t_size.keys().index(data_node.cur_job)
            if cur_job_pos + 1 < len(data_node.job_t_size):
                data_node.cur_job = data_node.job_t_size.keys()[cur_job_pos + 1]
                data_node.cur_bk = 0

        worker_node.s_time = time.time()*1000
        data_node.l_size = max(data_node.l_size - s_size, 0)
        worker_node.status = conf.RUNNING
        # if data_node.l_size == 0:
        #     sock = socket(AF_INET, SOCK_STREAM)
        #     sock.connect((data_node.name, data_node.port))
        #     sock.send('%10s%s' % (len(conf.END_TAG), conf.END_TAG))
        #     sock.close()
        return target_job, block_id

    def distribute_data(self, node):
        result = {} # {workerid : (datanode, jobid, blockid)}
        try:
            if isinstance(node, WorkerNode):
                dn = self.get_best_data_node(node)
                if dn:
                    jobid, bkid = self.send_cmd(dn, node)
                    if jobid is not None:
                        result[node.id] = (dn.id, dn.name, jobid, bkid)
            elif isinstance(node, DataNode):
                while True:
                    wn = self.get_avaliable_worker_node(node)
                    if wn is None or node.l_size <= 0:
                        break
                    jobid, bkid = self.send_cmd(node, wn)
                    if jobid is not None:
                        result[wn.id] = (node.id, node.name, jobid, bkid)
            return result
        except Exception, e:
            ParaLiteLog.info(traceback.format_exc())
                
class TaskManager:
    def __init__(self, iom, userconfig):
        self.taskqueue = {}        # store all task, once a task is finished, delete it
        self.queue = Queue.Queue() # store all task, once there is one, take it out
                                   # and process it
        self.nid_cid_map = {}  # node id --> collective query id
        self.process = {}
        self.iom = iom
        self.userconfig = userconfig
        self.operator_jobqueue = {} # operator : jobs
        self.operator_addrs = {}    # operator : <node:port:localaddr>
        self.operator_idle = {}     # operator : <node:port:localaddr>

    def put_task_to_queue(self, task, cnode, so):
        tid = task.id
        if self.taskqueue.has_key(tid) == 1:
            self.taskqueue[tid].clients.append(cnode)
            self.taskqueue[tid].reply_sock.append(so)
        else:
            task.clients.append(cnode)
            task.s_time = time.time()
            task.reply_sock.append(so)
            self.taskqueue[tid] = task
            self.queue.put(task)

    def get_task_from_queue(self, tid):
        if tid is None:
            if len(self.taskqueue) != 0:
                return self.taskqueue[self.taskqueue.keys[0]]
        if self.taskqueue.has_key(tid) != 1:
            return None
        else:
            return self.taskqueue[tid]
        
    def remove_task_from_queue(self, tid):
        if self.taskqueue.has_key(tid) == 1:
            self.taskqueue.pop(tid)
        else:
            es('Task %s is not in task queue' % (tid))

    def dispatch_job(self, opid, opname, datanode):
        jobqueue = self.operator_jobqueue[opid]
        target_job = None
        if opname != Operator.SQL:
            for job in jobqueue:
                if job.status == Job.READY:
                    target_job = job
        if opname != Operator.SQL:
            return target_job

        for job in jobqueue:
            # check if there is job locally
            if job.status == Job.READY and datanode == job.node:
                if job.sql.lower().startswith("create") or job.sql.lower().startswith("drop"):
                    # This is a CREATE job
                    target_job = job
                else:
                    job.target_db = [job.db[datanode]]
                    job.target_node = datanode
                    target_job = job
                    break
                    
        if target_job is None:
            # check if there is replica job locally
            for job in jobqueue:
                if job.status == Job.READY and datanode in job.db:
                    job.target_db = [job.db[datanode]]
                    job.target_node = datanode
                    target_job = job
                    break
                    
        # if target_job is None:
        #     # check if there is unfinished job running on other nodes -- Skew problem
        #     for job in jobqueue:
        #         if job.status == Job.RUNNING and datanode in job.db:
        #             target_job = job
        return target_job
    
    def new_schedule_task(self, task):
        """schedule operators from the top to the bottom

        Only operators whose parents are already started (after received all
        registration messages) should be started. So, in the first scheduling,
        only the root operator is scheduled.
        """
        root = task.plan
        try:
            # create job queue for the operator
            self.new_schedule_operator(root)
            return -1, None
        except Exception, e:
            return 1, traceback.format_exc()

    def new_schedule_operator(self, op):
        """Schedule different operators 
        """
        if op.name in [Operator.AGGREGATE, Operator.ORDER_BY, Operator.DISTINCT]:
            self.schedule_blocking_op(op)
        elif op.name == Operator().UDX:
            self.schedule_udx_op(op)
        else:
            self.schedule_general_op(op)
        
    def schedule_blocking_op(self, op):
        """create a job for aggrgation, order or distinct operator
        @param BasicOp op      the current operator that requires to be scheduled
        """
        assert op is not None
        self.operator_addrs[op.id] = []
        self.operator_idle[op.id] = []        
        self.operator_jobqueue[op.id] = []
        
        t = op.tables[0]
        task = self.taskqueue[self.taskqueue.keys()[0]]
        if t in task.table_alias:
            t = task.table_alias[t]
        if task.datanode == []:
            task.datanode = InfoCenter().get_data_node_info(metadata_db, t)
        parnode = task.datanode

        node = parnode[random.randint(0, len(parnode)-1)]
        job = Job()
        job.opid = op.id
        job.port = self.get_available_port(node)
        job.node = node
        job.id = "0"
        self.operator_jobqueue[op.id].append(job)
        self.start_operator(op, [node])
        
    def schedule_udx_op(self, op):
        """create jobs for udx operator based on the number of clients
        @param BasicOp op      the current operator that requires to be scheduled
        @param Task task       the current task
        """
        task = self.taskqueue[self.taskqueue.keys()[0]]
        op.worker_num = len(task.clients)
        
        self.operator_addrs[op.id] = []
        self.operator_idle[op.id] = []
        
        # create job
        count = 0
        self.operator_jobqueue[op.id] = []
        for w in task.clients:
            job = Job()
            job.opid = op.id
            job.port = self.get_available_port(w)
            job.node = w
            job.id = str(count)
            count += 1
            self.operator_jobqueue[op.id].append(job)
        self.start_operator(op, task.clients)
        
    def schedule_general_op(self, op):
        """create jobs for general operator based on the number of chunks
        @param BasicOp op      the current operator that requires to be scheduled
        """
        task = self.taskqueue[self.taskqueue.keys()[0]]
        t = op.tables[0]
        if t in task.table_alias:
            t = task.table_alias[t]
        if task.datanode == []:
            task.datanode = InfoCenter().get_data_node_info(metadata_db, t)
        parnode = task.datanode

        # set the port on each node for this operator
        self.operator_addrs[op.id] = []
        self.operator_idle[op.id] = []
        
        # create jobs 
        self.operator_jobqueue[op.id] = []            
        if op.name == Operator.SQL:
            # different handler for CREATE and SELECT query
            if op.expression.lower().startswith("create") or op.expression.lower().startswith("drop"):
                # only N jobs, the target dbs for each job are all dbs on the node
                id_count = 0
                for node in parnode:
                    job = Job()
                    job.id = str(id_count)
                    job.name = op.name
                    job.node = node
                    job.sql = op.expression
                    job.opid = op.id
                    db_replica = InfoCenter().get_replica_by_node(metadata_db, node)
                    db_orig = InfoCenter().get_active_db(metadata_db, node)
                    job.target_db = db_replica + db_orig
                    job.target_node = node
                    self.operator_jobqueue[op.id].append(job)
                    id_count += 1
            else:        
                dbs = InfoCenter().get_table_db_name(metadata_db, t)
                id_count = 0
                for db in dbs:
                    job = Job()
                    job.id = str(id_count)
                    job.node = db.split("_")[-3]
                    job.opid = op.id
                    job.sql = op.expression                    
                    job.name = op.name
                    db_replica = InfoCenter().get_replica_by_db(metadata_db, db)
                    for db_r in db_replica:
                        job.db[db_r.split()[1]] = db_r.split()[0]
                    job.db[job.node] = db
                    self.operator_jobqueue[op.id].append(job)
                    id_count += 1
        else:
            # for other general op (e.g. JOIN and GROUP),we set the same jobs as chunks
            dbs = InfoCenter().get_table_db_name(metadata_db, t)
            id_count = 0
            #parallel_degree = len(dbs)
            if hasattr(conf, "task_num_%s" % op.id):
                parallel_degree = getattr(conf, "task_num_%s" % op.id)
            else:
                parallel_degree = getattr(conf, "task_num_for_%s" % op.name)
            for i in range(parallel_degree):
                job = Job()
                job.id = str(id_count)
                job.opid = op.id
                job.name = op.name                
                self.operator_jobqueue[op.id].append(job)
                id_count += 1
        self.start_operator(op, parnode)
        
    def start_operator(self, op, nodes):
        ParaLiteLog.debug("execute operator: START --> %s" % nodes)
        path = WORKING_DIR
        for n in nodes:
            if n.find(":") != -1:
                # n = nodename:port
                node = n.split(":")[0]
                port = string.atoi(n.split(":")[1])
            else:
                node = n
                port = self.get_available_port(node)
                
            # python %.py master_node master_port cqid opid port log_dir [worker_id]
            name = '%s%s.py' % (path, op.name)
            t = self.taskqueue[self.taskqueue.keys()[0]]
            cqid = t.id
            log_dir = self.userconfig.log_dir
            
            args = "%s %s %s %s %s %s" % (master_node, master_port,
                                          cqid, op.id, port, log_dir)
            if op.name == Operator.UDX:
                args = "%s %s" % (args, len(t.clients)-1)

            # check if gxpc is in the path
            
            program = "gxpc"
            flag = 0
            for path1 in os.environ["PATH"].split(os.pathsep):
                path1 = path1.strip('"')
                exe_file = os.path.join(path1, program)
                if os.path.isfile(exe_file) and os.access(exe_file, os.X_OK):
                    flag = 1
                    break
            if flag == 0:
                raise(Exception("ERROR: Please set gxpc in the path"))

            cmd = "gxpc e -h %s python %s %s" % (node, name, args)
            ParaLiteLog.debug(cmd)
            
            pipes = [ (None, "w", 0), (None, "r", 1), (None, "r", 2) ]
            try:
                x = self.iom.create_process(cmd.split(), None, None, pipes, None)
            except:
                ParaLiteLog.debug("execute operator: ERROR --> %s" %
                                  traceback.format_exc())
            pid,r_chans,w_chans,err = x

            r_chans[0].flag = conf.PROCESS_STDOUT
            r_chans[1].flag = conf.PROCESS_STDERR
            for ch in r_chans:
                ch.buf = cStringIO.StringIO()

            if pid is None:
                ParaLiteLog.info("Failed to create process %s\n" % cmd)
                continue
            proc = ParaLiteProcess(
                cqid, pid, op.name, op.id, node, r_chans, w_chans, err)
            self.process[pid] = proc
            # insert the start time into the database
            """
            @TODO
            """
            t = time.time()
        ParaLiteLog.debug("execute operator: FINISH")
            
    def schedule_jobs(self):
        """schedule all jobs in the job queue

        Job status transfer:
                               <-(6)-- FINISH <
                               |              |
                               |             (4)
                               |              |
          NOTREADY --(1)->  READY  --(2)-> RUNNING
                               |              |
                               |             (3)
                               |              |
                               <---(5)-- FAIL <

             N: the number of all jobs on each node
             M: the maximum number of processors running on each node
             JOB_TYPE: 1 --> input is pipelined without persistant
                       2 --> input is pipelined with persistant
                       3 --> input is requried to be persisted
        (1): N = M; leaf node; a child is returned and JOB_TYPE = 1 or 2
        (2): Schedule Policy: a idle worker firstly gets a local job, if all
                              local jobs are finished, it gets a replica job
        (3): some exceptions occur
        (4): execute succussfully
        (5): if the parent job is failed and the output is not persisted
        (6): if JOB_TYPE = 3
        """
        idle_workers = self.get_idle_worker(task)
        for worker in idel_workers:
            jobs = self.get_job(worker)
            for job in jobs:
                self.execute_job(job)

    def execute_job(self, job):
        args = self.mk_job_args(job)
        name = '%s%s.py' % (path, op.name)
        cmd = "gxpc e -h %s python %s %s" % (worker, name, args)
        ParaLiteLog.debug("gxpc e -h %s python %s" % (worker, name))
        pipes = [ (None, "w", 0), (None, "r", 1), (None, "r", 2) ]
        try:
            x = self.iom.create_process(cmd.split(), None, None, pipes, None)
        except:
            ParaLiteLog.info(traceback.format_exc())
        pid,r_chans,w_chans,err = x

        r_chans[0].flag = conf.PROCESS_STDOUT
        r_chans[1].flag = conf.PROCESS_STDERR
        for ch in r_chans:
            ch.buf = cStringIO.StringIO()

        #ParaLiteLog.info("pid = %s proc = %s" % (pid, op.name))
        if pid is None:
            ParaLiteLog.info("Failed to create process %s\n" % cmd)
        proc = my_process(pid, op.name, node, r_chans, w_chans, err)
        self.process[pid] = proc
    
    def get_available_port(self, node):
        """get available port on the node

        Usually, the port is set by the system itself, however, in some cases, there
        are only a set of ports are available and specified by user. 
        """
        userconf = self.userconfig
        if userconf.port_range != []:
            if w not in userconf.node_port:
                userconf.node_port[w] = []
                for p in userconf.port_range:
                    userconf.node_port[w].append(p)
            port = userconf.node_port[w].pop(0)
        else:
            port = 0
        return port

    def schedule_task(self, task):
        """
        Each general operators are scheduled based on the data partioning info;
        UDX operators are schduled according to the clients info;
        """
        plan = task.plan
        queue = deque([])
        queue.append(plan)
        split_flag = 0
        used_num = 0
        userconf = self.userconfig
        try:
            while list(queue) != []:
                op = queue.popleft()
                if op.name == logical_operator().AGGREGATE or op.name == logical_operator().ORDER_BY:
                    parnode = task.datanode
                    node = parnode[random.randint(0, len(parnode)-1)]
                    if userconf.port_range != []:
                        if node not in userconf.node_port:
                            userconf.node_port[node] = []
                            for p in userconf.port_range:
                                userconf.node_port[node].append(p)
                        port = userconf.node_port[node].pop(0)
                    else:
                        port = get_unique_num(10000,14999)
                    op.node[port] = node
                    op.worker_num = 1
                    for child in op.children:
                        child.p_node = op.node
                        queue.append(child)
                elif op.name == logical_operator().UDX:
                    task.has_udx = True
                    op.worker_num = len(task.clients)
                    count = 0
                    for w in task.clients:
                        """
                        UDX is special here. 
                        We use the count number (client id) as the key.
                        If user does not speicfy the port range, the value should be 
                        the name of the node;
                        else the value should be the node name with assigned port.
                        """
                        if userconf.port_range != []:
                            if w not in userconf.node_port:
                                userconf.node_port[w] = []
                                for p in userconf.port_range:
                                    userconf.node_port[w].append(p)
                            port = userconf.node_port[w].pop(0)
                        else:
                            port = 0
                        value = "%s:%s" % (w, port)
                        op.node[count] = value
                        count += 1
                    for child in op.children:
                        queue.append(child)
                else:
                    parnode = task.datanode
                    if self.userconfig.worker_num != []:
                        worker_num_op = self.userconfig.worker_num[op.id]
                    else:
                        worker_num_op = len(parnode)
                    if worker_num_op == -1:
                        worker_num_op = len(parnode)
                    op.worker_num = worker_num_op
                    count = 0
                    for i in range(worker_num_op):
                        node = parnode[(i + used_num) % len(parnode)] 
                        if userconf.port_range != []:
                            if node not in userconf.node_port:
                                userconf.node_port[node] = []
                                for p in userconf.port_range:
                                    userconf.node_port[node].append(p)
                            port = userconf.node_port[node].pop(0)
                        else:
                            if op.name == logical_operator().SQL:
                                port = 0
                            else:
                                port = get_uqinue_num(10000,14999)
                        value = "%s:%s" % (node, port)
                        op.node[count] = value
                        count += 1
                    used_num += worker_num_op
                    for child in op.children:
                        child.p_node = op.node
                        queue.append(child)
            return True
        except:
            traceback.print_exc()
            return False

    def parse_task(self, task):
        task.status = task.TASK_RUNNING
        try:
            start = time.time()*1000
            cons = LogicalPlanMaker(task)
            return cons.make()
        except Exception, e:
            ParaLiteLog.info(traceback.print_exc())
            task.status = task.TASK_FAIL
            return 1, traceback.format_exc()

    def execute_task(self, task):
        path = WORKING_DIR
        plan = task.plan
        queue = deque([])
        queue.append(plan)
        while list(queue) != []:
            op = queue.popleft()
            count = 0            
            for port in op.node.keys():
                opid = "%s_%s" % (op.id, count)
                args = self.mk_job_args(task, op, port, opid)
                count += 1
                name = '%s%s.py' % (path, op.name)
                node =op.node[port]
                cmd = "gxpc e -h %s python %s %s" % (node, name, args)
                ParaLiteLog.info("gxpc e -h %s python %s" % (node, name))
                #cmd = "gxpc e -h %s python %s" % (node, name)
                #cmd = "ssh %s \"python %s %s\"" % (node, name, args)
                pipes = [ (None, "w", 0), (None, "r", 1), (None, "r", 2) ]
                try:
                    x = self.iom.create_process(cmd.split(), None, None, pipes, None)
                except:
                    ParaLiteLog.info(traceback.format_exc())
                pid,r_chans,w_chans,err = x

                r_chans[0].flag = conf.PROCESS_STDOUT
                r_chans[1].flag = conf.PROCESS_STDERR
                for ch in r_chans:
                    ch.buf = cStringIO.StringIO()
                    
                if pid is None:
                    ws("failed to create process %s\n" % cmd)
                    """
                    TODO: if the process is failed, how to do?
                    """
                proc = my_process(pid, op.name, node, r_chans, w_chans, err)
                self.process[pid] = proc
            for child in op.children:
                if not child.is_visited:
                    queue.append(child)
                    child.is_visited = 1
        return True

    def execute_special_task(self, t):
        ret = None
        if t.type == t.OUTPUT:
            if len(t.cmd.split()) == 1:
                ret = "please enter right command"
            else:
                output = t.cmd.split()[1]
                if InfoCenter().update_setting_info(t.database, 2, output):
                    ret = "OK"
                else:
                    ret = "FAIL"
        elif t.type == t.ROW_SEPARATOR:
            if len(t.cmd.split()) == 1:
                ret = "please enter right command"
            else:
                sep = t.cmd.split()[1].decode("string-escape")
                if InfoCenter().update_setting_info(t.database, 0, sep):
                    ret = "OK"
                else:
                    ret = "FAIL"
        elif t.type == t.COL_SEPARATOR:
            if len(t.cmd.split()) == 1:
                ret = "please enter right command"
            else:
                sep = t.cmd.split()[1].decode("string-escape")
                if InfoCenter().update_setting_info(t.database, 1, sep):
                    ret = "OK"
                else:
                    ret = "FAIL"
        elif t.type == t.INDEX:
            table = None
            if len(t.cmd.split()) > 1:
                table = t.cmd.split()[1]
            ret = InfoCenter().get_index_info(t.database, table)
        elif t.type == t.SHOW:
            ret = InfoCenter().get_setting_info(t.database)
        elif t.type == t.TABLE:
            ret = InfoCenter().get_table_info(t.database)
        elif t.type == t.ANALYZE:
            temp_task = Task()
            temp_task.database = metadata_db
            sql = t.cmd[t.cmd.find("\"")+1:len(t.cmd)-1]
            temp_task.query = sql
            if self.parse_task(temp_task):
                ret = temp_task.plan.get()
        else:
            assert(0), t.type
            
        for addr in t.reply_sock:
            sock = socket(AF_INET,SOCK_STREAM)
            sock.connect(addr)
            ret = string.strip(ret)
            sock.send("%10s%s" % (len(ret), ret))
            sock.close()

    def execute(self): 
        task = self.queue.get()
        if isinstance(task, Task):
            ParaLiteLog.debug("parse task : START")
            err_type, err_info = self.parse_task(task)
            ParaLiteLog.info(task.plan.get())
            if err_type != -1:
                ParaLiteLog.debug("parse task : ERROR %s" % err_info)
                return err_type, err_info
            ParaLiteLog.debug("parse task : FINISH")

            # ParaLiteLog.debug("set checkpoint : START")        
            # err_type, err_info = self.set_checkpoint_heuristic(task)
            # if err_type != -1:
            #     ParaLiteLog.debug("set checkpoint for task : ERROR %s" % err_info)
            #     return err_type, err_info
            # ParaLiteLog.debug(task.plan.show_ftp())
            # ParaLiteLog.debug("set checkpoint : FINISH")

            ParaLiteLog.debug("schedule task : START")            
            err_type, err_info = self.new_schedule_task(task)
            if err_type != -1:
                ParaLiteLog.debug("schedule task : ERROR %s" % err_info)            
                return err_type, err_info
            ParaLiteLog.debug("schedule task : FINISH")
        else:
            ParaLiteLog.debug("execute special task : START")            
            ParaLiteLog.info("TASK: %s --> %s : start" % (task.id, task.type))
            self.execute_special_task(task)
            self.remove_task_from_queue(task.id)
            ParaLiteLog.debug("execute special task : FINISH")                        
        return -1, None

    def get_all_sub_plan(self, plan):
        """
        As each sub-plan consists of an operator with its children, it can be
        represented as an operator
        """
        subplans = []
        q = deque([plan])
        while list(q) != []:
            op = q.popleft()
            if op.children != []:
                subplans.append(op)
                for child in op.children:
                    q.append(child)
        return subplans

    def opt_sub_plan(self, subplan):
        """
        An subplan is actually an operator with its children. We add an virtual
        operator to the top of subplan.
        """
        all_solutions = []
        
        # a temp list to record the choice for each operator
        tl = []
        if subplan.ftproperty.checkpoint_value == 0: tl.append((0, ))
        else: tl.append((0, 1))
        for child in subplan.children:
            if child.ftproperty.checkpoint_value == 0: tl.append((0, ))
            else: tl.append((0, 1))

        # reverse all solutions
        if len(tl) == 2:
            for i in tl[0]:
                for j in tl[1]:
                    all_solutions.append((i, j))
        elif len(tl) == 3:
            for i in tl[0]:
                for j in tl[1]:
                    for k in tl[2]:
                        all_solutions.append((i, j, k))
                        
        # add an virtual root and set all property to be 1
        vroot = BasicOp()
        vroot.children = [subplan]
        vroot.id = "-1"

        # look for best solution to minimize the ect of the virtual root
        min_ect = sys.maxint
        best_solution = None
        for s in all_solutions:
            subplan.ftproperty.checkpoint_value = s[0]
            for i in range(len(subplan.children)):
                subplan.children[i].ftproperty.checkpoint_value = s[i + 1]
            ect = self.get_ect_op(vroot)
            if ect < min_ect:
                min_ect = ect
                best_solution = s

        # set the checkpoint value for operators with best solution
        subplan.ftproperty.checkpoint_value = best_solution[0]
        for i in range(len(subplan.children)):
            subplan.children[i].ftproperty.checkpoint_value = best_solution[i + 1]

        # delete virtual root
        vroot.children = []
        del(vroot)
        
    def get_ect_op(self, op):
        if len(op.children) == 0:
            ftp = op.ftproperty
            ftp.processing_time = (
                (
                getattr(conf, "exec_func_%s" % op.id) * ftp.input_tuple) + (
                getattr(conf, "trans_func_%s" % op.id) * ftp.output_tuple
                )) / ftp.node_num
            #ParaLiteLog.debug("processing_time %s" % ftp.processing_time)
            
            ftp.checkpoint_time = (
                ftp.checkpoint_value * conf.tio * ftp.output_tuple) / ftp.node_num
            #ParaLiteLog.debug("checkpointing_time %s" % ftp.checkpoint_time)

            ftp.recovery_time = ftp.processing_time
            
            recovery = (getattr(conf, "exec_func_%s" % op.id)) * (
                ftp.input_tuple * (1-ftp.checkpoint_value) / ftp.node_num + (
                    ftp.input_tuple * ftp.checkpoint_value / ftp.task_num))

            #ParaLiteLog.debug("recovery_time %s" % recovery)
            ect = ftp.processing_time + (
                getattr(conf, "fail_prob_%s" % op.id) * recovery) + ftp.checkpoint_time
            return ect
        
        else:
            for child in op.children:
                child.ftproperty.ect = self.get_ect_op(child)
                ParaLiteLog.debug("OP %s ect: %s" % (child.id, child.ftproperty.ect))
            ftp = op.ftproperty

            if op.id == "-1":
                # this is the virtual operator
                c1 = op.children[0]
                ftpc1 = c1.ftproperty
                
                if len(c1.children) == 0:
                    tp1_c1 = ftpc1.processing_time
                    
                elif len(c1.children) == 1:
                    f1 = c1.children[0].ftproperty
                    tp1_c1 = ftpc1.processing_time + f1.recovery_time * (
                        1 - f1.checkpoint_value) 
                    
                elif len(c1.children) == 2:
                    f1 = c1.children[0].ftproperty
                    f2 = c1.children[1].ftproperty
                    tp1_c1 = ftpc1.processing_time + max(
                        f1.recovery_time * (1 - f1.checkpoint_value),
                        f2.recovery_time * (1 - f2.checkpoint_value))
                    
                ftp.recovery_time = tp1_c1
                recovery = ftpc1.recovery_time * (1 - ftpc1.checkpoint_value) + (
                    conf.tio * ftp.input_tuple * ftpc1.checkpoint_value) 

                # assume processing time, checkpointing time and self-recovery time 0
                # failure probability 1
                ect = ftpc1.ect + recovery
                return ect
            

            ftp.processing_time = ((
                getattr(conf, "exec_func_%s" % op.id) * ftp.input_tuple) + (
                getattr(conf, "trans_func_%s" % op.id) * ftp.output_tuple
                )) / ftp.node_num
            
            ftp.checkpoint_time = (
                ftp.checkpoint_value * conf.tio * ftp.output_tuple) / ftp.node_num
            
            tr1 = getattr(
                conf, "exec_func_%s" % op.id) * (ftp.input_tuple * (
                1-ftp.checkpoint_value) / ftp.node_num + (
                ftp.input_tuple * ftp.checkpoint_value / ftp.task_num))
            
            if len(op.children) == 1:
                c1 = op.children[0]
                ftpc1 = c1.ftproperty
                
                if len(c1.children) == 0:
                    tp1_c1 = ftpc1.processing_time
                    
                elif len(c1.children) == 1:
                    f1 = c1.children[0].ftproperty
                    tp1_c1 = ftpc1.processing_time + f1.recovery_time * (
                        1 - f1.checkpoint_value) 
                    
                elif len(c1.children) == 2:
                    f1 = c1.children[0].ftproperty
                    f2 = c1.children[1].ftproperty
                    tp1_c1 = ftpc1.processing_time + max(
                        f1.recovery_time * (1 - f1.checkpoint_value),
                        f2.recovery_time * (1 - f2.checkpoint_value))
                    
                ftp.recovery_time = tp1_c1
                
                recovery = ftpc1.recovery_time * (1 - ftpc1.checkpoint_value) + (
                    conf.tio * ftp.input_tuple * ftpc1.checkpoint_value) + tr1

                ect = ftp.processing_time + ftpc1.ect + getattr(
                    conf, "fail_prob_%s" % op.id) * recovery + ftp.checkpoint_time

            elif len(op.children) == 2:
                c1, c2 = op.children
                ftpc1 = c1.ftproperty
                ftpc2 = c2.ftproperty

                if len(c1.children) == 0:
                    tp1_c1 = ftpc1.processing_time
                    
                elif len(c1.children) == 1:
                    f1 = c1.children[0].ftproperty
                    tp1_c1 = ftpc1.processing_time + f1.recovery_time * (
                        1 - f1.checkpoint_value) 
                    
                elif len(c1.children) == 2:
                    f1 = c1.children[0].ftproperty
                    f2 = c1.children[1].ftproperty
                    tp1_c1 = ftpc1.processing_time + max(
                        f1.recovery_time * (1 - f1.checkpoint_value),
                        f2.recovery_time * (1 - f2.checkpoint_value))

                if len(c2.children) == 0:
                    tp1_c2 = ftpc2.processing_time
                    
                elif len(c2.children) == 1:
                    f1 = c2.children[0].ftproperty
                    tp1_c2 = ftpc2.processing_time + f1.recovery_time * (
                        1 - f1.checkpoint_value) 
                    
                elif len(c2.children) == 2:
                    f1 = c2.children[0].ftproperty
                    f2 = c2.children[1].ftproperty
                    tp1_c2 = ftpc2.processing_time + max(
                        f1.recovery_time * (1 - f1.checkpoint_value),
                        f2.recovery_time * (1 - f2.checkpoint_value))
                    
                ftp.recovery_time = max(tp1_c1, tp1_c2)
                
                recovery = max(
                    ftpc1.recovery_time * (1 - ftpc1.checkpoint_value) + (
                        conf.tio*ftp.input_tuple*ftpc1.checkpoint_value/ftp.node_num),
                    ftpc2.recovery_time * (1 - ftpc2.checkpoint_value) + (
                        conf.tio*ftp.input_tuple*ftpc2.checkpoint_value/ftp.node_num
                        )) + tr1
                #ParaLiteLog.debug("recovery_time %s" % recovery)
                
                ect = ftp.processing_time + max(
                    ftpc1.ect, ftpc2.ect) + getattr(
                    conf, "fail_prob_%s" % op.id) * recovery + ftp.checkpoint_time
            return ect

    def set_checkpoint_heuristic(self, task):
        # find candidate checkpoints: set CK=0 for the operators who cannot satisfy
        # the inequality
        self.find_candidate_checkpoints(task)
        
        # store sub-plans in a list
        subplans = self.get_all_sub_plan(task.plan)

        # traverse sub-plans from bottom to top and get optimal solution for each one
        for subplan in reversed(subplans):
            self.opt_sub_plan(subplan)
            
        return -1, None

    def find_candidate_checkpoints(self, task):
        # initiate fault tolerance property for the plan
        q = deque([task.plan])
        while list(q) != []:
            op = q.popleft()

            t = op.tables[0]
            if t in task.table_alias: t = task.table_alias[t]
            nodenum = len(InfoCenter().get_data_node_info(metadata_db, t))
            op.ftproperty.node_num = nodenum
            
            # set node and task number for each operator
            if len(op.children) == 0:
                # task num is chunk num
                tasknum = InfoCenter().get_chunk_num(metadata_db) * nodenum
                op.ftproperty.task_num = tasknum

                # get tuple number of related tables
                tuple_num = 0
                for t in op.tables:
                    if t in task.table_alias: t1 = task.table_alias[t]
                    else: t1 = t
                    tn = InfoCenter().get_record_num_by_table(metadata_db, t1)
                    tuple_num += tn
                op.ftproperty.input_tuple = tuple_num

            else:
                if len(op.children) == 2:
                    op.children[0].sibling = op.children[1]
                    op.children[1].sibling = op.children[0]

                if op.name == Operator.AGGREGATE or op.name == Operator.ORDER_BY:
                    op.ftproperty.task_num = 1
                    
                elif op.name != Operator.UDX:
                    # if UDX: task_num = input_size / block_size, this is set later
                    # otherwise, task_num is specified in conf
                    if hasattr(conf, "task_num_%s" % (op.id)):
                        op.ftproperty.task_num = getattr(conf, "task_num_%s" % (op.id))
                    else:
                        op.ftproperty.task_num = getattr(
                            conf, "task_num_for_%s" % (op.name))

            for child in op.children:
                q.append(child)
                
        plan = task.plan
        # use recursive algorithm to calculate the ect and output tuples of op
        self.set_ck_op(plan)

    def single_checkpointing(self, op):
        """
        The checkpointing algorithm for operator who does not have any sibling
        """
        tio = conf.tio * op.ftproperty.output_tuple
        if 2 * tio > op.ftproperty.ect:
            op.ftproperty.checkpoint_value = 0
        else:
            op.ftproperty.checkpoint_value = 1

    def sibling_checkpointing(self, op, sibling):
        """
        The checkpointing algorithm for sibling operators
        """
        if op.ftproperty.ect < sibling.ftproperty.ect:
            large = sibling
            small = op
        else:
            large = op
            small = sibling
        small.ftproperty.checkpoint_value = 0
        self.single_checkpointing(large)

        largetio = conf.tio * large.ftproperty.output_tuple
        smalltio = conf.tio * small.ftproperty.output_tuple
        if large.ftproperty.checkpoint_value == 1:
            if large.ftproperty.ect + largetio > small.ftproperty.ect + smalltio:
                small.ftproperty.checkpoint_value = 1
            else:
                self.single_checkpoint(small)
        
    def set_ck_op(self, op):
        """
        Check if an checkpoint for each operator is not necessary based on
        the inequality 2*t_op > ect_op
        """
        if len(op.children) != 0:
            for child in op.children:
                self.set_ck_op(child)
                op.ftproperty.input_tuple += child.ftproperty.output_tuple
                
        # set related property for operator
        op.ftproperty.output_tuple = getattr(
            conf, "tuple_func_%s" % (op.id)) * op.ftproperty.input_tuple
        op.ftproperty.ect = getattr(
            conf, "exec_func_%s" % (op.id)) * op.ftproperty.input_tuple

        if op.sibling is not None:
            # Sibling Checkpointing algorithm
            sibling = op.sibling
            if sibling.ftproperty.ect == -1:
                sibling.ftproperty.output_tuple = getattr(
                    conf,
                    "tuple_func_%s" % (sibling.id)) * sibling.ftproperty.input_tuple
                sibling.ftproperty.ect = getattr(
                    conf,
                    "exec_func_%s" % (sibling.id)) * sibling.ftproperty.input_tuple
            self.sibling_checkpointing(op, sibling)
            
        else:
            # Single Checkpointing alogrithm
            self.single_checkpointing(op)
    
    def set_checkpoint(self, task):
        plan = task.plan
        if len(plan.children) == 0:
            return -1, None
        try:
            t = plan.tables[0]
            if t in task.table_alias:
                t = task.table_alias[t]
            num_of_node = len(task.datanode)
            data_size, ect, is_ck = self.set_checkpoint_op(
                plan, {}, num_of_node, task.table_alias)
            return -1, None
        except Exception, e:
            return 1, traceback.format_exc()

    def set_checkpoint_op(self, op, temp_store, num_of_node, table_alias):
        if op.is_checkpoint is not None:
            temp_store[op.id] = (op.data_size, op.ect, op.is_checkpoint)
            return op.data_size, op.ect, op.is_checkpoint
        
        if op.id in temp_store:
            return temp_store[op.id]
        
        if len(op.children) == 0:
            data_size = 0
            for t in op.tables:
                if t in table_alias:
                    t = table_alias[t]
                s = InfoCenter().get_table_size(metadata_db, t)
                data_size += s
                    
            ect = self.get_et_of_op(op, data_size, num_of_node)

            if ect > conf.CHECKPOINT_INTERVAL:
                op.is_checkpoint_est = True
            else:
                op.is_checkpoint_est = False

            temp_store[op.id] = (data_size, ect, op.is_checkpoint_est)
            return data_size, ect, op.is_checkpoint_est
        
        else:
            child_whole_data_size = 0
            child_max_ect = 0
            for child in op.children:
                data_size, ect, is_checkpoint = self.set_checkpoint_op(
                    child, temp_store, num_of_node, table_alias)

                #print child.id, data_size, ect, is_checkpoint
                child_whole_data_size += data_size
                if not is_checkpoint and ect > child_max_ect:
                    child_max_ect = ect
                
            data_size = 0.9 * child_whole_data_size
            et = self.get_et_of_op(op, data_size, num_of_node)
            ect = et + child_max_ect
            if ect > conf.CHECKPOINT_INTERVAL:
                op.is_checkpoint_est = True
            else:
                op.is_checkpoint_est = False
                
            temp_store[op.id] = (data_size, ect, op.is_checkpoint_est)
            return data_size, ect, op.is_checkpoint_est

    def get_et_of_op(self, op, data_size, num_of_node):
        if op.name == Operator.SQL:
            if len(op.tables) > 1:
                et = conf.SQL_JOIN
            else:
                if op.expression.find("group by") != -1:
                    et = conf.SQL_GROUPBY
                else:
                    et = conf.SQL_WHERE
        elif op.name == Operator.GROUP_BY:
            et = conf.GROUP_BY
        elif op.name == Operator.JOIN:
            et = conf.JOIN
        elif op.name == Operator.UDX:
            et = conf.UDX_OP
        elif op.name == Operator.ORDER_BY:
            et = conf.ORDER_BY
        elif op.name == Operator.AGGREGATE:
            et = conf.AGGREGATE

        # et : KB/s
        et = float(data_size) / 1024 / et / num_of_node
        return et
    
    def all_children_finish(self, operator):
        for child in operator.children:
            if child.status == None or child.status != conf.JOB_FINISH:
                return 0
        return 1

    def is_job_finish(self, opid):
        jobqueue = self.operator_jobqueue[opid]
        for job in jobqueue:
            if job.status != Job.FINISH:
                return 0
        return 1

    def is_job_pending(self, opid):
        jobqueue = self.operator_jobqueue[opid]
        for job in jobqueue:
            if job.status != Job.PENDING:
                return 0
        return 1

    def is_task_finish(self, cqid):
        for opid in self.operator_jobqueue:
            jobqueue = self.operator_jobqueue[opid]
            for job in jobqueue:
                if job.status != Job.FINISH:
                    return 0
        return 1

    def is_checkpoint_necessary(self, par_op, op):
        if par_op is None:
            if op.dest != conf.DATA_TO_DB:
                return None
            par_op = BasicOp()
            par_op.children = [op]
            
        # first of all, check if all child of par_op are finished
        for child in par_op.children:
            if child.data_size == -1:
                return None

        # initiate all variables
        # the max ECT of childs of a op; i = the ith child of par_op
        max_ect_of_op_child = []
        # the checkpointing value of a op (0, 1); i = the ith child of par_op
        for child in par_op.children:
            max_ect = 0
            for childchild in child.children:
                if childchild.ect > max_ect:
                    max_ect = childchild.ect
            max_ect_of_op_child.append(max_ect)

        # retrieve all cases of checkpointing value
        # e.g. if len(par_op.children) == 2: there are 2^2 cases, that is,
        # c_i, c_j = (0, 0), (0, 1), (1, 0), (1, 1)
        ck_value = []
        if len(par_op.children) == 1:
            ck_value = [(0,), (1,)]
        elif len(par_op.children) == 2:
            # at this point, we assume that a parent can have two children at most
            ck_value = [(0, 0), (0, 1), (1, 0), (1, 1)]

        min_ect = sys.maxint
        rs = 0
        for i in range(len(ck_value)):
            """
            caclulate the ect of par_op
            ECT_par_op =   max(ect_child)            -----------max_ect
                         + p*max(ect_child*(1-ck_child)) -------max_ect_with_ck
                         + e_par_op                     --------ignored
                         + ck_par_op*o_par_op           --------ignored
            """
            prob = 0.5
            ck_value_i = ck_value[i]
            max_ect = 0
            max_ect_with_ck = 0
            for j in range(len(par_op.children)):
                child = par_op.children[j]
                ck_o = self.get_overhead(child.data_size) 
                ect = child.execution_time + max_ect_of_op_child[j] + ck_value_i[j]*ck_o
                if ect > max_ect:
                    max_ect = ect
                ect_ck = ect * (1 - ck_value_i[j])
                if ect_ck > max_ect_with_ck:
                    max_ect_with_ck = ect_ck
            ect_par_op = max_ect + prob * max_ect_with_ck
            if ect_par_op < min_ect:
                min_ect = ect_par_op
                rs = i

        # # set the ECT for child
        # for i in range(len(par_op.children)):
        #     child = par_op.children[i]
        #     ck_o = self.get_overhead(child.data_size)
        #     child.ect = child.execution_time + max_ect_of_op_child[i] + ck_value[rs][i]*ck_o
        
        return ck_value[rs]

    def get_overhead(self, data_size):
        disk_speed = 200*1024*1024
        return data_size / disk_speed + 1
    
    def mk_job_args(self, task, op):
        dic = {}
        attrs = {}

        if task.table_alias != {}:
            table_to_alias = {}  
            
            # get all original tables
            tbls = []
            for t in op.tables:
                if t in task.table_alias:
                    tbls.append(task.table_alias[t])
                    table_to_alias[task.table_alias[t]] = t
            # since in all operators, we use alias of table, so we have to replace the
            # orginal table name by the alias again
            attrs = InfoCenter().get_all_attrs(metadata_db, tbls)

            new_attrs = {}
            for attr in attrs:
                if attr.find(".") != -1:
                    m = attr.split(".")
                    n_attr = "%s.%s" % (table_to_alias[m[0]], m[1])
                else:
                    n_attr = attr
                new_attrs[n_attr] = attrs[attr]

        else:
            new_attrs = InfoCenter().get_all_attrs(metadata_db, op.tables)

        # set the type for the alais for some attributes
        for col in task.column_alias:
            if col in new_attrs:
                new_attrs[task.column_alias[col][0]] = new_attrs[col]
            else:
                if col.find("(") != -1:
                    # this is a function
                    fun = col[0:col.find("(")]
                    if fun in conf.GENERAL_FUNC:
                        # TODO: consider it as FLOAT
                        new_attrs[task.column_alias[col][0]] = conf.FLOAT
        dic[conf.ATTRS] = new_attrs
            
        # common args for all operators
        # start
        dic[conf.TEMP_DIR] = self.userconfig.temp_dir
        dic[conf.INPUT] = op.input
        dic[conf.OUTPUT] = op.output
        dic[conf.NAME] = op.name

        dic[conf.OUTPUT_DIR] = task.output_dir
        dic[conf.DB_ROW_SEP] = InfoCenter().get_separator(metadata_db)[0]
        dic[conf.DB_COL_SEP] = InfoCenter().get_separator(metadata_db)[1]
        dic[conf.CLIENT_SOCK] = task.reply_sock

        # set limit
        if op.limit != 0:
            dic[conf.LIMIT] = op.limit
            
        t = op.tables[0]
        if t in task.table_alias:
            t = task.table_alias[t]

        # get the parent op
        datanode = task.datanode                
        par_op = None
        q = deque([])
        q.append(task.plan)
        while list(q) != []:
            t_op = q.popleft()
            for child in t_op.children:
                if child == op:
                    par_op = t_op
                    break
            for child in t_op.children:
                q.append(child)
                
        if par_op is not None:
            if par_op.name == Operator.JOIN or par_op.name == Operator.GROUP_BY:
                #dic[conf.PARTITION_NUM] = InfoCenter().get_chunk_num(
                # metadata_db)*len(datanode)
                if hasattr(conf, "task_num_%s" % par_op.id):
                    pnum = getattr(conf, "task_num_%s" % par_op.id)
                else:
                    pnum = getattr(conf, "task_num_for_%s" % par_op.name)
                dic[conf.PARTITION_NUM] = pnum
            else:
                dic[conf.PARTITION_NUM] = 1
        num_of_children = len(datanode)
        dic[conf.NUM_OF_CHILDREN] = num_of_children
        
        if task.failed_node != []:
            dic[conf.FAILED_NODE] = task.failed_node
        
        if op.p_node != {}:
            dic[conf.P_NODE] = op.p_node
        else:
            dic[conf.P_NODE] = {}
            
        if op.split_key != None:
            dic[conf.SPLIT_KEY] = op.split_key

        if op.dest != None: dic[conf.DEST] = op.dest
        dic[conf.EXPRESSION] = op.expression
        
        dic[conf.IS_CHECKPOINT] = op.ftproperty.checkpoint_value
        # end
        
        if op.dest == conf.DATA_TO_DB:
            t = op.map[conf.DEST_TABLE]
            dic[conf.DEST_TABLE] = t
            dic[conf.DEST_DB] = task.database
            hash_key = InfoCenter().get_table_partition_key(task.database, t)
            if hash_key is not None and hash_key != []:
                dic[conf.FASHION] = conf.HASH_FASHION
                dic[conf.HASH_KEY] = hash_key
                aa = InfoCenter().get_table_columns(task.database, t)
                temppos = []
                for eachkey in hash_key:
                    temppos.append(aa.index(eachkey))
                dic[conf.HASH_KEY_POS] = temppos
            else:
                dic[conf.FASHION] = conf.ROUND_ROBIN_FASHION     
        """
        special for SQL operator: database info
        get active database name for each node
        """
        if op.name == Operator.SQL:
            if self.userconfig.cache_size != 0:
                dic[conf.CACHE_SIZE] = self.userconfig.cache_size
            if self.userconfig.temp_store != 0:
                dic[conf.TEMP_STORE] = self.userconfig.temp_store

        if op.name == Operator.GROUP_BY:
            dic[conf.IS_CHILD_SQL] = op.is_child_sql
            dic[conf.KEY] = op.group_key
            dic[conf.FUNCTION] = op.function
            dic[conf.GROUP_KEY] = op.group_key
            
        elif op.name == Operator.AGGREGATE:
            dic[conf.FUNCTION] = op.function
            dic[conf.IS_CHILD_SQL] = op.is_child_sql

        elif op.name == Operator.ORDER_BY:
            dic[conf.ORDER_KEY] = op.order_key
            dic[conf.ORDER_TYPE] = op.order_type
            dic[conf.IS_CHILD_SQL] = op.is_child_sql

        elif op.name == Operator.UDX:
            dic[conf.KEY] = op.keys
            dic[conf.UDX] = op.udxes
            time_str = time.strftime('%Y-%m-%d-%H-%M-%S',time.localtime(time.time()))
            dic[conf.LOCAL_ADDR] = "%s%s%s-%s-%s" % (self.userconfig.temp_dir,
                                                     os.sep, "UNIX.d", time_str,
                                                     random.randint(1,10000))
        elif op.name == Operator.JOIN:
            """
            the inputs of JOIN operator is special:
            {'opid1', ['in1', 'in2']}
            {'opid2', ['in2', 'in2']}
            """
            dic[conf.PREDICTION] = op.prediction
            dic[conf.INPUT] = op.map
        s = cPickle.dumps(dic)
        b = base64.encodestring(s)
        return b

    def set_job_status(self, message):
        op_name, op_id, op_status, cost_time= self.parse(message)
        self.set_op_status(self.plan, op_name, op_id, op_status, cost_time)

class InfoCenter:

    def create_metadata_table(self, database):
        conn = sqlite3.connect(database)
        """
        First of all, check metadata tables:
        (1) setting_info:      |row_separator|col_separator|output|chunk|replica|
        (2) data_node_info:    |table_name|node_name|
        (3) table_size_info:   |name|size|record_num|
        (4) table_attr_info:   |name|attribute|type|is_key|is_index|index_name
        (5) table_pos_info:    |name|node|db|size|
        (6) table_partition_info: |name|partition_num|partition_key|
        (7) sub_db_info:       |node|db_name|db_size|status|
        (8) db_replica_info:   |db_name|db_replica_name|node|
        """
        cr = conn.cursor()
        tsql = 'select * from sqlite_master where type="table" and name="%s"' % (
            conf.SETTING_INFO)
        cr.execute(tsql)
        rs = cr.fetchall()
        if len(rs) == 0:
            csql = "create table setting_info(row_separator varchar(10), col_separator varchar(10), output varchar(100), chunk int, replica int)"
            cr.execute(csql)
            conn.commit()
            sql = 'insert into setting_info values("\n", "|", "stdout", 1, 1)'
            cr.execute(sql)
            conn.commit()

        tsql = 'select * from sqlite_master where type="table" and name="%s"' % (
            conf.DATA_NODE_INFO)
        cr.execute(tsql)
        rs = cr.fetchall()
        if len(rs) == 0:
            csql = "create table data_node_info (table_name, node_name varchar(50))"
            cr.execute(csql)
            conn.commit()
        
        tsql = 'select * from sqlite_master where type="table" and name="%s"' % (
            conf.TABLE_SIZE_INFO)
        cr.execute(tsql)
        rs = cr.fetchall()
        if len(rs) == 0:
            csql = "create table table_size_info(name varchar(20) primary key, size int, record_num int)"
            cr.execute(csql)
            conn.commit()

        tsql = 'select * from sqlite_master where type="table" and name="%s"' % (
            conf.TABLE_ATTR_INFO)
        cr.execute(tsql)
        rs = cr.fetchall()
        if len(rs) == 0:
            csql = "create table table_attr_info(name varchar(20), attribute varchar(20), type varchar(20),is_key varchar(5),is_index varchar(5), index_name varchar(20))"
            cr.execute(csql)
            conn.commit()

        tsql = 'select * from sqlite_master where type="table" and name="%s"' % (
            conf.TABLE_POS_INFO)
        cr.execute(tsql)
        rs = cr.fetchall()
        if len(rs) == 0:
            csql = "create table table_pos_info(name varchar(20), node varchar(50), db varchar(20), size int)"
            cr.execute(csql)
            conn.commit()

        tsql = 'select * from sqlite_master where type="table" and name="%s"' % (
            conf.TABLE_PARTITION_INFO)
        cr.execute(tsql)
        rs = cr.fetchall()
        if len(rs) == 0:
            csql = 'create table table_partition_info(name varchar(20) primary key,  partition_num int, partition_key varchar(50))'
            cr.execute(csql)
            conn.commit()

        tsql = 'select * from sqlite_master where type="table" and name="%s"' % (
            conf.SUB_DB_INFO)
        cr.execute(tsql)
        rs = cr.fetchall()
        if len(rs) == 0:
            csql = "create table sub_db_info(node varchar(50),db_name varchar(20), db_size int, status int)"
            cr.execute(csql)
            conn.commit()

        tsql = 'select * from sqlite_master where type="table" and name="%s"' % (
            conf.DB_REPLICA_INFO)
        cr.execute(tsql)
        rs = cr.fetchall()
        if len(rs) == 0:
            csql = "create table db_replica_info(db_name varchar(50), db_replica_name varchar(50), node varchar(50))"
            cr.execute(csql)
            conn.commit()

        cr.close()
    
    def update_table_partition_info(self, database, table, key):
        conn = sqlite3.connect(database)
        c = conn.cursor()
        sql = 'update table_partition_info set partition_key = "%s" where name="%s"' % (table)
        c.execute(sql)
        conn.commit()
        conn.close()

    def get_table_columns(self, database, table):
        conn = sqlite3.connect(database)
        columns = []
        c = conn.cursor()
        sql = 'select attribute from table_attr_info where name="%s"' % (table)
        c.execute(sql)
        rs = c.fetchall()
        for r in rs:
            columns.append(r[0])
        return columns

    def get_separator(self, database):
        conn = sqlite3.connect(database)
        c = conn.cursor()
        sql = "select row_separator, col_separator from setting_info"
        rs = c.execute(sql).fetchone()
        output = (rs[0].encode("ascii"), rs[1].encode("ascii"))
        conn.close()
        return output
        
    def get_output(self, database):
        conn = sqlite3.connect(database)
        c = conn.cursor()
        sql = "select output from setting_info"
        rs = c.execute(sql).fetchone()
        output = rs[0]
        conn.close()
        return output

    def get_chunk_num(self, database):
        conn = sqlite3.connect(database)
        c = conn.cursor()
        sql = "select chunk from setting_info"
        rs = c.execute(sql).fetchone()
        output = rs[0]
        conn.close()
        return output

    def get_replica_num(self, database):
        conn = sqlite3.connect(database)
        c = conn.cursor()
        sql = "select replica from setting_info"
        rs = c.execute(sql).fetchone()
        output = rs[0]
        conn.close()
        return output

    def get_replica_info(self, database):
        conn = sqlite3.connect(database)
        c = conn.cursor()
        sql = "select * from db_replica_info"
        rs = c.execute(sql).fetchall()
        if rs is None or len(rs) == 0:
            return None
        output = []
        for row in rs:
            output.append(" ".join(row))
        conn.close()
        return output

    def get_replica_by_db(self, metadb, db):
        """get the replica information for db

        @param metadb  the metadata database
        @param db      the target database
        """
        conn = sqlite3.connect(metadb)
        c = conn.cursor()
        sql = "select db_replica_name, node from db_replica_info where db_name = '%s'" % db
        rs = c.execute(sql).fetchall()
        if rs is None or len(rs) == 0:
            return []
        output = []
        for row in rs:
            output.append(" ".join(row))
        conn.close()
        return output

    def get_replica_by_node(self, metadb, node):
        """get the replica information for node

        @param metadb  the metadata database
        @param node    the target data node
        """
        conn = sqlite3.connect(metadb)
        c = conn.cursor()
        sql = "select db_replica_name from db_replica_info where node = '%s'" % node
        rs = c.execute(sql).fetchall()
        if rs is None or len(rs) == 0:
            return []
        output = []
        for row in rs:
            output.append(row[0].encode("ascii"))
        conn.close()
        return output

        
    def get_index_info(self, database, table):
        conn = sqlite3.connect(database)
        ret = []
        conn = sqlite3.connect(database)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        if table is not None:
            sql = 'select attribute,index_name from table_attr_info where name="%s" and is_index = "yes"' % (table)
            c.execute(sql)
            rs = c.fetchall()
            for row in rs:
                ret.append(row[0] + " : " + row[1])
        else:
            sql = 'select name,attribute,index_name from table_attr_info where is_index="yes"'
            c.execute(sql)
            rs = c.fetchall()
            for row in rs:
                ret.append(row[0] + " : " + row[1] + " : " + row[2])
        conn.close()
        if len(ret) > 0:
            return "\n".join(ret)
        else:
            return "NO index now!"
    
    def get_setting_info(self, database):
        conn = sqlite3.connect(database)
        ret = []
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        sql = "select * from setting_info"
        c.execute(sql)
        rs = c.fetchone()
        for i in range(len(rs)):
            temp = rs[i]
            if temp == "\n":
                # show \n instead of empty line
                temp = "\\n"
            ret.append(rs.keys()[i] + " : " + str(temp))
        conn.close()
        return "\n".join(ret)

    def get_table_name_by_col(self, database, col):
        conn = sqlite3.connect(database)
        c = conn.cursor()
        sql = 'select name from table_attr_info where attribute="%s"' % (col)
        c.execute(sql)
        rs = c.fetchone()
        table_name = None
        if rs is not None and len(rs) != 0:
            table_name = rs[0]
        conn.close()
        return table_name
        
    def get_table_db_name(self, database, table):
        conn = sqlite3.connect(database)
        c = conn.cursor()
        sql = 'select db from table_pos_info where name="%s"' % (table)
        c.execute(sql)
        rs = c.fetchall()
        output = []
        for row in rs:
            output.append(row[0])
        conn.close()
        return output

    def get_table_size(self, database, table):
        conn = sqlite3.connect(database)
        c = conn.cursor()
        sql = 'select size from table_size_info where name="%s"' % (table)
        c.execute(sql)
        rs = c.fetchone()[0]
        return rs

    def get_record_num_by_table(self, database, table):
        conn = sqlite3.connect(database)
        c = conn.cursor()
        sql = 'select record_num from table_size_info where name="%s"' % (table)
        c.execute(sql)
        rs = c.fetchone()[0]
        return rs
        
    def get_active_db(self, database, node):
        conn = sqlite3.connect(database)
        c = conn.cursor()
        sql = 'select db_name from sub_db_info where node="%s" and status = %s' % (node, 1)
        dbs = []
        for row in c.execute(sql):
            dbs.append(row[0].encode("ascii"))
        conn.close()
        return dbs

    # return a list of attrs     [['x.a'],['y.a','y.b']]
    def get_all_attrs(self, database, tables):
        attrs = {}
        for table in tables:
            self.get_all_attrs_table(database, table, attrs)
        return attrs

    # return a list of attrs for a table ['x.a', 'x.b']
    def get_all_attrs_table(self, database, table, attrs):
        conn = sqlite3.connect(database)
        c = conn.cursor()
        sql = 'select attribute,type from table_attr_info where name = "%s"' % (table)
        c.execute(sql)
        rs = c.fetchall()
        for row in rs:
            _attr = table + '.' + row[0]
            attrs[_attr] = row[1]
        conn.close()
          
    # return a list of nodes that have data
    def get_data_node_info(self, database, table):
        conn = sqlite3.connect(database)
        nodes = []
        c = conn.cursor()
        sql = "select node_name from data_node_info where table_name ='%s'" % (table)
        c.execute(sql)
        rs = c.fetchall()
        for row in rs:
           nodes.append(row[0])
        conn.close()
        return nodes
    
    def get_table_partition_key(self, database, table):
        conn = sqlite3.connect(database)
        c = conn.cursor()
        sql = 'select partition_key from table_partition_info where name = "%s"' % (table)
        c.execute(sql)
        rs = c.fetchone()
        if rs is not None and len(rs) > 0:
            keys = rs[0]
            if keys == "":
                return []
            conn.close()
            return keys.split(",")
        return []

    def get_table_info(self, database):
        conn = sqlite3.connect(database)
        c = conn.cursor()
        sql = 'select name from table_partition_info'
        c.execute(sql)
        rs = c.fetchall()
        tables = []
        if rs:
            for row in rs:
                tables.append(row[0])
        conn.close()
        return "\n".join(tables)
    """
    Table: data_pos_info   |database_name|table_name|partition_num|partition_key|
    """
    def insert_data_pos_info(self, database, dbname, table, par_num, key):
        conn = sqlite3.connect(database)
        cr = conn.cursor()
        sqlcheck = 'select * from data_pos_info where database_name = "%s" and table_name = "%s"' % (dbname, table)
        cr.execute(sqlcheck)
        rs = cr.fetchall()
        if len(rs) != 0:
            sqldelete = 'delete from data_pos_info where database_name = "%s" and table_name = "%s"' % (dbname, table)
            cr.execute(sqldelete)
            conn.commit()
        sql = 'insert into data_pos_info values("%s", "%s", %s, "%s")' % (dbname,
                                                                          table,
                                                                          par_num, key)
        cr.execute(sql)
        conn.commit()
        cr.close()

    def update_data_pos_info(self, database, dbname, table, key):
        conn = sqlite3.connect(database)
        cr = conn.cursor()
        sql = 'update data_pos_info set partition_key = "%s" where database_name = "%s" and table_name = "%s"' % (key, dbname, table)
        cr.execute(sql)
        conn.commit()
        cr.close()

    def delete_data_pos_info(self, database, dbname, table):
        conn = sqlite3.connect(database)        
        conn = sqlite3.connect(database)
        cr = conn.cursor()
        sqldelete = 'delete from data_pos_info where database_name = "%s" and table_name = "%s"' % (dbname, table)
        cr.execute(sqldelete)
        conn.commit()
        cr.close()

    """
    Table: table_attr_info |name|attribute|type|is_key|is_index|
    """
    def insert_table_attr_info(self, database, table, column):
        conn = sqlite3.connect(database)
        cr = conn.cursor()
        sqldelete = 'delete from table_attr_info where name = "%s"' % (table)
        cr.execute(sqldelete)
        conn.commit()
        for i in range(len(column)):
            is_key = "no"
            if column[i][0].lower()!="primary":
                if i<len(column)-1:
                    if (column[i+1][0]).lower() == "primary":
                        is_key = "yes"
                if len(column[i]) < 2:
                    col_type = "varchar"
                else:
                    col_type = column[i][1]
                sql = 'insert into table_attr_info values("%s", "%s", "%s", "%s", "%s", "%s")' % (table, column[i][0], col_type, is_key, "no", "")
                cr.execute(sql)
                conn.commit()
        conn.commit()
        cr.close()

    def update_table_info(self, database, table, column, index_name):
        conn = sqlite3.connect(database)
        cr = conn.cursor()
        for col in column:
            sql = 'update table_attr_info set is_index = "yes", index_name = "%s" where name = "%s" and attribute = "%s"' % (index_name, table, col)
            cr.execute(sql)
        conn.commit()
        cr.close()
        
    def delete_table_info(self, database, table):
        conn = sqlite3.connect(database)
        cr = conn.cursor()
        # (1) delete info in table_attr_info, table_partition_info and table_size_info
        sqldelete = 'delete from table_attr_info where name = "%s"' %  (table)
        cr.execute(sqldelete)
        sqldelete = 'delete from table_size_info where name = "%s"' %  (table)
        cr.execute(sqldelete)
        sqldelete = 'delete from table_partition_info where name = "%s"' %  (table)
        cr.execute(sqldelete)
        conn.commit()
        # (2) get the table size on each node
        sqlquery = 'select node, db, size from table_pos_info where name="%s"' % (table)
        rs = cr.execute(sqldelete).fetchall()
        for row in rs:
            # (3) update the info of db size in sub_db_info
            node = row[0]
            db = row[1]
            size = row[2]
            temp = 'select db_size from sub_db_info where node="%s" and db_name="%s"' % (node,db)
            cur_size = cr.execute(temp).fetchone()[0]
            new_size = cur_size - size
            temp = 'update sub_db_info set db_size=%s where node="%s" and db_name="%s"' % (new_size, node,db)
            cr.execute(temp)
            conn.commit()
        # (4) delete info in table_pos_info
        sqldelete = 'delete from table_pos_info where name="%s"' % (table)
        cr.execute(sqldelete)
        conn.commit()
        # (5) delete info in data_node_info
        sqldelete = 'delete from data_node_info where table_name = "%s"' % (table)
        cr.execute(sqldelete)
        conn.commit()
        cr.close()

    """
    sub_db_info: |node|db_name|db_size|status|
    """
    def insert_sub_db_info(self, database, table):
        conn = sqlite3.connect(database)
        cr = conn.cursor()
        f = open(CONF_DIR + NODE_CONF, "rb")
        part = string.strip(f.read()).split("\n")
        sqlcheck = 'select * from sub_db_info where db_name like "%s_%%"' % (database)
        cr.execute(sqlcheck)
        rs = cr.fetchall()
        if len(rs) == 0:
            sqldelete = 'delete from partition_info where database_name like "%s_%%" and table_name = "%s"' % (dbname, table)
            cr.execute(sqldelete)
            conn.commit()
        real_db_name = "%s_%d" % (dbname, 0)
        for i in range(len(part)):
            sql = 'insert into partition_info values("%s", "%s", "%s", %d, %d)' % (part[i], real_db_name, table, 0, 0)
            cr.execute(sql)
        conn.commit()
        cr.close()

    def update_sub_db_info(self, database, table):
        conn = sqlite3.connect(database)
        # query the size of the table
        cr = conn.cursor()
        sql = 'select nsize from table_pos_info where name="%s"' % (table)
        cr.execute(sql)
        rs = cr.fetchall()
        size = rs[0][0]
        sql = "select db_size from sub_db_"
        cr.close()

    def update_sub_db_info2(self, database, dbs):
        conn = sqlite3.connect(database)
        cr = conn.cursor()
        for i in range(1, len(dbs)):
            cur_db = dbs[i]
            dbname = cur_db[conf.DATABASE]
            table = cur_db[conf.TABLE]
            node = cur_db[conf.NODE]
            added_record = cur_db[conf.ADDED_RECORD]
            added_size = cur_db[conf.ADDED_SIZE]
            insert_sql = 'insert into partition_info values("%s", "%s", "%s", %d, %d)' % (node, dbname , table, added_record, added_size)

        cur_db = dbs[0]
        dbname = cur_db[conf.DATABASE]
        table = cur_db[conf.TABLE]
        node = cur_db[conf.NODE]
        added_record = cur_db[conf.ADDED_RECORD]
        added_size = cur_db[conf.ADDED_SIZE]
        select_sql = 'select table_size, record_num from partition_info where partition_node = "%s" and database_name = "%s" and table_name = "%s"' % (node, dbname, table)
        rs = cr.execute(select_sql).fetchall()
        record = rs[0][1] + added_record
        size = rs[0][0] + added_size
        update_sql = 'update partition_info set table_size = %s, record_num = %s where partition_node = "%s" and database_name = "%s" and table_name = "%s"' % (size, record, node, dbname, table)
        cr.execute(update_sql)
        conn.commit()
        cr.close()

    def delete_partition_info(self, database, dbname, table):
        conn = sqlite3.connect(database)
        cr = conn.cursor()
        sqldelete = 'delete from partition_info where database_name = "%s" and table_name = "%s"' % (dbname, table)
        cr.execute(sqldelete)
        conn.commit()
        cr.close()

    # invoked when table is created
    def insert_table_info(self, database, table, column, nodes, chunk_num, replica_num, hash_key):
        conn = sqlite3.connect(database)
        cr = conn.cursor()
        # set the chunk and replica num
        sql = "update setting_info set chunk=%s, replica=%s" % (chunk_num, replica_num)
        cr.execute(sql)
        conn.commit()
        
        # insert table_attr_info
        sqldelete = 'delete from table_attr_info where name = "%s"' % (table)
        cr.execute(sqldelete)
        conn.commit()
        for i in range(len(column)):
            is_key = "no"
            if column[i][0].lower()!="primary":
                if i<len(column)-1:
                    if (column[i+1][0]).lower() == "primary":
                        is_key = "yes"
                if len(column[i]) < 2:
                    col_type = conf.STRING
                else:
                    col_type = column[i][1]
                    if col_type.lower() in ["int", "integer"]:
                        col_type = conf.INT
                    elif col_type.lower() == "float":
                        col_type = conf.FLOAT
                    elif col_type.lower() == "real":
                        col_type = conf.REAL
                    elif col_type.lower() == "text":
                        col_type = conf.TEXT
                    else:
                        col_type = conf.STRING
                        
                sql = 'insert into table_attr_info values("%s", "%s", "%s", "%s", "%s", "%s")' % (table, column[i][0], col_type, is_key, "no", "")
                cr.execute(sql)
                conn.commit()
                
        # insert data_node_info and sub_db_info
        i = 0
        cr.execute('delete from data_node_info where table_name = "%s"' % (table))
        conn.commit()
        new_db = []
        for n in nodes:
            sql = 'insert into data_node_info values("%s", "%s")' % (table, n)
            cr.execute(sql)
            tt = cr.execute('select * from sub_db_info where node="%s" and status=%s' % (n, 1)).fetchall()
            if len(tt) != chunk_num:
                j = len(tt)
                while j < chunk_num:
                    # db_name = name_hostname_partitionNum_chunkNum
                    db_name = "%s_%s_%s_%s" % (database, n, str(i), str(j))
                    sql = 'insert into sub_db_info values("%s", "%s", %s, %s)' % (n, db_name, 0, 1)
                    cr.execute(sql)
                    new_db.append(db_name)
                    j += 1
            i += 1
            
        # insert db_replica_info
        # using Interlevead Declustering to decide the replica node
        # make sure the last sub cluster has at least two nodes
        if replica_num == 2:
            node_num = len(nodes)        
            # sub_nodes_num = min(chunk_num + 1, node_num)
            # if node_num % sub_nodes_num == 1:
            #     if sub_nodes_num > 2:
            #         sub_nodes_num -= 1
            #     else:
            #         sub_nodes_num += 1
            sub_nodes_num = conf.SUB_CLUSTER_SIZE
            
            chunk_count = 1
            pos_count = 1
            for db in new_db:
                db_node = db.split("_")[-3]
                pos = nodes.index(db_node)
                if pos >= node_num / sub_nodes_num * sub_nodes_num:
                    temp_num = node_num % sub_nodes_num
                    sub_nodes = nodes[-temp_num:]
                else:
                    sub_nodes = nodes[pos/sub_nodes_num * sub_nodes_num : (pos/sub_nodes_num + 1) * sub_nodes_num]
                    temp_num = sub_nodes_num

                pos_in_sub_nodes = sub_nodes.index(db_node)
                replica_pos = (pos_in_sub_nodes + pos_count) % temp_num
                if replica_pos == pos_in_sub_nodes:
                    replica_pos = (replica_pos + 1) % temp_num
                    pos_count += 1

                db_name = "%s_r_0" % (db)
                sql = 'insert into db_replica_info values("%s", "%s", "%s")' % (db, db_name, sub_nodes[replica_pos])
                cr.execute(sql)

                if chunk_count == chunk_num:
                    chunk_count = 1
                    pos_count = 0
                else:
                    chunk_count += 1
                    pos_count += 1

            conn.commit()
            
        # for db in new_db:
        #     k = 0
        #     n = db.split("_")[-3]
        #     used_node = [n]
        #     while k < replica_num - 1:
        #         r_node = nodes[random.randint(0, len(nodes) - 1)]
        #         while r_node in used_node:
        #             r_node = nodes[random.randint(0, len(nodes) - 1)]
        #         used_node.append(r_node)
        #         # replica_db_name = name_hostname_partitionNum_chunkNum_r_num
        #         db_name = "%s_r_%s" % (db, k)
        #         sql = 'insert into db_replica_info values("%s", "%s", "%s")' % (db, db_name, r_node)
        #         cr.execute(sql)
        #         k += 1
        # conn.commit()
        
        # insert table_size_info
        sql = 'delete from table_size_info where name = "%s"' % (table)
        cr.execute(sql)
        conn.commit()
        sql = 'insert into table_size_info values("%s", %s, %s)' % (table, 0, 0) 
        cr.execute(sql)
        conn.commit()
 
       # insert table_pos_info
        cr.execute('delete from table_pos_info where name="%s"' % (table))
        conn.commit()
        sql = "select node, db_name from sub_db_info where status = 1" 
        cr.execute(sql)
        rs = cr.fetchall()
        i = 0
        old_node = None
        for row in rs:
            node = row[0]
            if old_node is None or old_node != node:
                old_node = node
                i = 0
            if i >= chunk_num:
                continue
            db_name = row[1]
            sql = 'insert into table_pos_info values("%s", "%s", "%s", 0)' % (table, node, db_name)
            cr.execute(sql)
            i += 1
        conn.commit()

        # insert table_partition_info
        sql = 'delete from table_partition_info where name = "%s"' % (table)
        cr.execute(sql)
        conn.commit()
        sql = 'select node_name from data_node_info where table_name="%s"' % (table)
        num = len(cr.execute(sql).fetchall())
        sql = 'insert into table_partition_info values("%s", %s, "%s")' % (table, num, hash_key) 
        cr.execute(sql)
        conn.commit()
        cr.close()
        
    """
    Table db_info:  |node_name|db_name|db_size|status|
    """
    def insert_db_info(self, database, nodes, dbname):
        conn = sqlite3.connect(database)
        c = conn.cursor()
        for n in nodes:
            db_real_name = dbname + "_" + "0"
            sqlcheck = 'select * from db_info where node_name = "%s" and db_name = "%s"' % (n, db_real_name)
            c.execute(sqlcheck)
            rs = c.fetchall()
            if len(rs) != 0:
                continue
            sql = 'insert into db_info values ("%s", "%s", %s, %s)' % (n, db_real_name, 0, 1)
            c.execute(sql)
        conn.commit()
        cr.close()


    def update_db_info(self, dbs):
        conn = sqlite3.connect(metadata_db)
        cr = conn.cursor()
        total_size = 0   # the total increased size of table 
        total_record_num = 0
        if len(dbs) == 0:
            ParaLiteLog.debug("WARNING: There is no modified db from dload_server, please check it!!")
            return
        for i in range(len(dbs)):
            cur_db = dbs[i]
            dbname = cur_db[conf.DATABASE]
            node = cur_db[conf.NODE]
            table = cur_db[conf.TABLE]
            status = cur_db[conf.STATUS]
            db_size = cur_db[conf.DB_SIZE]
            added_record = cur_db[conf.ADDED_RECORD]
            added_size = cur_db[conf.ADDED_SIZE]
            total_size += added_size
            total_record_num += added_record
            update_sql = 'update sub_db_info set db_size=%s where db_name="%s"' % (db_size, dbname)
            cr.execute(update_sql)
            sql = 'select size from table_pos_info where name = "%s" and node ="%s" and db = "%s"' % (table, node, dbname)
            current_size = cr.execute(sql).fetchone()[0]
            update_sql = 'update table_pos_info set size=%s where name = "%s" and node ="%s" and db = "%s"' % (current_size + added_size, table, node, dbname)
            cr.execute(update_sql)
            conn.commit()

        
        sql = 'select size,record_num from table_size_info where name = "%s"' % (table)
        cr.execute(sql)
        rs = cr.fetchone()
        cu_size = rs[0]
        re_num = rs[1]
        sql = 'update table_size_info set size = %s, record_num = %s where name ="%s"'  % (cu_size + total_size, re_num + total_record_num, table)
        cr.execute(sql)
        conn.commit()
        cr.close()
    
    # return sub-database_name and its size
    def get_db_info(self, database, node, status):
        conn = sqlite3.connect(database)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        sql = 'select db_name, db_size from sub_db_info where node = "%s" and status = %s' % (node, status)
        rs = c.execute(sql).fetchall()
        ret = ("%s:%s" % (rs[0][0], rs[0][1]))
        conn.close()
        return ret

    def update_setting_info(self, database, tag, arg):
        conn = sqlite3.connect(database)
        c = conn.cursor()
        if tag == 0:
            sql = 'update setting_info set row_separator = "%s"' % (arg)
        elif tag == 1:
            sql = 'update setting_info set col_separator = "%s"' % (arg)
        elif tag == 2:
            sql = 'update setting_info set output = "%s"' % (arg)
        c.execute(sql)
        conn.commit()
        conn.close()
        return True
    
class SpecialTask:
    IMPORT = ".import"
    TABLE = ".tables"
    OUTPUT = ".output"
    INDEX = ".indices"
    ROW_SEPARATOR = ".row_separator"
    COL_SEPARATOR = ".col_separator"
    SHOW = ".show"
    ANALYZE = ".analyze"

    def __init__(self):
        self.cmd = None
        self.type = None
        self.id = None
        self.worker_nodes = {}
        self.task_flag = None
        self.used_worker_num = 0
        self.succ_worker_num = 0
        self.succ_datanode_num = 0
        self.clients = []
        self.reply_sock = []
        self.has_udx = False
        self.database = None

class Job:
    
    READY   = "ready"
    FINISH  = "finish"
    FAIL    = "fail"
    RUNNING = "running"
    WAITING = "waiting"
    PENDING = "pending"
    
    def __init__(self):
        self.opid = None
        self.id = None
        self.db = {}  # node : db
        self.node = None
        self.name = None # the name of Operator
        self.status = Job.READY
        self.sql = None
        self.target_node = None
        self.target_db = []
        # data distributed to UDX operator comes from which block of which job        
        self.child_jobs = []
        self.is_checkpoint = False
        
class Task:

    def __init__(self):
        self.TASK_READY = 0
        self.TASK_RUNNING = 1
        self.PLAN_EXECUTE_END = 2
        self.DATA_PARALLEL_END = 3
        self.TASK_SUCCESS = 4
        self.TASK_FAIL = 5
        self.NOTIFY_ALL_CLIENT = 6

        self.id = None
        self.query = None
        self.input = None
        self.output = None
        self.alias = {}  #{col:alias}
        self.worker_nodes = {} # {workid: worker_node}
        self.data_nodes = {}  # {}
        self.clients = []
        self.plan = BasicOp()
        """
        if there is (not) exist predication, then the subquery is considerred as a
        pre_plan which should be first executed before the plan
        """
        self.pre_plan = None  
        self.status = self.TASK_READY
        self.database = None
        self.used_worker_num = 0
        self.succ_worker_num = 0
        self.succ_datanode_num = 0
        self.s_time = 0
        self.e_time = 0
        self.reply_sock = []
        self.has_udx = False  # used to decide whether to start data_paralleler or not
        self.output_dir = None
        self.table = None  # the only table this task is involved
        self.pre_table = None # used to temp store the table in outer query if there is a nested query
        self.tag = None   # to identify the query with "SELECT", "CREATE", "DROP", ...
        self.is_data_node_done = False
        self.create_table = "temporary"
        self.table_column = {} # store the columns for all tables
        self.table_alias = {}  # {alias:table}
        self.column_alias = {} # {column:alias}
        self.tmp_table = {}
        self.data_replica = {} # {opid:{localnode:replicanode}}
        self.failed_node = []
        self.datanode = []     # underlying data nodes

    def show(self):
        ws(self.id+'\t'+self.query+'\t'+self.database + '\n')

class WorkerNode:
    def __init__(self):
        self.id = None   # the port number randomly dispatched by task_manager
        self.cqid = None # the id of collective query
        self.opid = 0    # the id of the operator in the logical plan
        self.name = None 
        self.port = 0    # actually is the same with id
        self.local_addr = None 
        self.status = None
        self.score = 1
        self.s_time = 0 # the time of this client starting a task
        self.speed = 1  # the initial speed of the client

class DataNode:
    def __init__(self):
        self.id = None   # the port randomly dispatched by task_manager
        self.name = None
        self.port = 0    
        self.t_size = 0 # total size of data
        self.l_size = 0 # size of left data
        self.b_size = 0 # block size that send to worker at once
        self.job_t_size = {} # total size of data for each job
        self.job_l_size = {} # size of left data for each job
        self.cur_job = None  # current job whose data is to be distributed
        self.cur_bk = 0   # current block to be distributed for current job
        self.has_client = False
        self.clients = []
        self.ECT = 0

class ParaLiteException:
    def __init__(self):
        self.SELF_EXCEPTION = "ERROR: Expcetions in the master process"
        self.CHILD_EXCEPTION = "ERROR: Expcetions in the child process"
        self.QUERY_PARSE_EXCEPTION = "ERROR: Exceptions in parsing collective query"
        self.PLAN_MAKE_EXCEPTION = "ERROR: Exceptions in making execution plan"
        self.QUERY_SYNTAX_PARSE_EXCEPTION = "ERROR: Exceptions in parsing SQL syntaxly"
        self.USELESS_QUERY = "ERROR: All data is distributed to udx workers, so this query is actually useless and ParaLite ignores it."
        self.NO_UDX_DEF = "ERROR: You used User-Defined Executable, please define them first"
        self.PARAMETER_ERROR = "ERROR: Please specify right parameters"
        self.NON_EQUI_JOIN_ERROR = "ERROR: ParaLite cannot support this query with JOIN operations other than Equi-JOIN"

class DataLoader:
    def __init__(self, iom, userconfig):
        self.iom = iom
        self.dl_server = {}
        self.h_info = {}   #{node:hold_size, ...}
        self.n_info = []   #[node1, node2,...]
        self.n_client = {} #{node:unfished_client_num, ...}
        #{node:(port, local_socket), ...}
        #record the socket info for each node, set it after dloader server registration
        self.p_info = {}   
        self.client = {}   #{port:node, port:node, ...}
        self.tag = 0       # 0--loading source data; 1--loading intermediate data
        self.count_dbm = 0     # the number of finished workers
        self.count_req = 0     # the number of finished clients
        self.fashion = None
        self.key = None       
        self.cqid = None
        self.table = None
        self.process = {}
        self.database = None
        self.request = []
        self.client_addrs = []
        self.row_sep = None # the row separator of data, NULL -- if it is \n or None
        self.userconfig = userconfig
    """
    # if all clients are finished (all data on clients are dispatched), send END
    _TAG to all workers
    # if all workers are finished (all db are changed), set the dloader instance 
    None.
    """
    def proc_req_msg(self, node, size, addr):
        sep = conf.SEP_IN_MSG
        sock = socket(AF_INET, SOCK_STREAM)
        sock.connect(addr)
        self.client_addrs.append(addr)
        if size == 0:
            msg = conf.DLOAD_REPLY
            sock.send("%10s%s" % (len(msg), msg))
            return 
        if self.fashion == conf.ROUND_ROBIN_FASHION:
            nodes = []
            s = size/len(self.n_info) + 1
            for n in self.p_info:
                m = conf.SEP_IN_MSG.join(
                    [n, self.p_info[n], str(s), str(len(self.n_info))])
                nodes.append(m)
        elif self.fashion == conf.REPLICATE_FASHION:
            nodes = []
            for n in self.p_info:
                m = "%s%s%s" % (n, conf.SEP_IN_MSG,self.p_info[n])
                nodes.append(m)
        elif self.fashion == conf.HASH_FASHION:
            nodes = []
            for n in self.p_info:
                m = "%s%s%s" % (n, conf.SEP_IN_MSG,self.p_info[n])
                nodes.append(m)
        elif self.fashion == conf.RANGE_FASHION:
            raise(Exception("RANGE_FASHION is not supportted now..."))
        self.n_client[node] -= 1
        rs = []
        
        # add node info (socket addr)
        rs.append(",".join(nodes))
        
        # add db info on each node
        sub_dbs = InfoCenter().get_table_db_name(metadata_db, self.table)
        rs.append(",".join(sub_dbs))
        
        # add chunk number
        chunk_num = InfoCenter().get_chunk_num(metadata_db)
        rs.append(str(chunk_num))
        
         # send the replica infomation for each db to the client
        replica_info = InfoCenter().get_replica_info(metadata_db)
        if replica_info is not None:
            rs.append(",".join(replica_info))
        else:
            rs.append("")
            
        #rs.append(str(client_num))
        #client_num += 1
        msg = sep.join([conf.DLOAD_REPLY, "#".join(rs)])
        send_bytes(sock, "%10s%s" % (len(msg), msg))
        sock.close()

    def proc_dbm_msg(self, node, se_db):
        self.count_dbm += 1
        self.update_metadata(se_db)

    def update_metadata(self, se_db):
        b = base64.decodestring(se_db)
        dbs = cPickle.loads(b)
        InfoCenter().update_db_info(dbs)

    def schedule_data(self, node, size):
        """
        Schedule Policy of multiple clients: 
        (1) When receive data (size = s1) from a client c1, paraLite assumes 
            that all of other unfinished clients have the same size of data with c1. 
        (2) Under the assumption, calculate the total size of data:
            Total_size = s1 * num_unfinished_clients + real_size_of_finished_clients
        (3) calculate the average size for each node:
            AVG_size = Total_size / num_of_datanode
        (4) find sutible datanodes:
            4-1: if local node is a datanode, AVG_size - hold_size --> local
            4-2: nodes B who don't have client on,  AVG_size - hold_size --> B
            4-3: nodes C whose clients are all finished, AVG_size - hold_size --> C
            4-4: other nodes D has least data, AVG_size - hold_size --> D
        (5) update the info of clients and hold size of each datanode
        """
        sche_res = []
        sche_info = {}
        # calculate the number of unfinished clients
        num_unfi_c = 0
        for n in self.n_client:
            num_unfi_c += self.n_client[n]
        # the total real size of finished clients
        real_fi_size = 0
        for n in self.h_info:
            real_fi_size += self.h_info[n]
        # assumed total size
        ass_total_size = size * num_unfi_c + real_fi_size
        # assumed average size
        ass_avg_size = ass_total_size / len(self.n_info) + 1
        sche_size = 0
        while sche_size < size:
            tar_node = None
            # check local node
            if node in self.h_info and self.h_info[node] < ass_avg_size:
                if ass_avg_size - self.h_info[node] <= size:
                    sche_info[node] = ass_avg_size - self.h_info[node]
                else:
                    sche_info[node] = size
                tar_node = node
                sche_size += sche_info[node]
            elif self.get_node_without_client(ass_avg_size)[0] is not None and self.get_node_without_client(ass_avg_size)[1] != 0:
                n, s = self.get_node_without_client(ass_avg_size)
                if n is not None and s != 0:
                    if s <= size:
                        sche_info[n] = s
                    else:
                        sche_info[n] = size
                    tar_node = n
                    sche_size += sche_info[n]
            else:
                n, s = self.get_node_least_data(ass_avg_size)
                if n is not None and s != 0:
                    if s <= size:
                        sche_info[n] = s
                    else:
                        sche_info[n] = size
                    tar_node = n
                    sche_size += sche_info[n]
            if tar_node is not None:
                self.h_info[tar_node] += sche_info[tar_node]
        sep = conf.SEP_IN_MSG
        for sche_node in sche_info:
            m = "%s%s%s%s%s" % (sche_node, sep, self.p_info[sche_node], sep,
                                sche_info[sche_node])
            sche_res.append(m)
        return sche_res

    def get_node_without_client(self, avg_size):
        min_size = avg_size
        res_n = None
        for n in self.n_info:
            if self.n_client[n] == 0:
                if n in self.h_info and self.h_info[n] < min_size:
                    res_n = n
                    min_size = self.h_info[n]
        """
        for n in self.n_client:
            if self.n_client[n] == 0:
                if n in self.h_info and self.h_info[n] < min_size:
                    res_n = n
                    min_size = self.h_info[n]
        """
        return res_n, avg_size - min_size

    def get_node_least_data(self, avg_size):
        min_size = avg_size
        res_n = None
        for n in self.n_info:
            if self.n_client[n] != 0:
                if self.h_info[n] < min_size:
                    res_n = n
                    min_size = self.h_info[n]
        """
        for n in self.n_client:
            if self.n_client[n] != 0:
                if self.h_info[n] < min_size:
                    res_n = n
                    min_size = self.h_info[n]
        """
        return res_n, avg_size - min_size

    def is_worker_finished(self):
        if self.count_dbm == len(self.n_info):
            return 1
        return 0

    def is_client_finished(self):
        if self.count_req == len(self.client):
            return 1
        return 0

    def all_server_started(self):
        if len(self.p_info) == len(self.n_info):
            return 1
        return 0
    
    def notify_dload_server(self):
        for n in self.p_info.keys():
            addr = (n, string.atoi(self.p_info[n].split(conf.SEP_IN_MSG)[0]))
            sock = socket(AF_INET, SOCK_STREAM)
            sock.connect(addr)
            sock.send(conf.END_TAG)
            sock.close()

    def notify_dload_client(self):
        for addr in self.client_addrs:
            sock = socket(AF_INET, SOCK_STREAM)
            sock.connect(addr)
            sock.send("%10s%s" % (len(conf.DLOAD_END_TAG), conf.DLOAD_END_TAG))
            sock.close()

    def get_available_port(self, node):
        """get available port on the node

        Usually, the port is set by the system itself, however, in some cases, there
        are only a set of ports are available and specified by user. 
        """
        userconf = self.userconfig
        if userconf.port_range != []:
            if w not in userconf.node_port:
                userconf.node_port[w] = []
                for p in userconf.port_range:
                    userconf.node_port[w].append(p)
            port = userconf.node_port[w].pop(0)
        else:
            port = 0
        return port

    def start_all_server(self):
        path = WORKING_DIR
        name = '%s%s.py' % (path, "dload_server")
        dic = {}
        dic[conf.CQID] = self.cqid
        dic[conf.MASTER_NAME] = master_node
        dic[conf.MASTER_PORT] = master_port
        dic[conf.OUTPUT_DIR] = InfoCenter().get_output(self.database)
        dic[conf.TABLE] = self.table
        for node in self.n_info:
            # give the initial db info to workers
            rs = InfoCenter().get_db_info(self.database, node, 1)
            db_name = rs.split(":")[0]
            db_size = string.atoi(rs.split(":")[1])
            dic[conf.DATABASE] = db_name
            dic[conf.DB_SIZE] = db_size
            dic[conf.PORT] = self.get_available_port(node)
            dic[conf.TEMP_DIR] = self.userconfig.temp_dir
            dic[conf.LOG_DIR] = self.userconfig.log_dir
            s = cPickle.dumps(dic)
            b = base64.encodestring(s)
            s_in_a_line = string.replace(b, '\n', '*')
            cmd = "gxpc e -h %s python %s %s" % (node, name, s_in_a_line)
            #cmd = "ssh %s \"%s\"" % (node, cmd)
            pipes = [(None, "w", 0), (None, "r", 1), (None, "r", 2)]
            pid, r_chans, w_chans, err = self.iom.create_process(
                cmd.split(), None, None, pipes, None)
            r_chans[0].flag = conf.PROCESS_STDOUT
            r_chans[1].flag = conf.PROCESS_STDERR
            for ch in r_chans:
                ch.buf = cStringIO.StringIO()

            ParaLiteLog.debug("gxpc e -h %s python %s" % (node, name))
            proc = ParaLiteProcess(
                self.cqid, pid, "dloader", None, node, r_chans, w_chans, err)
            proc.status = 2
            self.process[pid] = proc
        ParaLiteLog.info("dloaders are started")
        
    def start(self):
        self.start_all_server()
        # construct n_client
        for n in self.n_info:
            self.n_client[n] = 0
            self.h_info[n] = 0
        for client in self.client:
            node = self.client[client]
            if node not in self.n_client:
                self.n_client[node] = 0
            self.n_client[node] += 1
        
class ParaLiteMaster():
    """python ParaLiteMaster.py client_node client_port database master_port confinfo
    """

    CMD_EN_ERROR = -2
    CMD_RET_SUC = -1
    
    def __init__(self):
        self.iom = ioman.mk_ioman()
        self.is_running = True
        self.userconfig = UserConfig()
        self.init_user_config()
        self.task_manager = TaskManager(self.iom, self.userconfig)
        self.dloader = None
        self.data_parallel = None
        self.output = None
        self.eventqueue = Queue.Queue()
        self.my_exception = None
        
    def init_user_config(self):
        s = string.replace(sys.argv[5], '*', '\n')
        b1 = base64.decodestring(s)
        dic = cPickle.loads(b1)
        for key in dic.keys():
            value = dic[key]
            if hasattr(self.userconfig, key):
                setattr(self.userconfig, key, value)

    def next_event(self, t):
        while True:
            ev = self.iom.next_event(None)
            # non read channels are simple.
            if not isinstance(ev, ioman_base.event_read):
                return ev
            # an event from a read channel.
            # we may receive a part of a message, in which case
            # we should not return
            ch = ev.ch
            buf = ch.buf
            if ev.data is not None:
                buf.write(ev.data)

            # an I/O error or EOF. we return anyway
            if ev.eof:
                data_to_return = buf.getvalue()
                return ioman_base.event_read(ch, data_to_return, 1, ev.err)
            elif ev.ch.flag == conf.SOCKET_OUT:
                # the first 10 bytes is the length of the mssage
                if ch.length == 0:
                    ch.length = string.atoi(ev.data[0:10])
                if len(buf.getvalue()) >= ch.length + 10:
                    all_data = buf.getvalue()
                    data_to_return = all_data[0:ch.length+10]
                    buf.truncate(0)
                    buf.write(all_data[ch.length+10:])
                    ch.length = 0
                    return ioman_base.event_read(ch, data_to_return, 0, ev.err)
            # else:
            #     return ioman_base.event_read(ch, ev.data, 0, ev.err)

    def start(self):
        global master_port, log
        try:
            ch = self.iom.create_server_socket(AF_INET, SOCK_STREAM, 1000,
                                               ("", master_port))
        except Exception, e:
            if e.args[0] == 98:
                es(str(e.args[0]))
            return
        if not os.path.exists(self.userconfig.temp_dir):
            os.makedirs(self.userconfig.temp_dir)
        cur_time = time.strftime('%Y-%m-%d-%H-%M-%S',time.localtime(time.time()))
        ParaLiteLog.init("%s/master-%s-%s.log" % (self.userconfig.log_dir,
                                                  gethostname(),cur_time),
                         logging.DEBUG)
        addr = ch.ss.getsockname()
        ParaLiteLog.info("ParaLite is listening on port %s ..." % (repr(addr)))
        """
        Sending output and port back to the client:
        output: to specify where to store the output data----stdout or file
        port  : if client did not specify the port number, the master gets an usable port
        """
        if master_port == 0:
            p = addr[1]
            master_port = p
            msg = "%s%s%s" % (conf.INFO, conf.SEP_IN_MSG, p)
        else:
            msg = "%s%s%s" % (conf.INFO, conf.SEP_IN_MSG, conf.MASTER_READY)
        sock = socket(AF_INET, SOCK_STREAM)
        try:
            sock.connect((public_node, public_port))
        except:
            ParaLiteLog.debug(traceback.format_exc())
        sock.send("%10s%s" % (len(msg), msg))
        sock.close()
        ParaLiteLog.info("sending master ready info or port back to the client")
        """
        now, it starts to wait for any event and process them
        """
        
        # handler = Thread(target=self.event_recv)
        # handler.setDaemon(True)
        # handler.start()

        # self.handle_read()
        # ParaLiteLog.info("The main thread is finished...")
        # handler.join()
        # ParaLiteLog.info("The child thread is finished...")
        
        self.event_recv()

    def event_recv(self):
        while self.is_running:
            try:
                ev = self.next_event(None)
            except Exception, e:
                es("ERROR in next_event : %s\n" % (" ".join(str(s) for s in e.args)))
            if isinstance(ev, ioman_base.event_accept):
                try:
                    self.handle_accept(ev)
                except Exception, e:
                    es("ERROR in handle_accept: %s\n" % (e.args[1]))
                    return
            elif isinstance(ev, ioman_base.event_read):
                if ev.data != "":
                    if ev.ch.flag == conf.PROCESS_STDOUT or ev.ch.flag == conf.PROCESS_STDERR:
                        self.handle_read_from_process(ev)
                    elif ev.ch.flag == conf.SOCKET_OUT:
                        self.handle_read_from_socket(ev)

                    #self.eventqueue.put(ev)
                    if ev.data[10:] == conf.EXIT:
                        break
            elif isinstance(ev, ioman_base.event_death):
                try:
                    self.handle_death(ev)
                except Exception, e:
                    es("ERROR in handle_death: %s\n" % traceback.format_exc())
                    return
        

    def handle_read(self):
        while self.is_running:
            ParaLiteLog.debug("3333333333333")
            try:
                event = self.eventqueue.get()
                ParaLiteLog.debug("222222222222")
                flag = event.ch.flag
                if flag == conf.PROCESS_STDOUT or flag == conf.PROCESS_STDERR:
                    ParaLiteLog.debug("11111111")
                    self.handle_read_from_process(event)
                elif flag == conf.SOCKET_OUT:
                    self.handle_read_from_socket(event)

                # handle exceptions
                if self.my_exception is not None:
                    self.is_running = False
                    self.handle_exception(self.my_exception)
            except Exception, e:
                ParaLiteLog.debug("777777777777")
                ParaLiteLog.debug(traceback.format_exc())
                self.my_exception = (
                    ParaLiteException().SELF_EXCEPTION,
                    "ERROR in handle_read: %s\n" % (traceback.format_exc()))
                self.is_running = False
                self.handle_exception(self.my_exception)

        ParaLiteLog.debug("444444444444")
        # notify event_recv by sending a message
        sock = socket(AF_INET, SOCK_STREAM)
        sock.connect((master_node, master_port))
        msg = "exit"
        sock.send("%10s%s" % (len(msg),msg))
        sock.close()
        
    def handle_death(self, event):
        pid = event.pid
        status = event.status
        err = event.err
        """
        No FAULT-TOLERANCE Support in this version
        """
        return
    
        if pid in self.task_manager.process:
            proc = self.task_manager.process.pop(pid)
            if proc.cqid not in self.task_manager.taskqueue:
                return
            t = self.task_manager.taskqueue[proc.cqid]            
            if status != 0:
                proc.status = 1
                # If a process exits abnormally, fault recovery mechanism is started
                try:
                    if self.is_dead(proc.node):
                        if proc.node not in t.failed_node:
                            self.recovery_from_node_failure(proc)
                            t.failed_node.append(proc.node)
                    else:
                        self.recovery_from_process_failure(proc)
                except:
                    ParaLiteLog.debug("ERROR: in fault recovery: %s" % traceback.format_exc())
                    raise Exception(traceback.format_exc())
            else:
                op = self.get_op_by_id(t.plan, proc.opid)
                if op.status != conf.FINISH and op.status != conf.PENDING:
                    proc.status = 1
                    try:
                        if self.is_dead(proc.node):
                            if proc.node not in t.failed_node:
                                self.recovery_from_node_failure(proc)
                                t.failed_node.append(proc.node)
                        else:
                            self.recovery_from_process_failure(proc)
                    except:
                        ParaLiteLog.debug("ERROR: in fault recovery: %s" % traceback.format_exc())
                        raise Exception(traceback.format_exc())
                else:
                    proc.status = 0

    def is_dead(self, node):
        return True
        
    def recovery_from_node_failure(self, proc):
        ParaLiteLog.debug(
            "**ERROR: Node %s is DEAD**" % (proc.node))
        ParaLiteLog.debug(
            "**Operator opid=%s name=%s node=%s**" % (
                proc.opid, proc.tag, proc.node))

        ParaLiteLog.debug("**********RECOVERY FROM NODE FAILURE IS STARTED********")
        # delete all addresses of the failed node for all operators
        for opid in self.task_manager.operator_addrs:
            addrs = self.task_manager.operator_addrs[opid]
            for addr in addrs:
                if addr[0] == proc.node:
                    self.task_manager.operator_addrs[opid].remove(addr)
                    if addr in self.task_manager.operator_idle[opid]:
                        self.task_manager.operator_idle[opid].remove(addr)
                    break
                
        # find the first operator whose status is not READY
        task = self.task_manager.taskqueue[proc.cqid]
        ops_to_start = []
        q = deque([])
        q.append(task.plan)
        while list(q) != []:
            op = q.popleft()

            if op.status == conf.READY:
                for child in op.children:
                    q.append(child)
                # send NODE_FAIL to op
                msg = conf.SEP_IN_MSG.join([proc.node, ""])
                addrs = self.task_manager.operator_addrs[op.id]
                self.send_msg_to_worker(
                        conf.NODE_FAIL,
                        [(addr[0],addr[1]) for addr in addrs],
                        msg)
                ParaLiteLog.debug("**MESSAGE**: %s Operator %s --> %s" % (
                    conf.NODE_FAIL, op.id, [addr[0] for addr in addrs]))
                
            elif op.status == conf.RUNNING:
                # reset the status of jobs in the failed node
                for job in self.task_manager.operator_jobqueue[op.id]:
                    if job.target_node == proc.node:
                        job.status = conf.READY
                        job.target_node = None
                for child in op.children:
                        q.append(child)
                # send NODE_FAIL to op
                msg = conf.SEP_IN_MSG.join([proc.node, ""])
                addrs = self.task_manager.operator_addrs[op.id]
                self.send_msg_to_worker(
                        conf.NODE_FAIL,
                        [(addr[0],addr[1]) for addr in addrs],
                        msg)
                ParaLiteLog.debug("**MESSAGE**: %s Operator %s --> %s" % (
                    conf.NODE_FAIL, op.id, [addr[0] for addr in addrs]))
                
            elif op.status == conf.PENDING:
                op.is_fail = True
                if op.is_checkpoint is not None and op.is_checkpoint:
                    # notify the node who has data on the failed node
                    replica_node = task.data_replica[op.id][proc.node]
                    msg = conf.SEP_IN_MSG.join([proc.node, replica_node])
                    addrs = self.task_manager.operator_addrs[op.id]
                    self.send_msg_to_worker(
                        conf.NODE_FAIL,
                        [(addr[0],addr[1]) for addr in addrs],
                        msg)
                    ParaLiteLog.debug("**MESSAGE**: %s Operator %s --> %s" % (
                        conf.NODE_FAIL, op.id, [addr[0] for addr in addrs]))
                else:
                    # reset the status of jobs in the failed node
                    for job in self.task_manager.operator_jobqueue[op.id]:
                        if job.target_node == proc.node:
                            job.status = conf.READY
                            job.target_node = None
                            op.status = conf.RUNNING
                    
                    # schedule sql job
                    if op.name == Operator.SQL:
                        t_addr = []
                        while len(self.task_manager.operator_idle[op.id]) > 0:
                            idle_node = self.task_manager.operator_idle[op.id].pop(0)
                            t_job = self.task_manager.dispatch_job(
                                op.id, op.name, idle_node[0])
                            if t_job is None:
                                t_addr.append(idle_node)
                                continue
                            self.send_msg_to_worker(
                                conf.JOB, [(idle_node[0], idle_node[1])], t_job)
                            t_job.status = conf.RUNNING
                            ParaLiteLog.debug(
                                "**MESSAGE**: %s Operator %s Job %s DB %s--> %s" % (
                                    conf.JOB, op.id, t_job.id, t_job.target_db, idle_node[0]))
                            t_job.target_node = idle_node[0]
                        self.task_manager.operator_idle[op.id] = t_addr
                    
                    for child in op.children:
                        q.append(child)
                    
                # send NODE_FAIL to op
                msg = conf.SEP_IN_MSG.join([proc.node, ""])
                addrs = self.task_manager.operator_addrs[op.id]
                self.send_msg_to_worker(
                        conf.NODE_FAIL,
                        [(addr[0],addr[1]) for addr in addrs],
                        msg)
                ParaLiteLog.debug("**MESSAGE**: %s Operator %s --> %s" % (
                    conf.NODE_FAIL, op.id, [addr[0] for addr in addrs]))
                
            elif op.status == conf.FINISH:
                op.is_fail = True
                ops_to_start.append(op.status)
                if not op.is_checkpoint:
                    for child in op.children:
                        q.append(child)

        for op in ops_to_start:
            op.status = conf.FAIL
            op.is_fail = True
            if op.name == Operator.UDX:
                self.task_manager.start_operator(op, task.clients)
            else:
                addrs = self.task_manager.operator_addrs[op.id]
                self.task_manager.start_operator(
                    op, ["%s:%s" % (addr[0], addr[1]) for addr in addrs])
            self.task_manager.operator_addrs[op.id] = []
            
        ParaLiteLog.debug("**********RECOVERY FROM NODE FAILURE IS FINISHED********")
        
    def recovery_from_process_failure(self, proc):
        ParaLiteLog.debug(
            "**ERROR: Operator %s %s exits on %s abnormally" % (
                proc.opid, proc.tag, proc.node))
        
        ParaLiteLog.debug("**********RECOVERY FROM PROCESS FAILURE IS STARTED********")
        
        task = self.task_manager.taskqueue[proc.cqid]
        fail_op = self.get_op_by_id(task.plan, proc.opid)
        ParaLiteLog.debug("the status of failed op is: %s" % fail_op.status)

        if fail_op.status == conf.FAIL:
            return
        # delete the related addr in operator_addrs
        addrs = self.task_manager.operator_addrs[proc.opid]
        node = None
        for addr in addrs:
            if addr[0] == proc.node:
                node = "%s:%s" % (addr[0], addr[1])
                addrs.remove(addr)
                break
        assert node is not None

        # kill all children processes, at most two level

        if fail_op.status == conf.READY:
            ParaLiteLog.debug(
                "The failed proc does not have data, so we only restart it!")
            fail_op.status = conf.FAIL

            # start the failed process with the same port
            ParaLiteLog.debug("***restart the failed process: start***")
            self.task_manager.start_operator(fail_op, [node])
            ParaLiteLog.debug("***restart the failed process: finish***")
            
        elif fail_op.status == conf.PENDING:
            fail_op.status = conf.FAIL            
            if fail_op.is_checkpoint is not None and fail_op.is_checkpoint:
                ParaLiteLog.debug(
                    "The failed proc is checkpointed, so we only restart it!")

                # start the failed process with the same port
                ParaLiteLog.debug("***restart the failed process: start***")
                self.task_manager.start_operator(fail_op, [node])
                ParaLiteLog.debug("***restart the failed process: finish***")
                
            else:
                self.set_failed_job_status(proc, False)

                # start the failed process with the same port
                ParaLiteLog.debug("***restart the failed process: start***")
                self.task_manager.start_operator(fail_op, [node])
                ParaLiteLog.debug("***restart the failed process: finish***")

                self.restart_related_operators(fail_op)
        elif fail_op.status == conf.RUNNING:
            fail_op.status = conf.FAIL            
            if fail_op.is_checkpoint is not None and fail_op.is_checkpoint:
                self.set_failed_job_status(proc, True)
            else:
                self.set_failed_job_status(proc, False)

            # start the failed process with the same port
            ParaLiteLog.debug("***restart the failed process: start***")
            self.task_manager.start_operator(fail_op, [node])
            ParaLiteLog.debug("***restart the failed process: finish***")

            self.restart_related_operators(fail_op)
        ParaLiteLog.debug("********RECOVERY FROM PROCESS FAILURE IS FINISHED*********")
        
    def set_failed_job_status(self, proc, is_ck):
        ParaLiteLog.debug("***reset the status of jobs on failed process: start***")
        for job in self.task_manager.operator_jobqueue[proc.opid]:
            if job.target_node == proc.node:
                if is_ck:
                    if job.status == conf.RUNNING or job.status == conf.WAITING:
                        ParaLiteLog.debug("**reset job %s: READY" % job.id)
                        job.status = conf.READY
                else:
                    ParaLiteLog.debug("**reset job %s: READY" % job.id)
                    job.status = conf.READY
        ParaLiteLog.debug("***reset the status of jobs on failed process: finish***")

    def restart_related_operators(self, fail_op):
        # get all operators need to be restarted and re-set the status
        # of all operators to be restarted.
        ParaLiteLog.debug("***restart the necessary child process: start***")
        
        # get all operators that need to be restarted
        op_start = self.get_operator_to_restart(fail_op)
        if op_start == []:
            ParaLiteLog.debug("**NO** other operator needs to be restrated!!")
            return
        ParaLiteLog.debug("*****Operators required to be restarted: %s*****" % " ".join(oopp.id for oopp in op_start))
        
        # re-start the operator using the old port
        # nodes = InfoCenter().get_data_node_info(metadata_db, op.tables[0])
        # **only** for process failure, not considering the node failure
        for op in op_start:
            op.status = conf.FAIL
            op.is_fail = True
            if op.name == Operator.UDX:
                self.task_manager.start_operator(op, task.clients)
            else:
                t_addr = []
                for addr in self.task_manager.operator_addrs[op.id]:
                    t_addr.append("%s:%s" % (addr[0], addr[1]))
                self.task_manager.start_operator(op, t_addr)
            self.task_manager.operator_addrs[op.id] = []
        ParaLiteLog.debug("***restart the necessary child process: finish***")
        
    def handle_accept(self, event):
        event.new_ch.flag = conf.SOCKET_OUT
        event.new_ch.length = 0
        event.new_ch.buf = cStringIO.StringIO()

    def handle_read_from_process(self, event):
        #if event.data.startswith("ERROR"):
        ParaLiteLog.error("PROCESS: %s" % event.data)
        es("%s\n" % event.data)
        #self.safe_kill_master()
        self.is_running = False
        
    def handle_read_from_socket(self, event):
        """
        (1) REG for data node: 
            REG:DATA_NODE:cqid:dnid:nodename:datasize
        (2) REG for worker node:
            REG:WORKER_NODE:id:nodename:port:state
        (3) ACK for worker node:
            ACK:WORKER_NODE:id:nodename:datasize:state:costtime
        (4) ACK for data node:
            ACK:DATA_NODE:cqid:state
        (5) KAL for worker node:
            KAL:cqid:id:status
        (6) REG:CLIENT:collective_query:nodename
        (7) REQ:TABLE:SIZE
        (8) ERROR: info
        """
        message = event.data[10:]
        try:
            if message == conf.KILL:
                ParaLiteLog.info("MESSAGE: %s" % (message))
                self.safe_kill_master()
                self.is_running = False
                return
            elif message == conf.DETECT_INFO:
                return
            elif message == conf.EXIT:
                self.is_running = False
                return
            elif message.startswith("ERROR"):
                ParaLiteLog.info(message)
                self.my_exception = (ParaLiteException().CHILD_EXCEPTION, message)
                return
            mt = message.split(conf.SEP_IN_MSG)[0]
            if mt == conf.REG:
                t = message.split(conf.SEP_IN_MSG)[1]
                if t == conf.DATA_NODE:
                    ParaLiteLog.info(message)
                    self.reg_new_datanode(message)
                elif t == conf.CLIENT:
                    self.reg_new_client(message)
                elif t == conf.DLOADER_SERVER:
                    ParaLiteLog.info(message) 
                    self.reg_new_dloader_server(message)
                    
            elif mt == conf.ACK:
                ParaLiteLog.info(message)
                t = message.split(conf.SEP_IN_MSG)[1]
                if t == conf.JOB:
                    if self.new_process_ack_datanode(message) == 1:
                        ParaLiteLog.info("----This query is finished----")
                        self.is_running = False
                if t == conf.WORKER_NODE:
                    if self.process_ack_client(message) == 1:
                        ParaLiteLog.info("----This query is finished----")
                        self.is_running = False
                        
            elif mt == conf.PENDING:
                ParaLiteLog.info(message)
                self.process_pending_job(message)
                    
            elif mt == conf.KAL:
                ParaLiteLog.info(message)
                self.process_kal_client(message)
                
            elif mt == conf.RS:
                #ParaLiteLog.info(message)
                self.process_rs_info_datanode(message)
                
            elif mt == conf.REQ:
                ParaLiteLog.info(message)
                m = message.split(conf.SEP_IN_MSG)
                #REQ:END_TAG:NODE:CLIENT_ID
                if len(m) == 4 and m[1] == conf.END_TAG:
                    self.dloader.count_req += 1
                    if self.dloader.is_client_finished():
                        self.dloader.notify_dload_server()
                        ParaLiteLog.info("notify all dload server")
                    return

                #REQ:cqid:NODE:PORT:DATABASE:TABLE:DATA_SIZE:TAG:FASHION:ROW_SEP:client_id
                cqid, node, port, database, table, data_size, tag = m[1:8]
                port = string.atoi(port)
                node_info = InfoCenter().get_data_node_info(database, table)
                if len(node_info) == 0:
                    err_info = "ERROR: %s may not exists" % table
                    self.my_exception = (ParaLiteException().SELF_EXCEPTION, err_info)
                    return

                if self.dloader is None:
                    self.dloader = DataLoader(self.iom, self.userconfig)
                    self.dloader.tag = tag
                    self.dloader.fashion = m[8]
                    self.dloader.n_info = node_info
                    self.dloader.table = table
                    self.dloader.cqid = cqid
                    self.dloader.database = database
                    self.dloader.row_sep = m[8] #"NULL" or Other
                    if tag == conf.LOAD_FROM_API:
                        task = self.task_manager.taskqueue[cqid]
                        for addr in self.task_manager.operator_addrs[task.plan.id]:
                            self.dloader.client[addr[1]] = addr[0]
                        #self.dloader.client = task.plan.node
                    else:
                        self.dloader.client[0] = node
                    self.dloader.start()
                if self.dloader.all_server_started():
                    self.process_load_request(node, data_size, (node, port))
                else:
                    self.dloader.request.append((node, data_size, (node, port)))
            elif mt == conf.DBM:
                ParaLiteLog.debug(message)
                if self.process_db_modified(message) == 1:
                    self.is_running = False
            elif mt == conf.QUE:
                self.process_db_query(message)
            else:
                ws('we can not support this registeration\n')
                return
        except Exception, e:
            raise(Exception(traceback.format_exc()))

    def report_error_to_clients(self, message):
        q = self.task_manager.taskqueue
        if len(q) == 0:
            # this is a special task
            addr = (public_node, public_port)
            sock = socket(AF_INET,SOCK_STREAM)
            sock.connect(addr)
            sock.send("%10s%s" % (len(message), message))
            sock.close()
        else:
            task = q[q.keys()[0]]
            for addr in task.reply_sock:
                sock = socket(AF_INET,SOCK_STREAM)
                sock.connect(addr)
                sock.send("%10s%s" % (len(message), message))
                sock.close()

    def handle_exception(self, ex):
        ParaLiteLog.debug("start to handle exception...")
        self.safe_kill_master()

        # send the error info to the client
        self.report_error_to_clients(ex[1])

        
    def reg_new_dloader_server(self, message):
        #REG:DLOADER_SERVER:cqid:node:port:local_socket
        assert self.dloader is not None

        m = message.split(conf.SEP_IN_MSG)
        self.dloader.p_info[m[3]] = "%s%s%s" % (m[4], conf.SEP_IN_MSG, m[5])
        if self.dloader.all_server_started():
            while self.dloader.request != []:
                re = self.dloader.request.pop()
                self.process_load_request(re[0], re[1], re[2])
            
    # def process_data_distribute_request(self, message):
    #     #REQ:DATA_NODE:cqid:dnid:nodename:port:datasize
    #     m = message.split(conf.SEP_IN_MSG)
    #     cqid = m[2]
    #     task = self.task_manager.get_task_from_queue(cqid)
    #     assert task is not None
    #     dn = DataNode()
    #     dn.id = m[3]
    #     dn.name = m[4]
    #     dn.port = string.atoi(string.strip(m[5]))
    #     dn.t_size = string.atoi(m[6])
    #     dn.l_size = dn.t_size
    #     for key in task.worker_nodes:
    #         worker = task.worker_nodes[key]
    #         if worker.name == dn.name:
    #             dn.clients.append(worker)
    #     if len(dn.clients) != 0:
    #         dn.ECT = float(dn.l_size) / len(dn.clients)
    #         dn.has_client = True
    #     else:
    #         dn.ECT = sys.maxint

    #     worker_num = len(task.clients)
    #     """
    #     TODO: use more sophisticated method to decide block size
    #     """
    #     block_size = self.userconfig.block_size
    #     if block_size == 0:
    #         dn.b_size = dn.t_size/len(task.clients) + 1
    #     elif block_size == -1:
    #         dn.b_size = dn.t_size
    #     else:
    #         dn.b_size = min(block_size, dn.t_size)
        
    #     task.data_nodes[dn.id] = dn
    #     ParaLiteLog.info("DATA_NODE NUM: *******%s*******" % (len(task.data_nodes)))
    #     self.data_parallel.distribute_data(dn)

    def send_msg_to_worker(self, msg_type, addrs, content):
        for addr in addrs:
            sock = socket(AF_INET, SOCK_STREAM)
            sep = conf.SEP_IN_MSG
            try:
                sock.connect(addr)
                if msg_type == conf.JOB:
                    msg = sep.join([msg_type, content.id,
                                    " ".join(content.target_db),
                                    str(content.is_checkpoint)])
                else:
                    if content == "":
                        msg = msg_type
                    else:
                        msg = sep.join([msg_type, content])
                sock.send("%10s%s" % (len(msg), msg))
                sock.close()
                
            except Exception, e:
                if e.errno == 4:
                    sock = socket(AF_INET, SOCK_STREAM)
                    sock.connect(addr)
                    if msg_type == conf.JOB:
                        msg = sep.join([msg_type, content.id, " ".join(content.target_db)])
                    else:
                        msg = "%s%s%s" % (msg_type, sep, content)
                    sock.send("%10s%s" % (len(msg), msg))
                    sock.close()
                elif e.errno == 111:
                    pass
                else:
                    raise(Exception(e))
        
    def safe_kill_master(self):
        ParaLiteLog.debug("Kill the master safely ...")
        # check all threads terminated or not
        # for p in self.task_manager.process:
        #     proc = self.task_manager.process[p]
        #     ParaLiteLog.info("ERROR: %s is still running on %s" % (proc.tag, proc.node))
        #     cmd = "killall %s" % proc.tag
        #     os.system("gxpc e -h %s %s" % (proc.node, cmd))
        for tid in self.task_manager.taskqueue:
            task = self.task_manager.taskqueue[tid]
            plan = task.plan
            if plan is None:
                return
            q = deque([])
            q.append(plan)
            while list(q) != []:
                op = q.popleft()
                if op.status != conf.FINISH:
                    if op.id in self.task_manager.operator_addrs:
                        for addr in self.task_manager.operator_addrs[op.id]:
                            sock = socket(AF_INET, SOCK_STREAM)
                            try:
                                sock.connect((addr[0], addr[1]))
                                sock.send("%10s%s" % (len(conf.EXIT), conf.EXIT))
                                sock.close()
                            except:
                                pass
                for child in op.children:
                    q.append(child)
                    
        if self.dloader is not None:
            for p in self.dloader.process:
                proc = self.dloader.process[p]
                ParaLiteLog.info("ERROR: %s is still running on %s" % (proc.tag, proc.node))
                #os.system("gxpc e killall %s" % (proc.tag))
        
        # kill the master
        # (1) send a message=exit to the child thread
        # (2) child thread put a event=exit to the event queue
        # (3) child thread exits
        # (4) main thread exits
        sock = socket(AF_INET, SOCK_STREAM)
        sock.connect((master_node, master_port))
        sock.send("%10s%s" % (len(conf.EXIT), conf.EXIT))
        ParaLiteLog.info("sending EXIT to %s" % str((master_node,master_port)))
        sock.close()
        
        #kill all related process
        path = WORKING_DIR
        os.system("%sclear.sh" % path)
        ParaLiteLog.debug("executing clear.sh")

    def add_new_client(self, t, so, c):
        t.reply_sock.append(so)
        t.clients.append(c)
        # get the UDX node in the logical tree
        queue = deque([])
        queue.append(t.plan)
        while list(queue) != []:
            op = queue.popleft()
            if op.name == Operator.UDX:
                udx_op = op
                break
            for child in op.children:
                queue.append(child)
        assert udx_op is not None

        # make a job
        jobqueue = self.task_manager.operator_jobqueue[op.id]
        job = Job()
        job.opid = op.id
        job.node = c
        job.name = op.name
        job.id = str(len(jobqueue))
        jobqueue.append(job)

        # get necessary args and start the UDX process
        op.node[op.worker_num] = c
        opid = "%s_%s" % (op.id, op.worker_num)
        op.worker_num += 1
        self.task_manager.start_operator(op, [c])
        
    def reg_new_client(self, message):
        #REG:CLIENT:database:collective_query:nodename:port
        self.check_metadata_table(metadata_db)
        reply_so = (message.split(conf.SEP_IN_MSG)[4],
                    string.atoi(message.split(conf.SEP_IN_MSG)[5]))
        
        t, clientname = self.parse_query_args(message)
        if t:
            if self.output is None:
                self.output = InfoCenter().get_output(metadata_db)
            (row_sep, col_sep) = InfoCenter().get_separator(metadata_db)
            msg = "%s%s%s%s%s%s%s" % (conf.DB_INFO, conf.SEP_IN_MSG, self.output,conf.SEP_IN_MSG, row_sep, conf.SEP_IN_MSG, col_sep)
            try:
                sock = socket(AF_INET, SOCK_STREAM)
                sock.connect(reply_so)
                sock.send("%10s%s" % (len(msg), msg))
                sock.close()
            except Exception, e:
                if e.errno == 4:
                    sock = socket(AF_INET, SOCK_STREAM)
                    sock.connect(reply_so)
                    sock.send("%10s%s" % (len(msg), msg))
                    sock.close()
            old_task = self.task_manager.get_task_from_queue(t.id)
            if old_task:
                # check the current status of the task
                if old_task.status >= old_task.DATA_PARALLEL_END:
                    ex = exception().USELESS_QUERY
                    sock = socket(AF_INET, SOCK_STREAM)
                    sock.connect(reply_so)
                    sock.send("%10s%s" % (len(ex), ex))
                    sock.send("%10sOK" % (2))
                    sock.close()
                    ParaLiteLog.info("this query is useless")
                else:
                    self.add_new_client(old_task, reply_so, clientname)
            else:
                ParaLiteLog.info("this is a new task, we added it to the queue")
                self.task_manager.put_task_to_queue(t, clientname, reply_so)
                if isinstance(t, Task):
                    self.data_parallel = DataParalleler(t)
                    err_type, err_info = self.task_manager.execute()
                    if err_type != -1:
                        ParaLiteLog.info(err_info)
                        self.my_exception = (
                            ParaLiteException().SELF_EXCEPTION, err_info)
                        if err_type == 1:
                            self.report_error_to_clients("ERROR: %s" % err_info)
                        elif err_type == 0:
                            self.report_error_to_clients("OK")
                else:
                    err_type, err_info = self.task_manager.execute()
                    if err_type != -1:
                        self.my_exception = (
                            ParaLiteException().SELF_EXCEPTION, err_info)
                        self.report_error_to_clients("ERROR: %s" % err_info)
                    else:
                        # this is a special task
                        self.report_error_to_clients("OK")
                    self.is_running = False
                    ParaLiteLog.info("TASK: %s --> %s: FINISH" % (t.id, t.cmd))
        else:
            ex = ParaLiteException().QUERY_PARSE_EXCEPTION
            self.my_exception = (ParaLiteException().SELF_EXCEPTION, ex)

    def reg_new_datanode(self, message):
        #REG:DATANODE:cqid:opid:nodename:port:localaddr[:workerid]
        m = message.split(conf.SEP_IN_MSG)
        cqid, opid, nodename, port, localaddr = m[2:7]        
        if len(m) == 8:
            workerid = m[7]
        port = string.atoi(port)

        # find the operator
        task = self.task_manager.taskqueue[cqid]
        op = self.get_op_by_id(task.plan, opid)
        ParaLiteLog.debug("op status : %s" % op.status)
        
        # set the addr for the current job
        op_addrs = self.task_manager.operator_addrs[opid]
        sep = conf.SEP_IN_MSG
        op_addrs.append((nodename, port, localaddr))
        # set all addrs as idle
        self.task_manager.operator_idle[op.id].append((nodename, port, localaddr))

        # send the argument back to the job
        args = self.task_manager.mk_job_args(task, op)
        self.send_msg_to_worker(conf.JOB_ARGUMENT, [(nodename, port)], args)
        ParaLiteLog.debug("**MESSAGE**: %s --> op %s on %s" % (
            conf.JOB_ARGUMENT, op.id, nodename))

        # if the current op is UDX, register as a worker node
        if op.name == Operator.UDX:
            if op.status != conf.FAIL:
                self.reg_new_workernode_for_udx(task, cqid, opid, workerid, nodename,
                                                port, localaddr)
            # we cannot know the number of clients in advance, so we will start
            # its all children jobs once we received a registration
            if op.status is None:          
                num = 1                    
            elif op.status == conf.READY:  
                num = sys.maxint           

        elif op.name in [Operator.ORDER_BY, Operator.AGGREGATE, Operator.DISTINCT]:
            num = 1
        else:
            t = op.tables[0]
            if t in task.table_alias:
                t = task.table_alias[t]
            num = len(InfoCenter().get_data_node_info(metadata_db, t))
        num = num - len(task.failed_node)
        
        # if all parent jobs are registed, start all children jobs        
        if len(op_addrs) >= num:
            if op.status != conf.FAIL:
                for child in op.children:
                    if child.status == conf.FINISH:
                        continue
                    for addr in op_addrs:
                        child.p_node[addr[0]] = (addr[1],addr[2])
                    self.task_manager.new_schedule_operator(child)
            else:
                for child in op.children:
                    if child.name == Operator.SQL and len(self.task_manager.operator_addrs[child.id]) == len(self.task_manager.operator_jobqueue[child.id]):
                        for job in self.task_manager.operator_jobqueue[child.id]:
                            ParaLiteLog.debug("operator %s Job %s status %s" % (child.id, job.id, child.status))
                            
                            if job.status == conf.READY:
                                if self.task_manager.operator_idle[child.id] == []:
                                    break
                                addr = self.task_manager.operator_idle[child.id].pop(0)
                                job.target_node = addr[0]
                                job.status = conf.RUNNING
                                self.send_msg_to_worker(
                                    conf.JOB, [(addr[0], addr[1])], job)
                                ParaLiteLog.debug("**MESSAGE**: %s Operator %s Job %s --> %s" % (conf.JOB, child.id, job.id, addr[0]))
                                
                            child.status = conf.RUNNING     
            # if it is SQL operator, send jobs to all data nodes
            if op.name == Operator.SQL:
                core_num = 1
                i = 0 
                while i < core_num:
                    for addr in op_addrs:
                        cur_node, cur_port = addr[0:2]
                        t_job = self.task_manager.dispatch_job(opid, op.name, cur_node)
                        if t_job is not None:
                            t_job.is_checkpoint = op.is_checkpoint_est
                            self.send_msg_to_worker(conf.JOB,
                                                    [(cur_node, cur_port)], t_job)
                            self.task_manager.operator_idle[opid].remove(addr)

                            t_job.status = Job.RUNNING
                            ParaLiteLog.debug("**MESSAGE**: %s Operator %s Job %s --> %s" % (conf.JOB, opid, t_job.id, cur_node))
                    i += 1
                # for addr in op_addrs:
                #     cur_node, cur_port = addr[0:2]
                #     t_job = self.task_manager.dispatch_job(opid, op.name, cur_node)
                #     if t_job is not None:
                #         t_job.is_checkpoint = op.is_checkpoint_est
                #         self.send_msg_to_worker(conf.JOB,
                #                                 [(cur_node, cur_port)], t_job)
                #         t_job.status = Job.RUNNING
                #         ParaLiteLog.debug("**MESSAGE**: %s Operator %s Job %s --> %s" % (conf.JOB, opid, t_job.id, cur_node))
                op.status = conf.RUNNING
            else:
                op.status = conf.READY
            
    def get_op_type(self, data_size_of_child):
        """check the type of current operator based on the result data of its children
        """
        if data_size_of_child > 2*1024*1024*1024:
            return Job.DATA_PERSIST
        return Job.PIPELINE_DATA_WITHOUT_PERSIST
            
    def reg_new_workernode_for_udx(self, task, cqid, opid, wid, node, port, local_addr):
        wnode = WorkerNode()
        wnode.id = wid
        wnode.cqid = cqid
        wnode.opid = opid
        wnode.name = node
        wnode.status = conf.READY
        wnode.port = port
        wnode.local_addr = local_addr
        task.worker_nodes[wnode.id] = wnode
        ParaLiteLog.info("WORKER_NODE NUM: ----%s-----" % (len(task.worker_nodes)))
        # sometimes a workernode is late, when it issue the query to the master, there
        # are some data left. So the master starts a process for this udx. However,
        # sometimes the registration of this worker node happens after all data is
        # distributed. In this case, this worker node cannot receive the notifciation
        # from the master and keep running for ever.
        if task.is_data_node_done == True:
            addr = (wnode.name, wnode.port)
            try:
                ParaLiteLog.info("notify all udx client-----%s" % (wnode.id))
                so = socket(AF_INET, SOCK_STREAM)
                so.connect(addr)
                so.send('%10sEND' % (3))
                so.close()
            except Exception, e:
                if e.errno == 4:
                    ParaLiteLog.info("notify all client-----%s" % (wnode.id))
                    so = socket(AF_INET, SOCK_STREAM)
                    so.connect(addr)
                    so.send('%10sEND' % (3))
                    so.close()
        # We don't allow a client join during the computation now!
        # # TODO: if a client join in during the computation, the data_node
        # # should be updated.
        # if self.data_parallel.all_data_node_done():
        #     task.status = task.DATA_PARALLEL_END
        # else:
        #     self.data_parallel.distribute_data(wnode)

    def process_db_query(self, message):
        #QUE:node:port:database:table
        node,port,database,table = message.split(conf.SEP_IN_MSG)[1:]
        port = string.atoi(port)

        columns = InfoCenter().get_table_columns(database,table)
        s1 = " ".join(columns)

        key = InfoCenter().get_table_partition_key(database, table)
        if key == []:
            s2 = "NULL"
        else:
            s2 = ",".join(key)

        sep = InfoCenter().get_separator(database)
        s3 = sep[0]
        s4 = sep[1]

        msg = conf.SEP_IN_MSG.join([conf.METADATA_INFO, s1, s2, s3, s4])
        sock = socket(AF_INET, SOCK_STREAM)
        sock.connect((node, port))
        sock.send("%10s%s" % (len(msg), msg))
        sock.close()

    def process_kal_client(self, message):
        #KAL:cqid:opid:id:status:speed:time
        m = message.split(conf.SEP_IN_MSG)
        opid = m[2]
        cid = m[1]
        wid = m[3]
        status = m[4]
        speed = float(m[5])
        time = float(m[6])
        task = self.task_manager.taskqueue[cid]
        worker = task.worker_nodes[wid]
        worker.status = status
        worker.speed = speed
        """
        if self.data_parallel.all_data_node_done():
            task.status = task.DATA_PARALLEL_END
            # send END to this worker
            addr = (worker.name, worker.port)
            so = socket(AF_INET, SOCK_STREAM)
            so.connect(addr)
            so.send('%10sEND' % (3))
            so.close()
            ParaLiteLog.info("END --> WORKER %s" % (worker.id))
        else:
            self.data_parallel.distribute_data(worker)
        """
        if not self.data_parallel.all_data_node_done():
            result = self.data_parallel.distribute_data(worker)
            if result != {}:
                dn = task.data_nodes[result[worker.id][0]]
                if dn.l_size == 0:
                    self.send_msg_to_worker(conf.END_TAG, [(dn.name, dn.port)], "")
                    ParaLiteLog.debug("send ENG_TAG --> %s:%s" % (dn.name, dn.port))
                    
                for workerid in result:
                    self.task_manager.operator_jobqueue[opid][string.atoi(workerid)].child_jobs.append(result[workerid])

            
    def process_ack_client(self, message):
        #ACK:WORKER_NODE:cqid:id:nodename:state:costtime
        m = message.split(conf.SEP_IN_MSG)
        cqid = m[2]
        task = self.task_manager.taskqueue[cqid]
        task.succ_worker_num += 1
        ParaLiteLog.info("WORKER_NODE ACK NUM: --------%s-------" % (task.succ_worker_num))
        if task.succ_worker_num >= task.used_worker_num:
            for addr in task.reply_sock:
                try:
                    ParaLiteLog.info("CLIENT: %s" % (repr(addr)))
                    sock = socket(AF_INET, SOCK_STREAM)
                    sock.connect(addr)
                    sock.send("%10sOK" % (2))
                    sock.close()
                except Exception, e:
                    if e.errno == 4:
                        ParaLiteLog.info("CLIENT: %s" % (repr(addr)))
                        sock = socket(AF_INET, SOCK_STREAM)
                        sock.connect(addr)
                        sock.send("%10sOK" % (2))
                        sock.close()
            task.e_time = time.time()*1000
            costtime = task.e_time-task.s_time
            ParaLiteLog.info("task is finished")
            ParaLiteLog.info('------task %s cost %d ms------' % (task.id, costtime))
            self.task_manager.remove_task_from_queue(cqid)
            return 1
        return 0
    
    def process_load_request(self, node, data_size, addr):
        try:
            self.dloader.proc_req_msg(node, string.atoi(data_size), addr)
        except Exception, e:
            raise e
        
    def process_db_modified(self, message):
        #DBM:NODE:serialized_db_instance
        m = message.split(conf.SEP_IN_MSG)
        node = m[1]
        se_db = m[2]
        self.dloader.proc_dbm_msg(node, se_db)
        if self.dloader.is_worker_finished():
            self.dloader.notify_dload_client()
            ParaLiteLog.info('-------------import data succussfully-----------')
            tag = self.dloader.tag
            self.dloader = None
            ParaLiteLog.debug(tag)
            if tag == conf.LOAD_FROM_CMD:
                return 1
        return 0

    def new_process_ack_datanode(self, message):
        # ACK:DATA_NODE:cqid:opid:jobid_list:hostname:port
        # set job status
        cqid, opid, jobid_list, hostname, port = message.split(conf.SEP_IN_MSG)[2:]
        jobid_list = jobid_list.split()
        port = string.atoi(port)
        task = self.task_manager.taskqueue[cqid]
        jobqueue = self.task_manager.operator_jobqueue[opid]

        if jobid_list == []:
            return 0

        # FAULT TOLERANCE:
        cur_op = self.get_op_by_id(task.plan, opid)
        if task.failed_node != []:
            if cur_op.is_checkpoint is not None and cur_op.is_checkpoint:
                for job in jobqueue:
                    if job.target_node in task.failed_node:
                        job.status = conf.FINISH
                        
        for jobid in jobid_list:
            job = jobqueue[string.atoi(jobid)]
            job.status = Job.FINISH
        
        if job.name == Operator.SQL and (job.sql.startswith("create") or job.sql.startswith("drop")):
            cur_op = self.get_op_by_id(task.plan, opid)
            cur_op.status = conf.FINISH
            if self.task_manager.is_task_finish(cqid):
                if task.tag == "DROP":
                    InfoCenter().delete_table_info(task.database, task.table)

                self.notify_ok_to_clients(task.reply_sock)
                self.task_manager.remove_task_from_queue(cqid)
                return 1
            return 0
        
        # check if all jobs for the operator are finished
        if self.task_manager.is_job_finish(opid):
            # set the status of the op 
            cur_op.status = conf.FINISH
            ParaLiteLog.debug("set operator %s status : %s" % (cur_op.id, conf.FINISH))
            
            # notify all udx client if all jobs are finished
            par_op = self.get_parent_op_by_id(task.plan, opid)
            if par_op is not None and par_op.name == Operator.UDX:
                # notify all clients to close socket server
                task.is_data_node_done = True
                self.notify_all_clients(task)
                ParaLiteLog.debug("notify all clients")
                task.status = task.DATA_PARALLEL_END
                return 0
            
            # check if the task is finished
            if self.task_manager.is_task_finish(cqid):
                self.notify_ok_to_clients(task.reply_sock)
                self.task_manager.remove_task_from_queue(cqid)
                task.e_time = time.time()
                costtime = task.e_time-task.s_time
                ParaLiteLog.info(task.plan.get())
                ParaLiteLog.info("------Task %s cost %s seconds" % (task.id, costtime))
                return 1
        else:
            return 0
        
    def process_pending_job(self, message):
        # PENDING:JOB:cqid:opid:jobid:hostname:port
        # set job status
        sep = conf.SEP_IN_MSG
        cqid, opid, jobid, hostname, port = message.split(sep)[2:]
        port = string.atoi(port)
        task = self.task_manager.taskqueue[cqid]
        op = self.get_op_by_id(task.plan, opid)

        cur_addr = None
        for addr in self.task_manager.operator_addrs[opid]:
            if addr[0] == hostname:
                self.task_manager.operator_idle[opid].append(addr)
                cur_addr = addr
                break
        if cur_addr is None:
            # this is a pending message from a failed node
            return
        
        jobqueue = self.task_manager.operator_jobqueue[opid]
        job = jobqueue[string.atoi(jobid)]
        if job.status == Job.FINISH:
            # sometimes a job is executed by several processes due to the skew problem
            return 0
        job.status = conf.PENDING

        # check if all jobs for the operator are scheduled or not
        if self.task_manager.is_job_pending(opid):
            op.status = conf.PENDING
            ParaLiteLog.debug("set Operator %s status : %s" % (op.id, conf.PENDING))
            # notify all related processes that all jobs are scheduled
            addrs = self.task_manager.operator_addrs[opid]
            new_addrs = []
            for addr in addrs:
                new_addrs.append((addr[0], addr[1]))
            self.send_msg_to_worker(conf.JOB_END, new_addrs, "")
            ParaLiteLog.debug("**MESSAGE**: %s --> %s %s" % (conf.JOB_END, opid, new_addrs))
            if job.name not in [Operator.AGGREGATE, Operator.ORDER_BY, Operator.UDX]:
                # find the op and the addrs of its children
                op = self.get_op_by_id(task.plan, opid)
                child_addrs = []
                for child in op.children:
                    for addr in self.task_manager.operator_addrs[child.id]:
                        child_addrs.append((addr[0], addr[1]))
                self.send_msg_to_worker(conf.DATA_END, child_addrs, "")
                ParaLiteLog.debug(
                    "**MESSAGE**: %s  --> children of %s" % (conf.DATA_END, opid))

        else:
            # check if all children have already get data
            for child in op.children:
                # eliminate the situation that the child is a sql op to create table 
                if child.name == Operator.SQL and child.expression.lower().startswith("create"):
                    continue
                if child.status != conf.PENDING:
                    return

            t_job = self.task_manager.dispatch_job(opid, job.name, hostname)
            if t_job is not None:
                t_job.is_checkpoint = op.is_checkpoint_est
                t_job.status = Job.RUNNING
                t_job.target_node = hostname
                if t_job.name == Operator.SQL:
                    self.send_msg_to_worker(conf.JOB, [(hostname, port)], t_job)
                    if cur_addr in self.task_manager.operator_idle[opid]:
                        self.task_manager.operator_idle[opid].remove(cur_addr)
                    if opid == "0":
                        ParaLiteLog.debug("operator_idle pop %s" % (str(cur_addr)))

                    ParaLiteLog.debug(
                        "**MESSAGE**: %s Operator %s Job %s --> %s" % (
                            conf.JOB, opid, t_job.id, hostname))

                else:
                    # find the op and the addrs of its children
                    child_addrs = []
                    for child in op.children:
                        for addr in self.task_manager.operator_addrs[child.id]:
                            child_addrs.append((addr[0], addr[1]))
                    self.send_msg_to_worker(conf.JOB, [(hostname, port)], t_job)
                    if cur_addr in self.task_manager.operator_idle[opid]:
                        self.task_manager.operator_idle[opid].remove(cur_addr)
                    ParaLiteLog.debug(
                        "**MESSAGE**: %s Operator %s Job %s --> %s" % (
                            conf.JOB, opid, t_job.id, hostname))
                    
                    msg = sep.join([t_job.id, hostname])
                    self.send_msg_to_worker(conf.DATA_DISTRIBUTE, child_addrs, msg)
                    ParaLiteLog.debug("**MESSAGE**: %s : child of op %s --> %s --> %s" % (
                        conf.DATA_DISTRIBUTE, opid, t_job.id, hostname))

    def get_operator_to_restart(self, fail_op):
        """ get the first operator 1_op that needs to be restarted and reset the status
        for all operators till the last checkpointing
        
        @param opid   the id of the failed operator
        """
        # re_op = []

        # q = deque([])
        # for child in op.children:
        #     if child.status == conf.RUNNING:
        #         if child.is_checkpoint is not None and child.is_checkpoint:
        #             continue
        #         else: q.append(child)
                    
        #     elif child.status == conf.FINISH:
        #         if child.is_checkpoint is not None and child.is_checkpoint == conf.CHECKPOINT:
        #             re_op.append(child)
        #         else:
        #             q.append(child)
        #             re_op.append(child)
                    
        #     elif child.status == conf.READY:
        #         re_op.append(child)
        #         q.append(child)

        # while list(q) != []:
        #     sop = q.popleft()
        #     for child in sop.children:
        #         if child.is_checkpoint is not None and child.is_checkpoint == conf.CHECKPOINT:
        #             flag = 0
        #             for oopp in re_op:
        #                 if child in oopp.children:
        #                     flag = 1
        #                     break
        #             if flag == 1:
        #                 continue
        #             re_op.append(child)
        #             continue
        #         if child.status == conf.FINISH:
        #             child.status = conf.READY
        #             q.append(child)
        #             flag = 0
        #             for oopp in re_op:
        #                 if child in oopp.children:
        #                     flag = 1
        #                     break
        #             if flag == 1:
        #                 continue
        #             re_op.append(child)
        # return re_op
        re_op = []
        q = deque([])
        for child in fail_op.children:
            if fail_op.name == Operator.JOIN or fail_op.name == Operator.GROUP_BY:
                for job in self.task_manager.operator_jobqueue[child.id]:
                    if job.status != conf.RUNNING:
                        job.status = conf.READY
                        job.target_node = None
            else:
                # TODO: set for UDX
                continue
            q.append(child)
        while list(q) != []:
            op = q.popleft()
            if op.status == conf.FINISH:
                re_op.append(op)
            if op.is_checkpoint is not None and op.is_checkpoint:
                continue
            if op.status == conf.PENDING and op.data_size != -1:
                op.is_fail = True
                op.status = conf.READY
            for child in op.children:
                if op.name == Operator.JOIN or op.name == Operator.GROUP_BY:
                    for job in self.task_manager.operator_jobqueue[child.id]:
                        if job.status != conf.RUNNING:
                            job.status = conf.READY
                            job.target_node = None
                q.append(child)
        return re_op
    
    def get_parent_op_by_id(self, plan, opid):
        par_op = None
        q = deque([])
        q.append(plan)
        while list(q) != []:
            op = q.popleft()
            for child in op.children:
                if child.id == opid:
                    par_op = op
                    break
                q.append(child)
        return par_op

    def get_op_by_id(self, plan, opid):
        q = deque([])
        q.append(plan)
        while list(q) != []:
            op = q.popleft()
            if op.id == opid:
                break
            for child in op.children:
                q.append(child)
        return op
    
    def process_rs_info_datanode(self, message):
        # RS:DATANODE:cqid:opid:hostname:port:rs_type:partition_num:
        # total_size:total_time[:job_result_data_size]
        tm = self.task_manager
        
        sep = conf.SEP_IN_MSG
        cqid, opid, hostname, port, rs_type, part_num, total_size, total_time = message.split(sep)[2:10]
        port = string.atoi(port)
        part_num = string.atoi(part_num)
        total_size = string.atoi(total_size)
        total_time = float(total_time)
        
        # Find the operator
        task = tm.taskqueue[cqid]
        q = deque([])
        q.append(task.plan)
        par_op = None
        while list(q) != []:
            op = q.popleft()
            if op.id == opid:
                break
            for child in op.children:
                if child.id == opid:
                    par_op = op
                q.append(child)

        # FAULT TOLERANCE:
        # if op.is_fail:
        #     # some operators are not re-started, then their status can only be PENDING
        #     if op.is_first_job:
        #         if par_op.name != Operator.UDX:
        #             # schedule jobs of parent op to all nodes
        #             self.start_parent_op(par_op, op)
        #             op.is_first_job = False
        #             return
        
        # for UDX, register the data node again
        if par_op is not None and par_op.name == Operator.UDX:
            job_size = cPickle.loads(base64.decodestring(message.split(sep)[-1]))
            self.reg_new_datanode_for_udx(
                task, opid, hostname, port, total_size, job_size)

        flag = 0
        if op.data_size == -1:
            flag = 1
            op.data_size = total_size * len(tm.operator_addrs[op.id])
            ParaLiteLog.debug("=== %s execution time: %s  %s" % (op.id, total_time, op.data_size))
            op.execution_time = math.ceil(float(len(tm.operator_jobqueue[op.id])) / len(task.datanode)) * total_time
            if len(op.children) == 0:
                op.ect = op.execution_time
            else:
                max_ect = 0
                for child in op.children:
                    if not child.is_checkpoint and child.ect > max_ect:
                        max_ect = child.ect
                op.ect = max_ect + op.execution_time
                ParaLiteLog.debug("op: %s ect %s" % (op.name, op.ect))
            if par_op is None:
                return
            
            # # update the plan to set new checkpointing
            # flag = 0
            # for child in par_op.children:
            #     if child.data_size == -1:
            #         flag = 1
            #         break
            # #ParaLiteLog.debug(task.plan.get())
            # if flag == 0:
            #     self.task_manager.set_checkpoint(task)
            
        if flag == 1:
            if par_op.name != Operator.UDX:
                # schedule jobs of parent op to all nodes
                self.start_parent_op(par_op, op)
                par_op.status = conf.RUNNING


    def start_parent_op(self, par_op, op):
        child_addrs = []
        for addr in self.task_manager.operator_addrs[op.id]:
            child_addrs.append((addr[0], addr[1]))
            
        if len(par_op.children) == 1:
            del_addr = []
            for addr in self.task_manager.operator_idle[par_op.id]:
                cur_node, cur_port = addr[0:2]
                t_job = self.task_manager.dispatch_job(par_op.id,
                                                       par_op.name,
                                                       cur_node)
                
                if t_job is not None:
                    t_job.target_node = cur_node
                    if len(par_op.children) == 1:
                        t_job.status = Job.RUNNING
                    else:
                        t_job.status = Job.WAITING
                    self.send_msg_to_worker(
                        conf.JOB, [(cur_node, cur_port)], t_job)
                    del_addr.append(addr)
                    ParaLiteLog.debug("**MESSAGE**: %s Operator %s Job %s--> %s" % (
                        conf.JOB, par_op.id, t_job.id, cur_node))
                    
                    msg = conf.SEP_IN_MSG.join([t_job.id, cur_node])
                    self.send_msg_to_worker(conf.DATA_DISTRIBUTE, child_addrs, msg)
                    ParaLiteLog.debug(
                        "**MESSAGE**: %s Operator %s data %s --> Operator %s on %s" % (
                            conf.DATA_DISTRIBUTE, op.id, t_job.id, par_op.id, cur_node))
                    
            for addr in del_addr:
                self.task_manager.operator_idle[par_op.id].remove(addr)
                if par_op.id == "3":
                    ParaLiteLog.debug("operator_idle pop %s" % (str(addr)))

        else:
            for job in self.task_manager.operator_jobqueue[par_op.id]:
                if job.status == conf.READY:
                    if self.task_manager.operator_idle[par_op.id] == []:
                        continue
                    addr = self.task_manager.operator_idle[par_op.id].pop(0)
                    cur_node, cur_port = addr[0:2]
                    job.target_node = cur_node
                    job.status = Job.WAITING
                    self.send_msg_to_worker(
                        conf.JOB, [(cur_node, cur_port)], job)
                    ParaLiteLog.debug("**MESSAGE**: %s Operator %s Job %s --> %s" % (
                        conf.JOB, par_op.id, job.id, cur_node))

                    msg = conf.SEP_IN_MSG.join([job.id, cur_node])

                    # check if another child is pending (FAULT TOLERANCE)
                    ano_finish = False
                    ano_op = None
                    for ch in par_op.children:
                        if ch.id == op.id:
                            continue
                        if ch.status == conf.PENDING:
                            ano_finish = True
                            ano_op = ch
                            break
                        
                    if ano_finish:
                        par_op.finish_child.append(ano_op.id)
                        for ano_addr in self.task_manager.operator_addrs[ano_op.id]:
                            if (ano_addr[0], ano_addr[1]) not in child_addrs:
                                child_addrs.append((ano_addr[0], ano_addr[1]))

                    self.send_msg_to_worker(conf.DATA_DISTRIBUTE, child_addrs, msg)
                    ParaLiteLog.debug("**MESSAGE**: %s : %s --> %s" % (
                        conf.DATA_DISTRIBUTE, job.id, cur_node))
                    par_op.finish_child.append(op.id)
                    
                elif job.status == conf.WAITING:
                    if op.id in par_op.finish_child:
                        return
                    msg = conf.SEP_IN_MSG.join([job.id, job.target_node])
                    self.send_msg_to_worker(conf.DATA_DISTRIBUTE, child_addrs, msg)
                    job.status = Job.RUNNING
                    ParaLiteLog.debug("**MESSAGE**: %s : %s --> %s" % (
                        conf.DATA_DISTRIBUTE, job.id, job.target_node))
        par_op.status = conf.RUNNING            
                    
        #     # only send DATA DISTRIBUTE message back to the operator who has already
        #     # finished and sent the result information
        #     if waiting_job_num == 0:
        #         # this is the first time to schedule par_op
        #         for addr in self.task_manager.operator_idle[par_op.id]:
        #             cur_node, cur_port = addr[0:2]
        #             t_job = self.task_manager.dispatch_job(par_op.id,
        #                                                    par_op.name,
        #                                                    cur_node)
        #             if t_job is not None:
        #                 t_job.target_node = cur_node
        #                 t_job.status = Job.WAITING
        #                 self.send_msg_to_worker(
        #                     conf.JOB, [(cur_node, cur_port)], t_job)
                        
        #                 del_addr.append(addr)
                        
        #                 ParaLiteLog.debug("**MESSAGE**: %s --> %s" % (
        #                     conf.JOB, cur_node))

        #                 msg = conf.SEP_IN_MSG.join([t_job.id, cur_node])
        #                 self.send_msg_to_worker(conf.DATA_DISTRIBUTE, child_addrs, msg)
        #                 ParaLiteLog.debug("**MESSAGE**: %s : %s --> %s" % (
        #                     conf.DATA_DISTRIBUTE, t_job.id, cur_node))
            
        #     else:
        #         for job in self.task_manager.operator_jobqueue[par_op.id]:
        #             if job.status == Job.WAITING:
        #                 msg = conf.SEP_IN_MSG.join([job.id, job.target_node])
        #                 self.send_msg_to_worker(conf.DATA_DISTRIBUTE, child_addrs, msg)
        #                 job.status = Job.RUNNING
        #                 ParaLiteLog.debug("**MESSAGE**: %s : %s --> %s" % (
        #                     conf.DATA_DISTRIBUTE, job.id, job.target_node))

        # for addr in del_addr:
        #     self.task_manager.operator_idle[par_op.id].remove(addr)

        # child_addrs = []
        # for child in par_op.children:
        #     for addr in self.task_manager.operator_addrs[child.id]:
        #         child_addrs.append((addr[0], addr[1]))
        # for addr in self.task_manager.operator_addrs[par_op.id]:
        #     cur_node, cur_port = addr[0:2]
        #     t_job = self.task_manager.dispatch_job(par_op.id,
        #                                            par_op.name,
        #                                            cur_node)
        #     if t_job is not None:
        #         t_job.target_node = cur_node
        #         t_job.status = Job.RUNNING
        #         self.send_msg_to_worker(
        #             conf.JOB, [(cur_node, cur_port)], t_job)
        #         ParaLiteLog.debug("**MESSAGE**: %s --> %s" % (
        #             conf.JOB, cur_node))
        #         msg = conf.SEP_IN_MSG.join([t_job.id, cur_node])                    
        #         self.send_msg_to_worker(conf.DATA_DISTRIBUTE, child_addrs, msg)
        #         ParaLiteLog.debug("**MESSAGE**: %s : %s --> %s" % (
        #             conf.DATA_DISTRIBUTE, t_job.id, cur_node))


    def reg_new_datanode_for_udx(self, task, opid, node, port, t_size, job_size):
        ParaLiteLog.debug("REG_NEW_DATANODE: opid=%s node=%s port=%s size=%s job_size=%s" % (opid, node, port, t_size, str(job_size)))
        task.has_udx = True
        dn = DataNode()
        dn.id = len(task.data_nodes)
        dn.name = node
        dn.port = port
        dn.t_size = t_size
        dn.l_size = dn.t_size
        dn.job_t_size = job_size
        dn.job_l_size = job_size
        dn.cur_job = job_size.keys()[0]
        for key in task.worker_nodes:
            worker = task.worker_nodes[key]
            par_opid = worker.opid
            if worker.name == dn.name:
                dn.clients.append(worker)
        if len(dn.clients) != 0:
            dn.ECT = float(dn.l_size) / len(dn.clients)
            dn.has_client = True
        else:
            dn.ECT = sys.maxint

        worker_num = len(task.clients)

        # set block size
        block_size = self.userconfig.block_size
        if block_size == 0:
            dn.b_size = dn.t_size/len(task.clients) + 1
        elif block_size == -1:
            dn.b_size = dn.t_size
        else:
            dn.b_size = min(block_size, dn.t_size)
            
        task.data_nodes[dn.id] = dn
        ParaLiteLog.debug("DATA_NODE NUM: *******%s*******" % (len(task.data_nodes)))
        # sometime a datanode does not have any data, then we send a END_TAG to it
        result = self.data_parallel.distribute_data(dn)
        if dn.l_size == 0:
            self.send_msg_to_worker(conf.END_TAG, [(dn.name, dn.port)], "")
            ParaLiteLog.debug("send ENG_TAG --> %s:%s" % (dn.name, dn.port))
        if result != {}:
            jobqueue = self.task_manager.operator_jobqueue[par_opid]
            for workerid in result:
                jobqueue[string.atoi(workerid)].child_jobs.append(result[workerid])

    def process_ack_datanode(self, message):
        # ACK:JOB:cqid:hostname
        # tag = 0 : there is no data selected
        m = message.split(conf.SEP_IN_MSG)
        cid = m[2]
        task = self.task_manager.taskqueue[cid]
        task.succ_datanode_num += 1
        ParaLiteLog.info("DATA_NODE ACK NUM:-----%s------" % (task.succ_datanode_num))
        if task.plan.name == Operator.UDX:
            worker_num = task.plan.children[0].worker_num
        else:
            worker_num = task.plan.worker_num
        if task.succ_datanode_num == worker_num:
            if task.tag == "DROP":
                InfoCenter().delete_table_info(task.database, task.table)
            # notify all clients to close socket server
            task.is_data_node_done = True
            self.notify_all_clients(task)
            ParaLiteLog.info("notify all clients")
            task.status = task.DATA_PARALLEL_END
            
            """
            task.e_time = time.time()*1000
            costtime = task.e_time-task.s_time
            print '-------------task %s cost %d ms-----------' % (task.id, costtime)
            """
            if not task.has_udx:
                for addr in task.reply_sock:
                    sock = socket(AF_INET, SOCK_STREAM)
                    sock.connect(addr)
                    sock.send("%10s%s" % (2, "OK"))
                    sock.close()
                self.task_manager.remove_task_from_queue(cid)
                return 1
            else:
                return 0
        else:
            return 0

    def notify_ok_to_clients(self, addrs):
        for addr in addrs:
            sock = socket(AF_INET, SOCK_STREAM)
            try:
                sock.connect(addr)
                sock.send("%10s%s" % (2, "OK"))
                sock.close()
            except Exception, e:
                ParaLiteLog.debug(traceback.format_exc())
            
    def notify_all_clients(self, task):
        try:
            for key in task.worker_nodes:
                worker = task.worker_nodes[key]
                addr = (worker.name, worker.port)
                so = socket(AF_INET, SOCK_STREAM)
                try:
                    ParaLiteLog.info("notify all udx client-----%s" % (worker.id))
                    so.connect(addr)
                    so.send('%10s%s' % (len(conf.JOB_END), conf.JOB_END))
                    so.close()
                except Exception, e:
                    if e.errno == 4:
                        time.sleep(random.random())
                        ParaLiteLog.info("notify all client-----%s" % (worker.id))
                        so.connect(addr)
                        so.send('%10sEND' % (3))
                        so.close()
        except:
            traceback.print_exc()

    def parse_query_args(self, cmd):
        """
        cmd: REG:CLIENT:database:collective_query:nodename
        """
        try:
            m = cmd.split(conf.SEP_IN_MSG)
            cq = m[3]
            nodename = m[4]
            database = m[2]
            if cq.startswith("."):
                spec_task = SpecialTask()
                spec_task.database = database
                spec_task.cmd = cq
                ParaLiteLog.debug(cq)                
                if cq.startswith(".output"):
                    spec_task.type = spec_task.OUTPUT
                elif cq.startswith(".tables"):
                    spec_task.type = spec_task.TABLE
                elif cq.startswith(".indices"):
                    spec_task.type = spec_task.INDEX
                elif cq.startswith(".row_separator"):
                    spec_task.type = spec_task.ROW_SEPARATOR
                elif cq.startswith(".col_separator"):
                    spec_task.type = spec_task.COL_SEPARATOR
                elif cq.startswith(".show"):
                    spec_task.type = spec_task.SHOW
                elif cq.startswith(".analyze"):
                    spec_task.type = spec_task.ANALYZE
                else:
                    es("Error: ParaLite cannot support the query\n")
                spec_task.id = str(get_unique_num(1, 2000))
                return spec_task, nodename
            else:
                gtask = Task()
                m = cq.split("collective by")
                gtask.database = database
                gtask.query = m[0]
                if len(m) > 1:
                    gtask.id = string.strip(m[1])
                else: 
                    gtask.id = str(0-get_unique_num(1,1000))
                gtask.output_dir = InfoCenter().get_output(database)
                return gtask, nodename
        except Exception, e:
            traceback.print_exc()
            return None, None

    def do_analyze_cmd(self, database, query):
        t = Task()
        t.database = database
        t.query = query
        LogicalPlanMaker(t).make()
        
    def do_query_cmd(self, database, cq):
        if cq.lower().startswith("select"):
            c = client(database, cq, 1)
        else:
            c = client(database, cq, 0)
        c.proc()

    def check_metadata_table(self, database):
        InfoCenter().create_metadata_table(database)

    def quit(self):
        addr = (master_node, master_port)
        sock = socket(AF_INET, SOCK_STREAM)
        sock.connect(addr)
        msg = "quit"
        sock.send('%10s%s' % (len(msg), msg))
        sock.close()

def recv_bytes(so, n):
    A = []
    while n > 0:
        x = so.recv(n)
        if x == "": break
        A.append(x)
        n = n - len(x)
    return string.join(A, "")

def send_bytes(so, msg):
    totalsent = 0
    msglen = len(msg)
    while totalsent < msglen:
        try:
            sent = so.send(msg[totalsent:])
        except Exception, e:                                      
            if e.errno == 11:                                     
                # [Errno 11] Resource temporarily unavailable     
                continue                                          
        totalsent += sent
                                        
def run_cmd(cmd):
    os.system(cmd)

used_num = []

def get_unique_num(start, end):
    num = random.randint(start, end)
    while num in used_num:
        num = random.randint(start, end)
    used_num.append(num)
    return num

    
def test_dloader():
    dload = dloader(None)
    dload.n_info.append("0")
    dload.n_info.append("1")
    for i in range(3):
        node = str(i)
        dload.p_info[node] = node
        dload.client[random.randint(1, 1000)] = node
        dload.client[random.randint(1, 1000)] = node
    dload.tag = 1
    dload.fashion = conf.ROUND_ROBIN_FASHION
    dload.row_sep = "NULL"
    dload.start()
    print dload.proc_req_msg("0", 242232, None)
    print dload.proc_req_msg("2", 483232, None)
    print dload.proc_req_msg("1", 249232, None)
    print dload.proc_req_msg("1", 249232, None)
    print dload.proc_req_msg("2", 483232, None)
    print dload.proc_req_msg("0", 242232, None)

def test():
    plan = OrderbyOp()
    plan.id = 1

    ch1 = GroupbyOp()
    ch1.id = 2
    plan.children = [ch1]
    
    ch1_ch1 = JoinOp()
    ch1_ch1.id = 3
    ch1.children = [ch1_ch1]
    
    ch1_ch1_ch1 = BasicOp()
    ch1_ch1_ch1.name = Operator.SQL
    ch1_ch1_ch1.expression = "select * from x"
    ch1_ch1_ch1.tables = ["LineItem"]
    ch1_ch1_ch1.id = 4

    ch1_ch1_ch2 = JoinOp()
    ch1_ch1_ch2.id = 5
    
    ch1_ch1.children = [ch1_ch1_ch1, ch1_ch1_ch2]
    
    ch1_ch1_ch2_ch1 = BasicOp()
    ch1_ch1_ch2_ch1.name = Operator.SQL
    ch1_ch1_ch2_ch1.expression = "select * from x"
    ch1_ch1_ch2_ch1.tables = ["Customer"]
    ch1_ch1_ch2_ch1.id = 6

    ch1_ch1_ch2_ch2 = BasicOp()
    ch1_ch1_ch2_ch2.name = Operator.SQL
    ch1_ch1_ch2_ch2.expression = "select * from x"
    ch1_ch1_ch2_ch2.tables = ["Orders"]
    ch1_ch1_ch2_ch2.id = 7
    
    ch1_ch1_ch2.children = [ch1_ch1_ch2_ch1, ch1_ch1_ch2_ch2]
    print plan.get()

    global metadata_db
    metadata_db = "/home/ting/tmp/tpch/tpch.db"
    ins = TaskManager(None, None)
    #ins.set_checkpoint_op(plan, {}, 4)
    print plan.get()

    
    reop = ins.get_operator_to_restart(plan, plan)
    for oopp in reop:
         print "%s---%s" % (oopp.id, oopp.name)

def test2():
    # using Interlevead Declustering to decide the replica node
    database = "test.db"
    
    nodes = []
    for i in range(6):
        nodes.append("chenting%s" % str(i))
    chunk_num = 5

    new_db = []
    i = 0
    for n in nodes:
        j = 0
        while j < chunk_num:
            # db_name = name_hostname_partitionNum_chunkNum
            db_name = "%s_%s_%s_%s" % (database, n, str(i), str(j))
            new_db.append(db_name)
            j += 1
        i += 1

    print "nodes = %s" % nodes
    print "chunk_num = %s" % chunk_num
    print "dbs = %s" % new_db
    
    node_num = len(nodes)
    # make sure the last sub cluster has at least two nodes
    sub_nodes_num = min(chunk_num + 1, node_num)
    if node_num % sub_nodes_num == 1:
        if sub_nodes_num > 2:
            sub_nodes_num -= 1
        else:
            sub_nodes_num += 1
            
    count = 1
    count1 = 1
    for db in new_db:
        db_node = db.split("_")[-3]
        pos = nodes.index(db_node)
        if pos >= node_num / sub_nodes_num * sub_nodes_num:
            temp_num = node_num % sub_nodes_num
            sub_nodes = nodes[-temp_num:]
        else:
            sub_nodes = nodes[pos/sub_nodes_num * sub_nodes_num : (pos/sub_nodes_num + 1) * sub_nodes_num]
            temp_num = sub_nodes_num

        #print "db = %s sub_nodes = %s" % (db, sub_nodes)
        pos_in_sub_nodes = sub_nodes.index(db_node)
        replica_pos = (pos_in_sub_nodes + count1) % temp_num
        #print replica_pos
        if replica_pos == pos_in_sub_nodes:
            replica_pos = (replica_pos + 1) % temp_num
            count1 += 1
        #print "db = %s replica_pos = %s" % (db, replica_pos)
        db_name = "%s_r_0" % (db)
        print "%s -- %s on %s" % (db, db_name, sub_nodes[replica_pos])
        if count == chunk_num:
            print "~~~~~~~~~"
            count = 1
            count1 = 0
        else:
            count += 1
            count1 += 1
        

def collect_info():
    """
    For Experiments: This function is to collect useful information from worker nodes.
    """
    ex_f = open("/home/ting/loadbalance.dat", "wb")
    nodes = []
    for i in range(21, 31):
        n = "cloko%s" % string.zfill(i, 3)
        nodes.append(n)
    
    i = 1
    interval = 3
    s_time = time.time()
    while time.time() - s_time < 120:
        result = []
        for node in nodes:
            addr = (node, 15000)
            sock = socket(AF_INET, SOCK_STREAM)
            t = 0
            try:
                ex_f.write("try to connect %s\n" % node)
                sock.connect(addr)
                ex_f.write("connect to %s\n" % node)                
                sock.send("REQ")
                ex_f.write("send REQ to %s\n" % node)
                t = sock.recv(100)
                ex_f.write("receive %s from %s\n " % (t, node))
                ex_f.flush()
                sock.close()
            except:
                pass
            result.append(float(t))
        total = 0
        for s in result:
            total += s
        if total == 0:
            newresult = [0 for re in result]
        else:
            newresult = [float(re) / total for re in result]
        ex_f.write("%s\t%s\n" % (i * interval, "\t".join(str(s) for s in newresult)))
        ex_f.flush()
        time.sleep(interval)
        i += 1
    ex_f.close()

def main():
    m_paraLite_ins = ParaLiteMaster()
    try:
        m_paraLite_ins.start()
        """
        if os.path.exists(m_paraLite_ins.userconfig.temp_dir):
            os.system("rm -rf %s" % m_paraLite_ins.userconfig.temp_dir)
        """
    except KeyboardInterrupt, e:
        if log is not None:
            ParaLiteLog.info("KeyboardInterrupt")
        m_paraLite_ins.safe_kill_master()
    except Exception, e:
        ParaLiteLog.debug(traceback.format_exc())
        es(traceback.format_exc())
        traceback.print_exc()
        m_paraLite_ins.safe_kill_master()

    
if __name__=="__main__":
    #test()
    #test2()
    main()
    
    


    
