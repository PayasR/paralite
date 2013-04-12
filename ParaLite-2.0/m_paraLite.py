import semantic, sys, string, random, config, os, string, time, traceback
import cPickle, base64, sqlite3, re, Queue, shlex, cStringIO
import ConfigParser, ioman, ioman_base, pwd
from socket import *
from collections import deque
from multiprocessing import Process
from threading import Thread
 
log = None

def Ws(s):
    sys.stdout.write(s)
    
def Es(s):
    sys.stderr.write(s)

def getCurrentTime():
    return time.strftime('%Y-%m-%d-%H-%M-%S',time.localtime(time.time()))

def logging(s):
    log.write("%s: %s\n" % (getCurrentTime(), s))
    log.flush()

"""
Usage:
       python m_paraLite.py client_node client_port database master_port confinfo
"""

client_num = 0
conf = config.config()
NODE_CONF = os.getcwd() + os.sep + "node.conf"
MAIN_CONF = os.getcwd() + os.sep + "paraLite.conf"
WORKING_DIR = sys.path[0] + "/"
master_port = 0
master_node = gethostname()

try:
    assert len(sys.argv) == 6
    public_node = sys.argv[1]
    public_port = string.atoi(sys.argv[2])
    metadata_db = sys.argv[3]
    master_port = string.atoi(sys.argv[4])
    
except Exception, e:
    Es("ERROR: %s", " ".join(str(s) for s in e.args))

def unique_element(seq, idfun=None):
    # order preserving
    if idfun is None:
        def idfun(x): return x
    seen = {}
    result = []
    for item in seq:
        marker = idfun(item)
        if marker in seen: continue
        seen[marker] = 1
        result.append(item)
    return result

def send_msg_to_clients(addrs, msg):
    for addr in addrs:
        sock = socket(AF_INET, SOCK_STREAM)
        sock.connect(addr)
        sock.send("%10s%s" % (len(msg), msg))
        sock.close()

class my_process:
    def __init__(self, pid, tag, node, r_chans, w_chans, err):
        self.pid = pid
        self.tag = tag
        self.node = node
        self.status = -1  # -1: no status   0: success   1: failed
        self.r_chans = r_chans
        self.w_chans = w_chans
        self.err = err

class user_config:
    def __init__(self):
        self.block_size = 0
        self.worker_num = []        # {opid, worker_num}
        self.cache_size = 0
        self.temp_store = 0
        self.temp_dir = None
        self.log_dir = None
    """
    def load(self):
        confparser = ConfigParser.ConfigParser()
        confparser.read(CONF_DIR + MAIN_CONF)
        nums = confparser.get("runtime", "worker_num").split(",")  # 1, 2, 4...
        for i in nums:
            self.worker_num.append(string.atoi(string.strip(i)))
        
        self.cache_size = string.atoi(confparser.get("db", "cache_size"))
        self.temp_store = string.atoi(confparser.get("db", "temp_store"))

        f = open(CONF_DIR + NODE_CONF, "rb")
        self.datanode = string.strip(f.read()).split("\n")
        f.close()
    """

class logical_tree:
    def __init__(self):
        self.name = None
        self.id = None
        self.node = {}  # <port, node> --> port is unique and node may not be unique
        self.p_node = {} # <port, node>  for parent operator
        self.dest = conf.DATA_TO_ANO_OP
        self.expression = None
        self.input = []
        self.output = []
        self.children = []
        self.status = None
        self.partition = None
        self.split_key = None
        self.function = []
        self.table_split_key = {} # the partition key for a table may be changed 
                                  # during the construction of logical plan
        self.keys = []   # key is the columns involved by this operator: table.attr
        self.tables = [] # the tables involved by this operator
        self.is_split = False  # the output of the operator is splitted or not
        self.is_merge = False  # the input of the operator is merged or not
        self.is_sql = None    # mark this operator should be merged into SQL or not
        self.is_visited = 0
        self.is_child_sql = False
        self.is_first_op = False  # 
        self.map = {}  # for some other temporary attributes
        self.udxes = []
        self.worker_num = 0

    def show_rec(self, indent):
        spaces = "-" * indent
        logging(("\n%s (- %s %s expression=%s input=%s output=%s key=%s split_key=%s tables=%s dest=%s)\n" % (spaces, self.name, self.id, self.expression, self.input, self.output, self.keys, self.split_key, self.tables, self.dest)))
        if self.children != []:
            for c in self.children:
                c.show_rec(indent + 1)

    def show(self):
        #self.show_rec(0)
        logging(self.get())
        
    def get_rec(self, indent, buf):
        spaces = "-" * indent
        buf.write("\n%s (- %s %s expression=%s input=%s output=%s key=%s dest=%s split_key=%s tables=%s dest=%s node=%s p_node=%s)\n" % (spaces, self.name, self.id, self.expression, self.input, self.output, self.keys, self.dest, self.split_key, self.tables, self.dest, self.node, self.p_node))
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
            child = logical_tree()
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
        tree1.partition = self.partition
        tree1.p_node = {}
        tree1.split_key = self.split_key
        tree1.is_child_sql = self.is_child_sql
        tree1.is_first_op = self.is_first_op
        tree1.map = self.map
        tree1.udxes = self.udxes
        
class logical_operator:
    def __init__(self):
        self.JOIN = 'join'
        self.PROJ = 'proj'
        self.SCAN = 'from'
        self.GROUP_BY = 'group_by'
        self.SUM = 'sum'
        self.COUNT = 'count'
        self.LIMIT = 'limit'
        self.UNION = 'union'
        self.WHERE = 'where'
        self.ORDER_BY = 'order_by'
        self.DISTINCT = 'distinct'
        self.AND = 'and'
        self.OR = 'or'
        self.VIRTUAL = 'VIRTUAL'
        self.SQL = 'sql'
        self.UDX = 'udx'
        self.AGGREGATE = "aggregate"
        
class compound_operator:
    def __init__(self):
        self.CO_UNION = 'union'
        self.CO_ALL = 'all'
        self.CO_INTERSECT = 'intersect'
        self.CO_EXCEPT = 'except'

class select_stmt:
    def __init__(self):
        self.results = None
        self.op = {}  #one of CO_UNION CO_ALL CO_INTERSECT CO_EXCEPT
        self.src_list = None            # from clause
        self.where = None        # where clause
        self.group_by = None     # group by clause
        self.having = None       # having clause
        self.order_by = None     # order by clause
        self.next_select = None  # next select in a compound stmt
        self.limit = None        # limit clause
        self.offset = None       # offset clause
        self.num_select_rows = 0 # estimate num of result rows
        self.udxes = []

class udx_def:
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
        
class create_stmt:
    def __init__(self):
        self.table = None
        self.column = None        # columns defenition for table
        self.select = None        # columns given by select sub-query

class mk_logical_plan:
    def __init__(self, task):
        self.task = task
        self.sql = task.query
        self.select_stmt = None
        self.update_stmt = None
        self.insert_stmt = None
        self.create_stmt = None
        self.parse_result = None

    """
    make() is to create a logical plan for each kind of SQL
    1. parse sql query using pyparsing and obtain parsed result--semantic_proc
    2. make the logical plan
    """
    def make(self):
        try:
            parse_result = semantic.parse(self.sql)
            logging(parse_result.dump())
            if self.sql.lower().startswith('select'):
                plan = self.mk_select_plan(parse_result)
                if plan is None:
                    return False
                self.task.plan = plan
            elif self.sql.lower().startswith('create'):
                self.mk_create_plan(parse_result)
            elif self.sql.lower().startswith('drop'):
                self.mk_drop_plan(parse_result)
            else:
                print 'please enter right sql'
            self.task.plan.show()
            return True
        except ParseException:
            logging(traceback.format_exc())
            ex = exception().QUERY_SYNTAX_PARSE_EXCEPTION
            send_msg_to_clients(self.task.reply_sock, ex)
            sys.exit(1)
        except Exception, e:
            logging(traceback.format_exc())
            Es("ERROR: %s\n" % (" ".join(str(s) for s in e.args)))

    def mk_create_plan(self, stmt):
        if stmt.index != '':
            self.mk_create_index_plan(stmt)
        else:
            self.mk_create_table_plan(stmt)

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
        tree = logical_tree()
        node = logical_tree()
        node.name = logical_operator().SQL
        node.expression = self.sql
        node.id = 0
        tree = node
        self.task.plan = tree
        info_center().update_table_info(self.task.database, table[0], column,
                                        index_name[0])
        
    """
    table = ['t1']
    column = [['a','int'], ['PRIMARY','KEY'], ['b','int'], ['c', 'varchar', '20']]
    select = select statemate
    """
    def mk_create_table_plan(self, stmt):
        self.make_create(stmt)
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
            column_for_table_create = ()
            for col in self.task.plan.output:
                # first check if the col has alias
                c = []
                if col in self.task.alias:
                    alias = self.task.alias[col]
                    column_for_table_create += alias
                    for a in alias:
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
                sql_create = "create table %s (%s)" % (table_name,
                                                       ",".join(column_for_table_create))
            queue = deque([])
            queue.append(self.task.plan)
            #find the last op
            while list(queue) != []:
                op = queue.popleft()
                for child in op.children:
                    queue.append(child)
            assert op.name == logical_operator().SQL
            new_sql_op = logical_tree()
            new_sql_op.name = logical_operator().SQL
            new_sql_op.expression = sql_create
            new_sql_op.tables = [table_name]
            # set the dest for this operator to be DATA_TO_ANO_OP to avoid sending
            # ACK to the master. Because the master checks if the task is finished
            # or not by the successful operators in the plan. If this operator sends
            # ACK to the master, it will cause errors.
            new_sql_op.dest = conf.DATA_TO_ANO_OP
            new_sql_op.id = op.id + 1
            op.children = [new_sql_op]

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
        if stmt.hash_key != '':
            key = stmt.hash_key[0]
        info_center().insert_table_info(self.task.database, table_name, column,
                                        nodes, key)
        
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
        S = {}
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
                            S[x_str] = 1
                        else:
                            if S.has_key(x_str):
                                del S[x_str]
        H = []
        for x_str in S.keys():
            for y in self.parse_data_node("%s%s%s" % (prefix, x_str, suffix)):
                H.append(y)
        return H
 
    def make_create(self, stmt):
        if stmt.select == '':
            tree = logical_tree()
            node = logical_tree()
            node.name = logical_operator().SQL
            sql = self.sql
            if stmt.node != '' or stmt.file != '':
                sql = sql[0:self.sql.find(" on ")]
            if stmt.hash_key != '':
                sql = sql[0:self.sql.find(" partition by")]
            if stmt.replica_num != '':
                sql = sql[0:self.sql.find(" replica ")]
            node.expression = sql
            node.tables = stmt.table
            node.id = 0
            tree = node
            tree.dest = None
        else:
            tree = self.mk_select_plan(stmt.select)
            tree.dest = conf.DATA_TO_DB
            tree.map[conf.DEST_TABLE] = stmt.table[0]
        self.task.plan = tree

    def mk_drop_plan(self, stmt):
        node = logical_tree()
        node.name = logical_operator().SQL
        node.expression = self.sql
        node.id = 0
        node.dest = conf.NO_DATA
        node.tables = stmt.table
        data_node = info_center().get_data_node_info(metadata_db, stmt.table[0])
        if data_node == []:
            Ws("Error: table %s is not exists\n" % (stmt.table[0]))
            sys.exit(1)
        self.task.plan = node
        self.task.table = stmt.table[0]
        self.task.tag = "DROP"

    def mk_select_plan(self, stmt):
        plan = self.make_select(stmt)
        if not plan:
            return None
        plan = self.optimize(plan)
        plan.dest = conf.DATA_TO_LOCAL
        return plan

    def make_select(self, stmt):
        tree = logical_tree()
        srclist = stmt.fromList
        if srclist != '':
            num = len(srclist)
            """
            join sql or nested sql
            """
            if num > 1:
                if srclist[0].lower() == "select":
                    sub_plan = self.mk_select_plan(srclist)
                    tree = sub_plan
                else:
                    """
                    srclist: [table1, [join1], [join2]...] or [table1,[[table2]]]
                    join1: ['join',[table],[on condition]]
                    on condition: ['on', [[[c1],'and',[c2]], 'or', [c3]]
                    c : [['table','.','attr'], '>', '2']
                    """
                    first_node = logical_tree()
                    first_node.name = logical_operator().AND
                    for i in range(num):
                        if i==0:
                            continue
                        # join table on expression
                        if len (srclist[i][0]) < 3:
                            continue
                        node_join = logical_tree()
                        if "".join(srclist[i][0]).lower() == "join":
                            node_join = self.get_WHERE_CLAUSE_node(srclist[i][2][1])
                        """
                        adding a AND node to connect the explicit join 
                        conditions and where clause
                        """
                        first_node.children.append(node_join)
                        first_node.tables += node_join.tables
                    if first_node.children != []:
                        tree = first_node
            else:
                self.task.table = srclist[0]
                if self.task.table not in info_center().get_table_info(metadata_db).split("\n"):
                    logging("ERROR: %s is not exist" % (self.task.table))
                    for addr in self.task.reply_sock:
                        sock = socket(AF_INET,SOCK_STREAM)
                        sock.connect(addr)
                        msg = "ERROR: %s is not exist" % (self.task.table)
                        sock.send("%10s%s" % (len(msg), msg))
                        sock.close()
                    sys.exit(1)
                if stmt.where == '':
                    scan_node = logical_tree()
                    scan_node.name = logical_operator().SCAN
                    scan_node.expression = "".join(srclist[0])
                    scan_node.keys.append("".join(srclist[0]))
                    scan_node.tables += [srclist[0]]
                    if tree.name == None:
                        tree = scan_node
                    else:
                        scan_node.children.append(tree)
                        tree = scan_node
        if stmt.where != '':
            node = self.get_WHERE_CLAUSE_node(stmt.where)
            if tree.name != None:
                node_temp = logical_tree()
                node_temp.name = logical_operator().AND
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
        if stmt.select != '':
            aggr_node = None
            groupby_node = []
            proj_node = logical_tree()
            proj_node.name = logical_operator().PROJ
            if str(stmt.select[0]) == "distinct":
                if stmt.results[0].lower() == logical_operator().DISTINCT:
                    node = logical_tree()
                    node.name = logical_operator().DISTINCT
                    node.children.append(tree)
                    node.tables += tree.tables
                    node2.name = logical_operator().PROJ
                    node2.children.append(node)
                    node2.tables += node.tables
            else:
                columns = stmt.select
                i = 0
                while i < len(columns):
                    col = columns[i]
                    if isinstance(col, str) == False:
                        if col == columns.func:
                            # this is function
                            """
                            three cases:
                            (1) general functions with group by: sum, total, ...
                            (2) general functions without group by: sum, total...
                            (3) user-defined functions with definition
                            """
                            j = 0
                            args = []
                            for aa in columns.func.args:
                                if isinstance(aa, str): args.append(aa)
                                else: args.append("".join(aa))
                            for k in range(len(args)):
                                arg = args[k]
                                if arg.find(".") == -1 and arg != '*':
                                    args[k] = self.task.table + "." + arg
                            funcs = []
                            udxnode = None
                            while j < len(col) - len(args):
                                func = col[j]
                                funcs.append(func)
                                if func in conf.GENERAL_FUNC:
                                    """
                                    GENERAL_FUNC only has one argument
                                    """
                                    assert len(args) == 1
                                    arg = args[0]
                                    if stmt.group_by != "":
                                        if groupby_node == []:
                                            node = logical_tree()
                                            node.name = logical_operator().GROUP_BY
                                            if len(arg.split(".")) == 2:
                                                table = arg.split(".")[0]
                                            else:
                                                table = self.task.table
                                            node.tables.append(table)
                                            # use table.column as a key
                                            temp_col = "".join(stmt.group_by[0])
                                            if temp_col.find(".") == -1:
                                                temp_col = table + "." + temp_col
                                            node.keys.append(temp_col)
                                            node.expression = "".join(stmt.group_by[0])
                                            node.function.append(func + '(' + arg + ')')
                                            groupby_node.append(node)
                                        else:
                                            node = groupby_node[0]
                                            node.function.append(func)
                                    else:
                                        aggr_node = logical_tree()
                                        aggr_node.name = logical_operator().AGGREGATE
                                        aggr_node.keys.append(func + '(' + arg + ')')
                                        aggr_node.output.append(func + '(' + arg + ')')
                                        aggr_node.tables.append(self.task.table)
                                        aggr_node.expression = arg
                                        aggr_node.is_sql = 0
                                    if proj_node.expression == None:
                                        proj_node.expression = func + '(' + arg + ')'
                                    else:
                                        proj_node.expression += (','+ func + '(' + arg + ')')
                                    j += 1
                                else:
                                    """
                                    UDX support multiple arguments, e.g.
                                    F(a) ; F(a, b);  F(a, b, c)
                                    """
                                    if stmt.udx == "":
                                        return None
                                    udx_stmt = None
                                    for u in stmt.udx:
                                        if u[0] == func:
                                            udx_stmt = u
                                            break
                                    if udxnode is None:
                                        udxnode = logical_tree()
                                        udxnode.name = logical_operator().UDX
                                    udx = self.parse_udx(udx_stmt)
                                    udxnode.udxes.append(udx)
                                    j += 1
                            if udxnode is not None:
                                udxnode.children.append(tree)
                                udxnode.is_sql = 0
                                for arg in args:
                                    udxnode.keys.append(arg)
                                tree = udxnode
                            out = ""
                            for func in funcs:
                                out += (func + "(")
                            # use table.col way
                            out += ",".join(args)
                            for func in funcs:
                                out += ")"
                            proj_node.output.append(out)
                            # check if the column has alias
                            if i + 1 < len(columns) and columns[i + 1] == conf.AS:
                                alias = columns[i + 2]
                                if isinstance(alias, str):
                                    self.task.alias[out] = (alias,)
                                else:
                                    a = tuple()
                                    for al in alias:
                                        a += (al,)
                                    self.task.alias[out] = a
                                i += 3
                            else:
                                i += 1
                        else:
                            # this is a column with tablename [table_name.column]
                            if proj_node.expression == None:
                                proj_node.expression = "".join(col)
                            else:
                                proj_node.expression += (',' + "".join(col))
                            # use table.col way
                            cc = "".join(col)
                            if cc.find(".") == -1: cc = self.task.table + "." + cc
                            if cc == "%s.*" % (self.task.table):
                                ccs = info_center().get_table_columns(metadata_db,
                                                                      self.task.table)
                                for cc in ccs:
                                    proj_node.output.append(str(cc))
                            else:
                                proj_node.output.append(cc)
                            # check if the column has alias
                            if i+1 < len(columns):
                                if columns[i+1] == conf.AS:
                                    alias = columns[i+2]
                                    self.task.alias[cc] = (alias,)
                                    i += 3
                                else:
                                    i += 1
                            else:
                                i += 1
                    else:
                        # this column only has a column name
                        if proj_node.expression == None:
                            proj_node.expression = col
                        else:
                            proj_node.expression += (',' + col)
                        # use table.col way
                        cc = col
                        if cc.find(".") == -1: cc = self.task.table + "." + cc
                        if cc == "%s.*" % (self.task.table):
                            ccs = info_center().get_table_columns(metadata_db,
                                                                  self.task.table)
                            for cc in ccs:
                                proj_node.output.append(str(cc))
                        else:
                            proj_node.output.append(cc) 
                        # check if the column has alias
                        if i + 1 < len(columns) and columns[i+1] == conf.AS:
                            alias = columns[i+2]
                            self.task.alias[cc] = (alias,)
                            i += 3
                        else:
                            i += 1
            if groupby_node != []:
                groupby = groupby_node[0]
                groupby.children.append(tree)
                for t in tree.tables:
                    if t not in groupby.tables:
                        groupby.tables.append(t)
                tree = groupby
            if tree.name != None:
                proj_node.children.append(tree)
                proj_node.tables = unique_element(proj_node.tables + tree.tables)
            tree = proj_node
            if aggr_node != None:
                aggr_node.children = [tree]
                tree = aggr_node
        if stmt.order_by != '':
            # [['a', '.', 'term']]
            node = logical_tree()
            node.name = logical_operator().ORDER_BY
            node.expression = "".join(stmt.order_by[0])
            node.children = tree.children
            temp_col = "".join(stmt.order_by[0])
            if temp_col.find(".") == -1:
                temp_col = self.task.table + "." + temp_col
            node.keys = [temp_col]
            node.tables += tree.tables
            node.is_sql = 0
            tree.children = [node]
        if stmt.limit != '':
            node = logical_tree()
            node.name = logical_operator().LIMIT
            node.children = tree.children
            node.tables += tree.tables
            node.expression = stmt.limit
            tree.children = [node]
        return tree
        
    # the unit of where expression without 'and' or 'or'
    # the form is [['x', '.', 'a'], '>', '10'] or [['x', '.', 'a'], 'MATCH', 'protein'] or ['a', '>', '10']
 
    def get_WHERE_node(self,whereexpression):
        node = logical_tree()
        exp = ''
        for i in range(len(whereexpression)):
            exp += "".join(whereexpression[i]) + " "
        node.expression = exp
        tables = []
        attrs = []
        for m in range (len(whereexpression)):
            cond = whereexpression[m]
            cond3 = "".join(cond)
            if cond3.find(".") == -1:
                # if no table name is before the column name, we first query the
                # metadata database to get the table name
                col = cond3
                if self.task.table is not None:
                    col_table = self.task.table
                else:
                    col_table = info_center().get_table_name_by_col(self.task.database,
                                                                    col)
                if col_table is not None:
                    cond3 = col_table + '.' + cond3
            if cond3.find('.') != -1:
                table = cond3.split('.')[0]
                attr = cond3
                flag = 0
                for element in tables:
                    if table == element:
                        flag = 1
                        break
                if flag == 0:
                    tables.append(table)
                    attrs.append(attr)
        if len(tables) == 2:
            node.name = logical_operator().JOIN
            node.keys = attrs
            child1 = logical_tree() 
            child1.name = logical_operator().SCAN
            child1.expression = tables[0]
            child1.keys.append(attrs[0])
            child1.split_key = attrs[0]
            child1.tables.append(tables[0])
            child2 = logical_tree()
            child2.name = logical_operator().SCAN
            child2.keys.append(attrs[1])
            child2.split_key = attrs[1]
            child2.expression = tables[1]
            child2.tables.append(tables[1])
            node.children.append(child1)
            node.children.append(child2)
            node.tables += child1.tables
            node.tables += child2.tables
        else:
            node.name = logical_operator().WHERE
            node.keys = attrs
            child = logical_tree()
            child.name = logical_operator().SCAN
            child.expression = tables[0]
            child.tables.append(tables[0])
            for a in attrs:
                child.keys.append(a)
            node.children.append(child)
            node.tables += child.tables
        return node

    def get_AND_node(self,andexpression):
        nodeAND = logical_tree()
        if andexpression[1].lower() != 'and' :
            nodeAND = self.get_WHERE_node(andexpression)
        else:
            nodeAND.name = logical_operator().AND
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
        node = logical_tree()
        if len(where) > 1:
            if where[1].lower() == 'or':
                node.name = logical_operator().OR
                for i in range (len(where)):
                    if i % 2 == 0:
                        cond1 = where[i]
                        and_node = self.get_AND_node(cond1)
                        for key in and_node.keys:
                            node.keys.append(key)
                        node.children.append(and_node)
                        for t in and_node.tables:
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
            u = udx_def()
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
            self.set_output_attrs(plan)
            self.and_rule(plan)
            self.or_rule(plan)
            plan = self.split_rule(plan)
            plan = self.make_SQL(plan)
            plan = self.set_others(plan)
            return plan
        except:
            traceback.print_exc()

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
                if child.name == logical_operator().AND:
                    node = self.and_rule_op(child)
                    queue.append(operator)
                    operator.children.remove(child)
                    operator.children.append(node)
                else:
                    queue.append(child)

    def and_rule_op(self, and_node):
        for child in and_node.children:
            if child.name == logical_operator().AND:
                node = self.and_rule_op(child)
                node.copy(child)

        if len(and_node.children) == 1:
            temp = and_node.children[0]
            and_node = temp
        else:
            flag = 0
            # store the where node [table_name, where_node]
            table_dic = {}
            join_node = []
            l = len(and_node.children)
            k = 0
            for i in range(l):
                i -= k
                if i > l - k:
                    break
                child = and_node.children[i]
                if child.name == logical_operator().WHERE:
                    table = child.keys[0].split('.')[0]
                    if table_dic.has_key(table) == False:
                        table_dic[table] = child
                    else:
                        node = table_dic[table]
                        node.expression += (and_node.name + ' ' + child.expression) 
                        and_node.children.remove(child)
                        k += 1
                elif child.name == logical_operator().JOIN:
                    join_node.append(child)
                    flag = 1
            if flag == 1:
                # store join node [table_name, join_node]
                table_name_dic = {}
                for join in join_node:
                    for scan in join.children:
                        if scan.name != logical_operator().SCAN:
                            continue
                        scan_table = scan.keys[0].split('.')[0]
                        if table_name_dic.has_key(scan_table) == False :
                            table_name_dic[scan_table] = join
                        else:
                            join_pres = table_name_dic.pop(scan_table)
                            table_name_dic[scan_table] = join
                            join.children.remove(scan)
                            join.children.append(join_pres)
                            join_pres.split_key = scan.split_key
                            join.tables += unique_element(join_pres.tables)
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
                            continue
                        if table_dic.has_key(scan_table) == True:
                            where_node = table_dic[scan_table]
                            where_node.split_key = scan.split_key
                            join.children.remove(scan)
                            join.children.append(where_node)
                            and_node.children.remove(where_node)

                and_node = join
            else:
                one_node = logical_tree()
                one_node.name = logical_operator().WHERE
                for k in table_dic:
                    one_node.keys.append(table_dic[k].keys[0])
                    if one_node.expression == None:
                        one_node.expression = table_dic[k].expression
                    else:
                        one_node.expression += table_dic[k].expression 
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
                if child.name == logical_operator().OR and child.is_visited != 1:
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
            if child.name == logical_operator().WHERE:
                table = child.keys[0].split('.')[0]
                if wnd.has_key(table) == False:
                    wnd[table] = child
                else:
                    node = wnd[table]
                    node.expression += (or_node.name + ' ' + child.expression) 
                    or_node.children.remove(child)
                    k += 1
            elif child.name == logical_operator().JOIN:
                join_node.append(child)
                flag = 1
        
        if flag == 1:
            for join in join_node:
                for scan in join.children:
                    if scan.name != logical_operator().SCAN:
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
            one_node = logical_tree()
            one_node.name = logical_operator().WHERE
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
        if len(plan.children) > 0 and plan.name == logical_operator().PROJ:
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
            op.id = count
            count += 1
            if op.name == logical_operator().UDX:
                for child in op.children:
                    child.dest = conf.DATA_TO_CLIENTS
            if op.name == logical_operator().GROUP_BY:
                if op.children[0].name == logical_operator().SQL:
                    op.is_child_sql = True
                else:
                    input_new = []
                    for i in op.input:
                        m = re.match("(.*)\((.*)\)", i) 
                        if m:
                            if m.group(2) not in input_new:
                                input_new.append(m.group(2))
                        else:
                            if i not in input_new:
                                input_new.append(i)
                    op.input = input_new
            if op.name == logical_operator().SQL:
                ex = op.expression
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
            for child in op.children:
                queue.append(child)

        queue = deque([])
        queue.append(plan)
        while list(queue) != []:
            op = queue.popleft()
            if op.name == logical_operator().JOIN:
                if len(op.children) == 2:
                    op.map[op.children[0].id] = op.children[0].output
                    op.map[op.children[1].id] = op.children[1].output
            for child in op.children:
                queue.append(child)

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
            if operator.name == logical_operator().JOIN:
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

            if operator.name == logical_operator().UDX:
                for out in operator.output:
                    if out.find("(") != -1:
                        o = out[out.rfind("(") + 1:out.find(")")]
                        for oo in o.split(","):
                            if not (string.strip(oo) in operator.input):
                                operator.input.append(string.strip(oo))
                    else:
                        if not (out in operator.input):
                            operator.input.append(out)
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
            if operator.name == logical_operator().JOIN:
                for child in operator.children:
                    for inp in operator.input:
                        t = inp.split(".")[0]
                        if t in child.tables and inp not in child.output:
                            child.output.append(inp)
                    queue.append(child)
            else:
                for child in operator.children:
                    child.output = operator.input
                    queue.append(child)
                
    def split_rule(self, plan):
        """
        use hierarchical retiveal to visit each operator in
        logical_plan. If the operator is JOIN or GROUP_BY, check
        if the join key and group key are the same with the table
        partition key, if not, set the is_split attr of related
        operator be true.
        """
        if self.task.table is not None:
            partition_num = len(info_center().get_data_node_info(metadata_db,
                                                                 self.task.table))
            if partition_num == 1:
                # delete aggregation node
                if plan.name == logical_operator().AGGREGATE and len(plan.children) > 0:
                    plan = plan.children[0]
                return plan
        
        queue = []
        queue.append(plan)
        while queue != []:
            op = queue.pop()
            if op.table_split_key == {}:
                self.set_split_key_op(op)
            for child in op.children:
                queue.append(child)
        return plan

    def set_split_key_op(self, op):
        if len(op.children) == 0:
            for k in op.keys:
                table_name = k.split('.')[0]
                table_split_key = info_center().get_table_partition_key(metadata_db,
                                                                        table_name)
                op.table_split_key[table_name] = table_split_key
        else:
            for child in op.children:
                self.set_split_key_op(child)
                for key in child.table_split_key.keys():
                    op.table_split_key[key] = child.table_split_key[key]
            if op.name == logical_operator().JOIN:
                """
                check if two involved tables are partitioned on same nodes,
                if so, check the partition key;
                if not, op.is_sql = 0 in any case
                """
                p1 = info_center().get_data_node_info(metadata_db, op.tables[0])
                p2 = info_center().get_data_node_info(metadata_db, op.tables[1])
                if p1 == p2:
                    if len(p1) == 1:
                        op.is_sql = 1
                    else:
                        for k in op.keys:
                            table_name = k.split('.')[0]
                            attr_name = k.split('.')[1]
                            if attr_name != op.table_split_key[table_name]:
                                for child in op.children:
                                    if child.keys[0].split('.')[0] == table_name:
                                        child.is_split = True
                                        #child.split_key = child.keys[0]
                                        op.is_merge = True
                                        op.is_sql = 0
                                op.table_split_key[table_name] = attr_name
                else:
                    for child in op.children:
                        if child.keys[0].split('.')[0] == table_name:
                            child.is_split = True
                            #child.split_key = child.keys[0]
                            op.is_merge = True
                            op.is_sql = 0
                    op.table_split_key[table_name] = attr_name
            elif op.name == logical_operator().GROUP_BY:
                m = op.keys[0].split('.')
                if len(m) == 1 and self.task.table is not None:
                    table_name = self.task.table
                    attr_name = m[0]
                elif len(m) == 2:
                    table_name = m[0]
                    attr_name = m[1]
                if attr_name != op.table_split_key[table_name]:
                    child = op.children[0]
                    child.is_split = True
                    child.split_key = table_name + "." + attr_name
                    op.is_merge = True
                    op.is_sql = 0
                op.table_split_key[table_name] = attr_name
                
    def make_SQL(self, plan):
        self.mark_SQL(plan)
        sql_node = logical_tree()
        if plan.is_sql == 1:
            sql_node = self.make_SQL_op(plan)
            plan = sql_node
        else:
            queue = []
            queue.append(plan)
            while queue!= []:
                operator = queue.pop()
                for child in operator.children:
                    if child.is_sql == 1 and child.name != logical_operator().SQL:
                        if operator.name == logical_operator().GROUP_BY:
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
        sql = logical_tree()
        sql.name = logical_operator().SQL
        if operator.name == logical_operator().GROUP_BY:
            sql.is_split = operator.children[0].is_split
            sql.split_key = operator.children[0].split_key
            sql.is_merge = operator.children[0].is_merge
        else:
            sql.is_split = operator.is_split
            sql.is_merge = operator.is_merge
            sql.split_key = operator.split_key
        sql.tables = operator.tables

        if operator.name == logical_operator().GROUP_BY:
            sql.output = operator.input
        elif operator.name == logical_operator().PROJ:
            sql.output = operator.output
        else:
            flag = 0
            for out in operator.output:
                flag = 0
                for table in sql.tables:
                    if out.find(".") == -1 or out.split('.')[0] == table:
                        flag = 1
                        break
                if flag == 1:
                    sql.output.append(out)
        expression = ''
        dic = {}
        queue = []
        queue.append(operator)
        while queue != []:
            op = queue.pop()
            if dic.has_key(op.name) == 0:
                dic[op.name] = op.expression 
            else:
                exp = dic[op.name]
                if op.name == logical_operator().SCAN:
                    exp += (',' + op.expression)
                else:
                    exp += (logical_operator().AND + ' ' + op.expression + ' ')
                dic[op.name] = exp
            for child in op.children:
                queue.append(child)
        
        while dic != {}:
            for out in sql.output:
                if expression == '':
                    expression = ('select ' + out)
                else:
                    expression += (',' + out)
            if dic.has_key(logical_operator().PROJ) == 1:
                dic.pop(logical_operator().PROJ)
            expression += (' ')
            if dic.has_key(logical_operator().SCAN) == 1:
                temp = logical_operator().SCAN
                expression += (temp + ' ' + dic.pop(temp) + ' ')
            if dic.has_key(logical_operator().WHERE) == 1:
                temp = logical_operator().WHERE
                expression += (temp + ' ' + dic.pop(temp) + ' ')
                if dic.has_key(logical_operator().JOIN) == 1:
                    temp = logical_operator().JOIN
                    expression += (logical_operator().AND + ' ' + dic.pop(temp) + ' ')
            else:
                if dic.has_key(logical_operator().JOIN) == 1:
                    temp = logical_operator().JOIN
                    expression += (logical_operator().WHERE + ' ' + dic.pop(temp) + ' ')
            if dic.has_key(logical_operator().AGGREGATE) == 1:
                temp = logical_operator().AGGREGATE
                expression += (' ' + dic.pop(temp) + ' ')
            if dic.has_key(logical_operator().GROUP_BY) == 1:
                temp = logical_operator().GROUP_BY
                name = temp.replace('_', ' ')
                expression += (name + ' ' + dic.pop(temp) + ' ')
            if dic.has_key(logical_operator().ORDER_BY) == 1:
                temp = logical_operator().ORDER_BY
                name = temp.replace('_', ' ')
                expression += (name + ' ' + dic.pop(temp) + ' ')
            if dic.has_key(logical_operator().LIMIT) == 1:
                temp = logical_operator().LIMIT
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
            if child.is_sql == 0:
                operator.is_sql = 0
                flag = 1
                break
        if flag == 0:
            operator.is_sql = 1

class data_paralleler:

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
                if op.name == logical_operator().UDX:
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
            return False
        # msg = worker_id:worker_node:worker_port:block_size
        s_size = int(data_node.b_size + data_node.b_size * (worker_node.score - 1))
        if s_size > data_node.l_size:
            s_size = data_node.l_size
        sep = conf.SEP_IN_MSG
        if data_node.name == worker_node.name:
            addr = worker_node.local_addr
        else:
            addr = "%s%s%s" % (worker_node.name, sep, worker_node.port)
        msg = '%s%s%s%s%d' % (worker_node.id, sep, addr, sep, s_size)
        # add node to used_worker
        if self.used_worker.has_key(worker_node.id) != 1:
            self.used_worker[worker_node.id] = worker_node
            self.task.used_worker_num += 1
        sock = socket(AF_INET, SOCK_STREAM)
        addr = (data_node.name, data_node.port)
        try:
            sock.connect(addr)
            sock.send('%10s%s' % (len(msg), msg))
            sock.close()
        except Exception, e:
            if e.errno == 4:
                logging("ERROR: %s : %s --> %s " % (e.errno, data_node.name,
                                                    worker_node.id))
                return

        logging('CMD: %s --> %s id=%s' % (data_node.name, msg, worker_node.id))
        worker_node.s_time = time.time()*1000
        worker_node.cur_bk = s_size
        data_node.l_size = max(data_node.l_size - s_size, 0)
        worker_node.status = conf.RUNNING
        if data_node.l_size == 0:
            sock = socket(AF_INET, SOCK_STREAM)
            sock.connect((data_node.name, data_node.port))
            sock.send('%10s%s' % (len(conf.END_TAG), conf.END_TAG))
            sock.close()
        return True

    def distribute_data(self, node):
        try:
            if isinstance(node, worker_node):
                dn = self.get_best_data_node(node)
                if dn:
                    self.send_cmd(dn, node)
            elif isinstance(node, data_node):
                while True:
                    wn = self.get_avaliable_worker_node(node)
                    if wn is None or node.l_size <= 0:
                        break
                    self.send_cmd(node, wn)
        except Exception, e:
            logging(traceback.format_exc())
                
class task_manager:
    def __init__(self, iom, userconfig):
        self.taskqueue = {}        # store all task, once a task is finished, delete it
        self.queue = Queue.Queue() # store all task, once there is one, take it out
                                   # and process it
        self.nid_cid_map = {}  # node id --> collective query id
        self.process = {}
        self.remote_node_port_map = {}
        self.iom = iom
        self.userconfig = userconfig

    def put_task_to_queue(self, task, cnode, so):
        tid = task.id
        if self.taskqueue.has_key(tid) == 1:
            self.taskqueue[tid].clients.append(cnode)
            self.taskqueue[tid].reply_sock.append(so)
        else:
            task.clients.append(cnode)
            task.s_time = time.time()*1000
            task.reply_sock.append(so)
            self.taskqueue[tid] = task
            self.queue.put(task)

    def get_task_from_queue(self, tid):
        if self.taskqueue.has_key(tid) != 1:
            return None
        else:
            return self.taskqueue[tid]
        
    def remove_task_from_queue(self, tid):
        if self.taskqueue.has_key(tid) == 1:
            self.taskqueue.pop(tid)
        else:
            Es('Task %s is not in task queue' % (tid))

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
        try:
            while list(queue) != []:
                op = queue.popleft()
                if op.name == logical_operator().AGGREGATE or op.name == logical_operator().ORDER_BY:
                    parnode = info_center().get_data_node_info(task.database,
                                                               op.tables[0])
                    port = get_unique_num(10000,14999)
                    op.node[port] = parnode[random.randint(0, len(parnode)-1)]
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
                        UDX is special here, we don't specify the port number, but use
                        the count number as the node key.
                        """
                        op.node[count] = w
                        count += 1
                    for child in op.children:
                        queue.append(child)
                else:
                    parnode = info_center().get_data_node_info(task.database,
                                                               op.tables[0])
                    if self.userconfig.worker_num != []:
                        worker_num_op = self.userconfig.worker_num[op.id]
                    else:
                        worker_num_op = len(parnode)
                    if worker_num_op == -1:
                        worker_num_op = len(parnode)
                    op.worker_num = worker_num_op
                    for i in range(worker_num_op):
                        port = get_unique_num(10000,14999)
                        op.node[port] = parnode[(i + used_num) % len(parnode)] 
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
            cons = mk_logical_plan(task)
            return cons.make()
        except Exception, e:
            traceback.print_exc()
            task.status = task.TASK_FAIL
            return False

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
                node = op.node[port]
                cmd = "gxpc e -h %s python %s %s" % (node, name, args)
                #cmd = "gxpc e -h %s python %s" % (node, name)
                #cmd = "ssh %s \"python %s %s\"" % (node, name, args)
                pipes = [ (None, "w", 0), (None, "r", 1), (None, "r", 2) ]
                try:
                    x = self.iom.create_process(cmd.split(), None, None, pipes, None)
                except:
                    logging(traceback.format_exc())
                pid,r_chans,w_chans,err = x
                for ch in r_chans:
                    ch.flag = m_paraLite().PROCESS_OUT
                    ch.buf = cStringIO.StringIO()
                #logging("pid = %s proc = %s" % (pid, op.name))
                if pid is None:
                    Ws("failed to create process %s\n" % cmd)
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
                if info_center().update_setting_info(t.database, 2, output):
                    ret = "OK"
                else:
                    ret = "FAIL"
        elif t.type == t.ROW_SEPARATOR:
            if len(t.cmd.split()) == 1:
                ret = "please enter right command"
            else:
                sep = t.cmd.split()[1].decode("string-escape")
                if info_center().update_setting_info(t.database, 0, sep):
                    ret = "OK"
                else:
                    ret = "FAIL"
        elif t.type == t.COL_SEPARATOR:
            if len(t.cmd.split()) == 1:
                ret = "please enter right command"
            else:
                sep = t.cmd.split()[1].decode("string-escape")
                if info_center().update_setting_info(t.database, 1, sep):
                    ret = "OK"
                else:
                    ret = "FAIL"
        elif t.type == t.INDEX:
            table = None
            if len(t.cmd.split()) > 1:
                table = t.cmd.split()[1]
            ret = info_center().get_index_info(t.database, table)
        elif t.type == t.SHOW:
            ret = info_center().get_setting_info(t.database)
        elif t.type == t.TABLE:
            ret = info_center().get_table_info(t.database)
        elif t.type == t.ANALYZE:
            temp_task = task()
            temp_task.database = metadata_db
            sql = t.cmd[t.cmd.find("\"")+1:len(t.cmd)-1]
            temp_task.query = sql
            logging("TASK: %s -> %s : analyze" % (t.id, t.cmd))
            if self.parse_task(temp_task):
                ret = temp_task.plan.get()
        for addr in t.reply_sock:
            sock = socket(AF_INET,SOCK_STREAM)
            sock.connect(addr)
            ret = string.strip(ret)
            sock.send("%10s%s" % (len(ret), ret))
            sock.close()

    def execute(self): 
        task = self.queue.get()
        if task.__class__.__name__ == "task":
            s1 = time.time()*1000
            if not self.parse_task(task):
                for addr in task.reply_sock:
                    sock = socket(AF_INET,SOCK_STREAM)
                    sock.connect(addr)
                    m = "ERROR: cannot parse query correctly, please check the syntax of the query!"
                    sock.send("%10s%s" % (len(m), m))
                    sock.close()
                sys.exit(1)
            e1 = time.time()*1000
            if not self.schedule_task(task):
                Ws("ERROR: in schedule_task")
                return 0
            logging("schedule task successuflly")
            e2 = time.time()*1000
            if not self.execute_task(task):
                return 0
            e3 = time.time()*1000
            logging("TASK: %s : all related processes are started" % (task.id))
        else:
            logging("TASK: %s --> %s : start" % (task.id, task.type))
            self.execute_special_task(task)
            self.remove_task_from_queue(task.id)

    def all_children_finish(self, operator):
        for child in operator.children:
            if child.status == None or child.status != conf.JOB_FINISH:
                return 0
        return 1

    def mk_job_args(self, task, op, port, opid):
        dic = {}
        attrs = {}
        attrs = info_center().get_all_attrs(task.database, op.tables)
        # common args for all operators
        # start
        dic[conf.MASTER_NAME] = master_node
        dic[conf.MASTER_PORT] = str(master_port)
        dic[conf.TEMP_DIR] = self.userconfig.temp_dir
        dic[conf.LOG_DIR] = self.userconfig.log_dir
        dic[conf.CQID] = task.id
        dic[conf.OPID] = opid
        dic[conf.INPUT] = op.input
        dic[conf.OUTPUT] = op.output
        dic[conf.NAME] = op.name        
        dic[conf.ATTRS] = attrs
        node = op.node[port]
        dic[conf.NODE] = (node, port) # (node,port)
        dic[conf.OUTPUT_DIR] = task.output_dir
        dic[conf.DB_ROW_SEP] = info_center().get_separator(task.database)[0]
        dic[conf.DB_COL_SEP] = info_center().get_separator(task.database)[1]
        dic[conf.CLIENT_SOCK] = task.reply_sock
        if op.p_node != {}:
            if op.is_split:
                dic[conf.P_NODE] = op.p_node
            else:
                # choose the same parent node
                t = {}
                if port not in op.p_node:
                    # its father should be AGGREGATE node
                    dic[conf.P_NODE] = op.p_node
                else:
                    t[port] = op.p_node[port]
                    dic[conf.P_NODE] = t
        else:
            dic[conf.P_NODE] = {}
        if op.split_key != None:
            dic[conf.SPLIT_KEY] = op.split_key
        num_of_children = 0
        for child in op.children:
            num_of_children += len(child.node)
        dic[conf.NUM_OF_CHILDREN] = num_of_children
        if op.dest != None: dic[conf.DEST] = op.dest
        dic[conf.EXPRESSION] = op.expression
        # end
        if op.dest == conf.DATA_TO_ONE_CLIENT:
            key = task.worker_nodes.keys()[0]
            dic[conf.DEST_CLIENT_NAME] = task.worker_nodes[key].name
            dic[conf.DEST_CLIENT_PORT] = task.worker_nodes[key].port
        elif op.dest == conf.DATA_TO_DB:
            t = op.map[conf.DEST_TABLE]
            dic[conf.DEST_TABLE] = t
            dic[conf.DEST_DB] = task.database
            hash_key = info_center().get_table_partition_key(task.database, t)
            if hash_key is not None and hash_key != "":
                dic[conf.FASHION] = conf.HASH_FASHION
                dic[conf.HASH_KEY] = hash_key
                aa = info_center().get_table_columns(task.database, t)
                dic[conf.HASH_KEY_POS] = aa.index(hash_key)
            else:
                dic[conf.FASHION] = conf.ROUND_ROBIN_FASHION     
        """
        special for SQL operator: database info
        get active database name for each node
        """
        if op.name == logical_operator().SQL:
            dbname = info_center().get_active_db(task.database, node)
            dic[conf.DATABASE] = dbname
            if self.userconfig.cache_size != 0:
                dic[conf.CACHE_SIZE] = self.userconfig.cache_size
            if self.userconfig.temp_store != 0:
                dic[conf.TEMP_STORE] = self.userconfig.temp_store

        if op.name == logical_operator().GROUP_BY:
            dic[conf.IS_CHILD_SQL] = op.is_child_sql
            dic[conf.KEY] = op.keys[0]
        if op.name == logical_operator().AGGREGATE or op.name == logical_operator().ORDER_BY:
            dic[conf.KEY] = op.keys[0]
        """
        special for udx operator
        """
        if op.name == logical_operator().UDX:
            dic[conf.KEY] = op.keys
            dic[conf.UDX] = op.udxes
            time_str = time.strftime('%Y-%m-%d-%H-%M-%S',time.localtime(time.time()))
            dic[conf.LOCAL_ADDR] = "%s%s%s-%s-%s" % (self.userconfig.temp_dir,
                                                     os.sep, "UNIX.d", time_str,
                                                     random.randint(1,10000))
            #udx = op.udxes[0]
        """
        the inputs of JOIN operator is special:
        {'opid1', ['in1', 'in2']}
        {'opid2', ['in2', 'in2']}
        """
        if op.name == logical_operator().JOIN:
            dic[conf.INPUT] = op.map
        s = cPickle.dumps(dic)
        b = base64.encodestring(s)
        s_in_a_line = string.replace(b, '\n', '*')
        return s_in_a_line

    def set_job_status(self, message):
        op_name, op_id, op_status, cost_time= self.parse(message)
        self.set_op_status(self.plan, op_name, op_id, op_status, cost_time)

class info_center:

    def create_metadata_table(self, database):
        conn = sqlite3.connect(database)
        """
        First of all, check metadata tables:
        (1) setting_info: |row_separator|col_separator|output|
        (2) data_node_info: |table_name|node_name|
        (3) table_size_info: |name|size|record_num|
        (4) table_attr_info: |name|attribute|type|is_key|is_index|index_name
        (5) table_pos_info: |name|node|db|size|
        (6) table_partition_info: |name|partition_num|partition_key|
        (7) sub_db_info: |node|db_name|db_size|status|
        """
        cr = conn.cursor()
        tsql = 'select * from sqlite_master where type="table" and name="%s"' % (
            conf.SETTING_INFO)
        cr.execute(tsql)
        rs = cr.fetchall()
        if len(rs) == 0:
            csql = "create table setting_info(row_separator varchar(10), col_separator varchar(10), output varchar(100))"
            cr.execute(csql)
            conn.commit()
            sql = 'insert into setting_info values("\n", "|", "stdout")'
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
                temp = '\n'
            ret.append(rs.keys()[i] + " : " + temp)
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
        
    def get_database_name(self, database, id):
        conn = sqlite3.connect(database)
        c = conn.cursor()
        sql = 'select database_name from data_pos_info where id="%s"' % (id)
        c.execute(sql)
        rs = c.fetchone()
        database_name = rs[0]
        conn.close()
        return database_name
#        return '%sdata.db' % (conf.DATABASE_DIR)

    def get_active_db(self, database, node):
        conn = sqlite3.connect(database)
        c = conn.cursor()
        sql = 'select db_name from sub_db_info where node="%s" and status = %s' % (node, 1)
        c.execute(sql)
        rs = c.fetchone()
        database_name = rs[0]
        conn.close()
        return database_name

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
        key = rs[0]
        conn.close()
        return key

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
    def insert_table_info(self, database, table, column, nodes, hash_key):
        conn = sqlite3.connect(database)
        cr = conn.cursor()
        # first insert table_attr_info
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
        # insert data_node_info and sub_db_info
        i = 0
        cr.execute('delete from data_node_info where table_name = "%s"' % (table))
        conn.commit()
        for n in nodes:
            sql = 'insert into data_node_info values("%s", "%s")' % (table, n)
            cr.execute(sql)
            tt = cr.execute('select * from sub_db_info where node="%s" and status=%s' % (n, 1)).fetchall()
            if len(tt) == 0:
                db_name = "%s_%s_%s_0" % (database, n, getCurrentTime())
                sql = 'insert into sub_db_info values("%s", "%s", %s, %s)' % (n, db_name, 0, 1)
                cr.execute(sql)
            i += 1
        conn.commit()
        
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
        for row in rs:
            node = row[0]
            db_name = row[1]
            sql = 'insert into table_pos_info values("%s", "%s", "%s", 0)' % (table, node, db_name)
            cr.execute(sql)
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

    def update_db_info(self, database, dbs):
        conn = sqlite3.connect(metadata_db)
        cr = conn.cursor()
        total_size = 0   # the total increased size of table 
        total_record_num = 0
        for i in range(1, len(dbs)):
            cur_db = dbs[i]
            dbname = cur_db[conf.DATABASE]
            node = cur_db[conf.NODE]
            i = dbname[dbname.rfind("_")+1:]
            new_db_name = "%s_%s_%s_%s" % (metadata_db, node, getCurrentTime(), i)
            table = cur_db[conf.TABLE]
            status = cur_db[conf.STATUS]
            db_size = cur_db[conf.DB_SIZE]
            added_record = cur_db[conf.ADDED_RECORD]
            added_size = cur_db[conf.ADDED_SIZE]
            total_size += added_size
            total_record_num += added_record
            insert_sql = 'insert into sub_db_info values("%s", "%s", %d, %d)' % (node, new_db_name , db_size, status)
            cr.execute(insert_sql)
            insert_sql = 'insert into table_pos_info values("%s", "%s", "%s", %s)' % (table, node, dbname, db_size)
            cr.execute(insert_sql)
            conn.commit()

        cur_db = dbs[0]
        dbname = cur_db[conf.DATABASE]
        status = cur_db[conf.STATUS]
        node = cur_db[conf.NODE]
        table = cur_db[conf.TABLE]
        db_size = cur_db[conf.DB_SIZE]
        added_record = cur_db[conf.ADDED_RECORD]
        added_size = cur_db[conf.ADDED_SIZE]
        total_size += added_size
        total_record_num += added_record
        update_sql = 'update sub_db_info set db_size = %s, status = %s where node = "%s" and db_name = "%s"' % (db_size, status, node, dbname)
        cr.execute(update_sql)
        sql = 'select size from table_pos_info where name = "%s" and node ="%s" and db = "%s"' % (table, node, dbname)
        cr.execute(sql)
        rs = cr.fetchall()
        current_size = rs[0][0]
        update_sql = 'update table_pos_info set size=%s where name = "%s" and node ="%s" and db = "%s"' % (current_size + added_size, table, node, dbname)
        cr.execute(update_sql)
        conn.commit()

        sql = 'select size,record_num from table_size_info where name = "%s"' % (table)
        cr.execute(sql)
        rs = cr.fetchall()
        cu_size = rs[0][0]
        re_num = rs[0][1]
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
    
class special_task:
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

class task:

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
        self.worker_nodes = {}
        self.data_nodes = {}
        self.clients = []
        self.plan = logical_tree()
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
        self.tag = None   # to identify the query with "SELECT", "CREATE", "DROP", ...
        self.is_data_node_done = False

    def show(self):
        Ws(self.id+'\t'+self.query+'\t'+self.database + '\n')

class worker_node:
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

class data_node:
    def __init__(self):
        self.id = None   # the port randomly dispatched by task_manager
        self.name = None
        self.port = 0    
        self.t_size = 0 # total size of data
        self.l_size = 0 # size of left data
        self.b_size = 0 # block size that send to worker at once
        self.has_client = False
        self.clients = []
        self.ECT = 0

class exception:
    def __init__(self):
        self.QUERY_PARSE_EXCEPTION = "ERROR: Exceptions in parsing collective query"
        self.PLAN_MAKE_EXCEPTION = "ERROR: Exceptions in making execution plan"
        self.QUERY_SYNTAX_PARSE_EXCEPTION = "ERROR: Exceptions in parsing SQL syntaxly"
        self.USELESS_QUERY = "ERROR: All data is distributed to udx workers, so this query is actually useless and ParaLite ignores it."
        self.NO_UDX_DEF = "ERROR: You used User-Defined Executable, please define them first"
        self.PARAMETER_ERROR = "ERROR: Please specify right parameters"

class dloader:
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
        self.socks = []
        self.row_sep = None # the row separator of data, NULL -- if it is \n or None
        self.userconfig = userconfig
    """
    # if all clients are finished (all data on clients are dispatched), send END
    _TAG to all workers
    # if all workers are finished (all db are changed), set the dloader instance 
    None.
    """
    def proc_req_msg(self, node, size, sock):
        global client_num
        self.socks.append(sock)
        if size == 0:
            msg = str(client_num)
            client_num += 1
            sock.send("%10s%s" % (len(msg), msg))
            return 
        if self.fashion == conf.ROUND_ROBIN_FASHION:
            """
            if self.tag == 0 or self.row_sep != "NULL":
                rs = []
                s = size/len(self.n_info) + 1
                for n in self.p_info:
                    m = "%s%s%s%s%s%s%s" % (n,conf.SEP_IN_MSG, self.p_info[n], conf.SEP_IN_MSG, s, conf.SEP_IN_MSG, len(self.n_info))
                    rs.append(m)
            else:
                rs = self.schedule_data(node, size)
            """
            rs = []
            s = size/len(self.n_info) + 1
            for n in self.p_info:
                m = "%s%s%s%s%s%s%s" % (n,conf.SEP_IN_MSG, self.p_info[n],
                                        conf.SEP_IN_MSG, s, conf.SEP_IN_MSG,
                                        len(self.n_info))
                rs.append(m)
        elif self.fashion == conf.REPLICATE_FASHION:
            rs = []
            for n in self.p_info:
                m = "%s%s%s" % (n, conf.SEP_IN_MSG,self.p_info[n])
                rs.append(m)
        elif self.fashion == conf.HASH_FASHION:
            rs = []
            for n in self.p_info:
                m = "%s%s%s" % (n, conf.SEP_IN_MSG,self.p_info[n])
                rs.append(m)
        elif self.fashion == conf.RANGE_FASHION:
            rs.append("RANGE_FASHION is not supportted now...")
        self.n_client[node] -= 1
        rs.append("%s" % (client_num))
        client_num += 1
        msg = ",".join(rs)
        sock.send("%10s%s" % (len(msg), msg))

    def proc_dbm_msg(self, node, database, se_db):
        self.count_dbm += 1
        self.update_metadata(database, se_db)

    def update_metadata(self, database, se_db):
        b = base64.decodestring(se_db)
        dbs = cPickle.loads(b)
        info_center().update_db_info(database, dbs)

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
        for sock in self.socks:
            sock.send("%10s%s" % (len(conf.END_TAG), conf.END_TAG))
            
    def start_all_server(self):
        path = WORKING_DIR
        name = '%s%s.py' % (path, "dload_server")
        port = get_unique_num(10000, 20000)
        dic = {}
        dic[conf.CQID] = self.cqid
        dic[conf.MASTER_NAME] = master_node
        dic[conf.MASTER_PORT] = master_port
        dic[conf.OUTPUT_DIR] = info_center().get_output(self.database)
        dic[conf.TABLE] = self.table
        for node in self.n_info:
            # give the initial db info to workers
            rs = info_center().get_db_info(self.database, node, 1)
            db_name = rs.split(":")[0]
            db_size = rs.split(":")[1]
            dic[conf.DATABASE] = db_name
            dic[conf.DB_SIZE] = db_size
            dic[conf.PORT] = port
            dic[conf.TEMP_DIR] = self.userconfig.temp_dir
            dic[conf.LOG_DIR] = self.userconfig.log_dir
            s = cPickle.dumps(dic)
            b = base64.encodestring(s)
            s_in_a_line = string.replace(b, '\n', '*')
            cmd = "gxpc e -h %s python %s %s" % (node, name, s_in_a_line)
            #cmd = "ssh %s \"%s\"" % (node, cmd)
            pipes = [(None, "w", 0), (None, "r", 1), (None, "r", 2)]
            pid, r_chans, w_chans, err = self.iom.create_process(cmd.split(),
                                                                 None, None,
                                                                 pipes, None)
            if err:
                Es("ERROR: dloader is started failed: %s" % (err))
            for ch in r_chans:
                ch.flag = m_paraLite().PROCESS_OUT
                ch.buf = cStringIO.StringIO()
            proc = my_process(pid, "dloader", node, r_chans, w_chans, err)
            self.process[pid] = proc
        
            """
            addr = (node, self.remote_node_port_map[node])
            sock = socket(AF_INET, SOCK_STREAM)
            sock.connect(addr)
            sock.send("%10s%s" % (len(cmd), cmd))
            sock.close()
            """
        logging("dloaders are started")
        
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
        
class m_paraLite():

    CMD_EN_ERROR = -2
    CMD_RET_SUC = -1

    DATA_NODE = 'datanode'
    WORKER_NODE = 'workernode'

    REG = 'REG'
    ACK = 'ACK'
    
    def __init__(self):
        self.iom = ioman.mk_ioman()
        self.is_running = True
        self.userconfig = user_config()
        self.init_user_config()
        self.task_manager = task_manager(self.iom, self.userconfig)
        self.dloader = None
        self.data_parallel = None
        self.output = None
        self.eventqueue = Queue.Queue()
        self.SOCKET_OUT = 1
        self.PROCESS_OUT = 0
        self.EXIT = 2

    def init_user_config(self):
        s = string.replace(sys.argv[5], '*', '\n')
        b1 = base64.decodestring(s)
        dic = cPickle.loads(b1)
        for key in dic.keys():
            value = dic[key]
            if hasattr(self.userconfig, key):
                setattr(self.userconfig, key, value)

    def next_event(self, t):
        length = 0
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
            elif ev.ch.flag == self.SOCKET_OUT:
                # the first 10 bytes is the length of the mssage
                if length == 0:
                    length = string.atoi(ev.data[0:10])
                if len(buf.getvalue()) >= length + 10:
                    all_data = buf.getvalue()
                    data_to_return = all_data[0:length+10]
                    buf.truncate(0)
                    buf.write(all_data[length+10:])
                    length = 0
                    return ioman_base.event_read(ch, data_to_return, 0, ev.err)
            
    def start(self):
        global master_port, log
        try:
            ch = self.iom.create_server_socket(AF_INET, SOCK_STREAM, 1000,
                                               (master_node, master_port))
        except Exception, e:
            if e.args[0] == 98:
                Es(str(e.args[0]))
            return
        if not os.path.exists(self.userconfig.temp_dir):
            os.makedirs(self.userconfig.temp_dir)
        log = open("%s/master-%s-%s.log" % (self.userconfig.log_dir,
                                            gethostname(),getCurrentTime()), "wb")
        addr = ch.ss.getsockname()
        logging("ParaLite is listening on port %s ..." % (repr(addr)))
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
        sock.connect((public_node, public_port))
        sock.send("%10s%s" % (len(msg), msg))
        sock.close()
        logging("sending master ready info or port back to the client")
        """
        now, it starts to wait for any event and process them
        """
        handler = Thread(target=self.event_recv)
        handler.start()
        try:
            self.handle_read()
        except Exception, e:
            self.report_error_to_clients(" ".join(str(s) for s in e.args))
        finally:
            # notify event_recv by sending a message
            sock = socket(AF_INET, SOCK_STREAM)
            sock.connect((master_node, master_port))
            logging("sending exit")
            msg = "exit"
            sock.send("%10s%s" % (len(msg),msg))
            sock.close()
        logging("The main thread is finished...")
        handler.join()
        logging("The child thread is finished...")
        
    def event_recv(self):
        while self.is_running:
            try:
                ev = self.next_event(None)
            except Exception, e:
                Es("ERROR in next_event : %s\n" % (" ".join(str(s) for s in e.args)))
            if isinstance(ev, ioman_base.event_accept):
                try:
                    self.handle_accept(ev)
                except Exception, e:
                    Es("ERROR in handle_accept: %s\n" % (e.args[1]))
                    return
            elif isinstance(ev, ioman_base.event_read):
                if ev.data != "":
                    if ev.data[10:14] == "exit":
                        break
                    self.eventqueue.put(ev)
            elif isinstance(ev, ioman_base.event_death):
                try:
                    self.handle_death(ev)
                except Exception, e:
                    Es("ERROR in handle_death: %s\n" % (" ".join(str(s) for s
                                                                 in e.args)))
                    return
        

    def handle_read(self):
        while self.is_running:
            try:
                event = self.eventqueue.get()
                flag = event.ch.flag
                if flag == self.PROCESS_OUT:
                    self.handle_read_from_process(event)
                elif flag == self.SOCKET_OUT:
                    self.handle_read_from_socket(event)
            except Exception, e:
                logging(traceback.format_exc())
                raise(Exception("ERROR in handle_read: %s\n" % (e.args[0])))


        sock = socket(AF_INET, SOCK_STREAM)
        sock.connect((master_node, master_port))
        msg = "exit"
        sock.send("%10s%s" % (len(msg),msg))
        sock.close()
        
    def handle_death(self, event):
        pid = event.pid
        status = event.status
        err = event.err
        proc = self.task_manager.process.pop(pid)
        if status != 0:
            Es("process %s failed on %s: %s\n" % (proc.tag, proc.node, err))
        else:
            proc.status = 0
        
    def handle_accept(self, event):
        event.new_ch.flag = self.SOCKET_OUT
        event.new_ch.buf = cStringIO.StringIO()

    def handle_read_from_process(self, event):
        logging("receive data from process")
        self.report_error_to_clients(event.data)
        logging(event.data)
        self.safe_kill_master()
        """
        raise(Exception(event.data))
        if event.data.startswith("ERROR"):
            logging(event.data)
            raise(Exception(event.data))
        """
        
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
                logging("MESSAGE: %s" % (message))
                self.safe_kill_master()
                self.is_running = False
                return
            elif message == conf.DETECT_INFO:
                return
            elif message.startswith("ERROR"):
                logging("receive an ERROR info from socket of child process: ")
                logging(message)
                self.safe_kill_master()
                self.report_error_to_clients(message)
                self.is_running = False
                return 
            mt = message.split(conf.SEP_IN_MSG)[0]
            if mt == conf.REG:
                t = message.split(conf.SEP_IN_MSG)[1]
                if t == conf.DATA_NODE:
                    logging(message)
                    self.reg_new_datanode(message)
                elif t == conf.WORKER_NODE:
                    self.reg_new_workernode(message)
                elif t == conf.CLIENT:
                    self.reg_new_client(message)
                elif t == conf.DLOADER_SERVER:
                    logging(message)                    
                    self.reg_new_dloader_server(message)
            elif mt == conf.ACK:
                logging(message)
                t = message.split(conf.SEP_IN_MSG)[1]
                if t == conf.DATA_NODE:
                    if self.process_ack_datanode(message) == 1:
                        logging("----This query is finished----")
                        self.is_running = False
                elif t == self.WORKER_NODE:
                    if self.process_ack_client(message) == 1:
                        logging("----This query is finished----")
                        self.is_running = False
            elif mt == conf.KAL:
                logging(message)
                self.process_kal_client(message)
            elif mt == conf.REQ:
                logging(message)
                m = message.split(conf.SEP_IN_MSG)
                #REQ:END_TAG:NODE:CLIENT_ID
                if len(m) == 4 and m[1] == conf.END_TAG:
                    self.dloader.count_req += 1
                    if self.dloader.is_client_finished():
                        time.sleep(2)
                        self.dloader.notify_dload_server()
                        logging("notify all dload server")
                    return
                #REQ:cqid:NODE:DATABASE:TABLE:DATA_SIZE:TAG:FASHION:ROW_SEP:client_id
                cqid = m[1]
                node = m[2]
                database = m[3]
                table = m[4]
                data_size = m[5]
                tag = string.atoi(m[6])
                node_info = info_center().get_data_node_info(database, table)
                if self.dloader is None:
                    self.dloader = dloader(self.iom, self.userconfig)
                    self.dloader.tag = tag
                    self.dloader.fashion = m[7]
                    self.dloader.n_info = node_info
                    self.dloader.table = table
                    self.dloader.cqid = cqid
                    self.dloader.database = database
                    self.dloader.row_sep = m[8] #"NULL" or Other
                    if tag == 1:
                        task = self.task_manager.taskqueue[cqid]
                        self.dloader.client = task.plan.node
                    else:
                        self.dloader.client[0] = node
                    self.dloader.start()
                if self.dloader.all_server_started():
                    self.process_load_request(node, data_size, event.ch.so)
                else:
                    self.dloader.request.append((node, data_size, event.ch.so))
            elif mt == conf.DBM:
                if self.process_db_modified(message) == 1:
                    self.is_running = False
            elif mt == conf.QUE:
                self.process_db_query(message, event.ch.so)
            else:
                Ws('we can not support this registeration\n')
                return
        except Exception, e:
            logging(traceback.format_exc())
            raise e

    def report_error_to_clients(self, message):
        q = self.task_manager.taskqueue
        task = q[q.keys()[0]]
        for addr in task.reply_sock:
            try:
                sock = socket(AF_INET,SOCK_STREAM)
                sock.connect(addr)
                sock.send("%10s%s" % (len(message), message))
                sock.close()
            except:
                pass

    def reg_new_dloader_server(self, message):
        #REG:DLOADER_SERVER:cqid:node:port:local_socket
        assert self.dloader is not None
        m = message.split(conf.SEP_IN_MSG)
        self.dloader.p_info[m[3]] = "%s%s%s" % (m[4], conf.SEP_IN_MSG, m[5])
        if self.dloader.all_server_started():
            while self.dloader.request != []:
                re = self.dloader.request.pop()
                self.process_load_request(re[0], re[1], re[2])
            
    def reg_new_datanode(self, message):
        #REG:DATA_NODE:cqid:dnid:nodename:datasize
        flag = 1
        m = message.split(conf.SEP_IN_MSG)
        cqid = m[2]
        task = self.task_manager.get_task_from_queue(cqid)
        assert task is not None
        dn = data_node()
        dn.id = m[3]
        dn.name = m[4]
        dn.port = string.atoi(string.strip(m[5]))
        dn.t_size = string.atoi(m[6])
        dn.l_size = dn.t_size
        for key in task.worker_nodes:
            worker = task.worker_nodes[key]
            if worker.name == dn.name:
                dn.clients.append(worker)
        if len(dn.clients) != 0:
            dn.ECT = float(dn.l_size) / len(dn.clients)
            dn.has_client = True
        else:
            dn.ECT = sys.maxint

        worker_num = len(task.clients)
        """
        TODO: use more sophisticated method to decide block size
        """
        block_size = self.userconfig.block_size
        if block_size == 0:
            dn.b_size = dn.t_size/len(task.clients) + 1
        elif block_size == -1:
            dn.b_size = dn.t_size
        else:
            dn.b_size = min(block_size, dn.t_size)
        
        task.data_nodes[dn.id] = dn
        logging("DATA_NODE NUM: *******%s*******" % (len(task.data_nodes)))
        self.data_parallel.distribute_data(dn)

    def safe_kill_master(self):
        logging("start to kill all processes safely...")
        # check all threads terminated or not
        """
        for p in self.task_manager.process:
            proc = self.task_manager.process[p]
            logging("ERROR: %s is still running on %s" % (proc.tag, proc.node))
            cmd = "killall %s" % proc.tag
            os.system("gxpc e -h %s %s" % (proc.node, cmd))
                
        if self.dloader is None:
            return
        """
        udx_flag = 0
        sql_flag = 0
        tq = self.task_manager.taskqueue
        t = tq[tq.keys()[0]]
        self.notify_all_clients(t)
        self.notify_all_datanodes(t)
        if self.dloader is not None:
            self.dloader.notify_dload_client()
            self.dloader.notify_dload_server()

        """
        for p in self.dloader.process:
            proc = self.dloader.process[p]
            logging("ERROR: %s is still running on %s" % (proc.tag, proc.node))
            if proc.tag == "udx" and udx_flag == 0:

                udx_flag = 1
            if proc.tag == "sql" and 
            #os.system("gxpc e killall %s" % (proc.tag))
        """
            
    def add_new_client(self, t, so, c):
        t.reply_sock.append(so)
        t.clients.append(c)
        # get the UDX node in the logical tree
        queue = deque([])
        queue.append(t.plan)
        while list(queue) != []:
            op = queue.popleft()
            if op.name == logical_operator().UDX:
                udx_op = op
                break
            for child in op.children:
                queue.append(child)
        assert udx_op is not None
        # get necessary args and start the UDX process
        op.node[op.worker_num] = c
        opid = "%s_%s" % (op.id, op.worker_num)
        args = self.task_manager.mk_job_args(t, op, op.worker_num, opid)
        op.worker_num += 1
        path = WORKING_DIR
        name = "%s%s.py" % (path, op.name)
        cmd = "gxpc e -h %s python %s %s" % (c, name, args)
        pipes = [ (None, "w", 0), (None, "r", 1), (None, "r", 2) ]
        x = self.iom.create_process(cmd.split(), None, None, pipes, None)
        pid,r_chans,w_chans,err = x
        for ch in r_chans:
            ch.flag = m_paraLite().PROCESS_OUT
            ch.buf = cStringIO.StringIO()
        #logging("pid = %s proc = %s" % (pid, op.name))
        # sleep a random time
        #time.sleep(random.random())
        if pid is None:
            Ws("failed to create process %s\n" % cmd)
            """
            TODO: if the process is failed, how to do?
            """
        proc = my_process(pid, op.name, c, r_chans, w_chans, err)
        self.task_manager.process[pid] = proc
        
    def reg_new_client(self, message):
        #REG:CLIENT:database:collective_query:nodename:port
        self.check_metadata_table(metadata_db)
        t, clientname = self.parse_query_args(message)
        if t:
            reply_so = (message.split(conf.SEP_IN_MSG)[4],
                        string.atoi(message.split(conf.SEP_IN_MSG)[5]))
            if self.output is None:
                self.output = info_center().get_output(metadata_db)
            msg = "%s%s%s" % (conf.OUTPUT_INFO, conf.SEP_IN_MSG, self.output)
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
                    logging("this query is useless")
                else:
                    self.add_new_client(old_task, reply_so, clientname)
            else:
                logging("this is a new task, we added it to the queue")
                self.task_manager.put_task_to_queue(t, clientname, reply_so)
                if isinstance(t, task):
                    self.data_parallel = data_paralleler(t)
                    self.task_manager.execute()
                else:
                    self.task_manager.execute()
                    for addr in t.reply_sock:
                        sock = socket(AF_INET,SOCK_STREAM)
                        sock.connect(addr)
                        sock.send("%10sOK" % (2))
                        sock.close()
                    self.is_running = False
                    logging("TASK: %s --> %s: FINISH" % (t.id, t.cmd))
        else:
            ex = exception().QUERY_PARSE_EXCEPTION
            sock = socket(AF_INET, SOCK_STREAM)
            sock.connect(reply_so)
            sock.send("%10s%s" % (len(ex), ex))
            sock.send("%10sOK" % (2))
            sock.close()
        
    def reg_new_workernode(self, message):
        #REG:WORKER_NODE:cqid:opid:id:nodename:port:local_addr:state
        m = message.split(conf.SEP_IN_MSG)
        cqid = m[2]
        task = self.task_manager.get_task_from_queue(cqid)
        assert task is not None
        wnode = worker_node()
        wnode.id = m[4]
        wnode.cqid = cqid
        wnode.opid = m[3]
        wnode.name = m[5]
        wnode.status = m[8]
        wnode.port = string.atoi(m[6])
        wnode.local_addr = m[7]
        task.worker_nodes[wnode.id] = wnode
        logging("WORKER_NODE NUM: ----%s-----" % (len(task.worker_nodes)))
        # sometimes a workernode is late, when it issue the query to the master, there
        # are some data left. So the master starts a process for this udx. However,
        # sometimes the registration of this worker node happens after all data is
        # distributed. In this case, this worker node cannot receive the notifciation
        # from the master and keep running for ever.
        if task.is_data_node_done == True:
            addr = (wnode.name, wnode.port)
            try:
                logging("notify all udx client-----%s" % (wnode.id))
                so = socket(AF_INET, SOCK_STREAM)
                so.connect(addr)
                so.send('%10sEND' % (3))
                so.close()
            except Exception, e:
                if e.errno == 4:
                    logging("notify all client-----%s" % (wnode.id))
                    so = socket(AF_INET, SOCK_STREAM)
                    so.connect(addr)
                    so.send('%10sEND' % (3))
                    so.close()
        # TODO: if a client join in during the computation, the data_node
        # should be updated.
        if self.data_parallel.all_data_node_done():
            task.status = task.DATA_PARALLEL_END
        else:
            self.data_parallel.distribute_data(wnode)

    def process_db_query(self, message, client):
        #QUE:type:database:table (1) QUE:type:database,table 
        cmd_type = message.split(conf.SEP_IN_MSG)[1]
        database = message.split(conf.SEP_IN_MSG)[2]
        table = message.split(conf.SEP_IN_MSG)[3]
        if cmd_type == "TABLE_COLUMN":
            columns = info_center().get_table_columns(database,table)
            s = conf.SEP_IN_MSG.join(columns)
            client.send("%10s%s" % (len(s), s))
        elif cmd_type == "SEPARATOR":
            sep = info_center().get_separator(database)
            s = conf.SEP_IN_MSG.join(sep)
            client.send("%10s%s" % (len(s), s))
        elif cmd_type == "PARTITION_KEY":
            key = info_center().get_table_partition_key(database, table)
            client.send("%10s%s" % (len(key), key))

    def process_kal_client(self, message):
        #KAL:cqid:id:status:speed:time
        m = message.split(conf.SEP_IN_MSG)
        cid = m[1]
        wid = m[2]
        status = m[3]
        speed = float(m[4])
        time = float(m[5])
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
            logging("END --> WORKER %s" % (worker.id))
        else:
            self.data_parallel.distribute_data(worker)
        """
        if not self.data_parallel.all_data_node_done():
            self.data_parallel.distribute_data(worker)
            
    def process_ack_client(self, message):
        #ACK:WORKER_NODE:cqid:id:nodename:state:costtime
        m = message.split(conf.SEP_IN_MSG)
        cqid = m[2]
        task = self.task_manager.taskqueue[cqid]
        task.succ_worker_num += 1
        logging("WORKER_NODE ACK NUM: --------%s-------" % (task.succ_worker_num))
        if task.succ_worker_num >= task.used_worker_num:
            for addr in task.reply_sock:
                try:
                    logging("CLIENT: %s" % (repr(addr)))
                    sock = socket(AF_INET, SOCK_STREAM)
                    sock.connect(addr)
                    sock.send("%10sOK" % (2))
                    sock.close()
                except Exception, e:
                    if e.errno == 4:
                        logging("CLIENT: %s" % (repr(addr)))
                        sock = socket(AF_INET, SOCK_STREAM)
                        sock.connect(addr)
                        sock.send("%10sOK" % (2))
                        sock.close()
            task.e_time = time.time()*1000
            costtime = task.e_time-task.s_time
            logging("task is finished")
            logging('------task %s cost %d ms------' % (task.id, costtime))
            self.task_manager.remove_task_from_queue(cqid)
            return 1
        return 0
    
    def process_load_request(self, node, data_size, sock):
        self.dloader.proc_req_msg(node, string.atoi(data_size), sock)
        
    def process_db_modified(self, message):
        #DBM:NODE:database:serialized_db_instance
        m = message.split(conf.SEP_IN_MSG)
        node = m[1]
        se_db = m[3]
        database = m[2]
        self.dloader.proc_dbm_msg(node, database, se_db)
        if self.dloader.is_worker_finished():
            self.dloader.notify_dload_client()
            logging('-------------import data succussfully-----------')
            tag = self.dloader.tag
            self.dloader = None
            if tag == 0:
                return 1
        return 0

    def process_ack_datanode(self, message):
        #ACK:DATA_NODE:cqid:hostname
        # tag = 0 : there is no data selected
        m = message.split(conf.SEP_IN_MSG)
        cid = m[2]
        task = self.task_manager.taskqueue[cid]
        task.succ_datanode_num += 1
        logging("DATA_NODE ACK NUM:------%s-------" % (task.succ_datanode_num))
        if task.plan.name == logical_operator().UDX:
            worker_num = task.plan.children[0].worker_num
        else:
            worker_num = task.plan.worker_num
        if task.succ_datanode_num == worker_num:
            if task.tag == "DROP":
                info_center().delete_table_info(task.database, task.table)
            # notify all clients to close socket server
            task.is_data_node_done = True
            self.notify_all_clients(task)
            logging("notify all clients")
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

    def notify_all_datanodes(self, task):
        for key in task.data_nodes:
            try:
                data_node = task.data_nodes[key]
                sock = socket(AF_INET, SOCK_STREAM)
                sock.connect((data_node.name, data_node.port))
                sock.send('%10s%s' % (len(conf.END_TAG), conf.END_TAG))
                sock.close()
            except Exception, e:
                pass

    def notify_all_clients(self, task):
        try:
            for key in task.worker_nodes:
                worker = task.worker_nodes[key]
                addr = (worker.name, worker.port)
                so = socket(AF_INET, SOCK_STREAM)
                try:
                    logging("notify all udx client-----%s" % (worker.id))
                    so.connect(addr)
                    so.send('%10sEND' % (3))
                    so.close()
                except Exception, e:
                    if e.errno == 4:
                        time.sleep(random.random())
                        logging("notify all client-----%s" % (worker.id))
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
                spec_task = special_task()
                spec_task.database = database
                spec_task.cmd = cq
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
                spec_task.id = str(get_unique_num(1, 2000))
                return spec_task, nodename
            else:
                gtask = task()
                m = cq.split("collective by")
                gtask.database = database
                gtask.query = m[0]
                if len(m) > 1:
                    gtask.id = string.strip(m[1])
                else: 
                    gtask.id = str(0-get_unique_num(1,1000))
                gtask.output_dir = info_center().get_output(database)
                return gtask, nodename
        except Exception, e:
            traceback.print_exc()
            return None, None

    def do_analyze_cmd(self, database, query):
        t = task()
        t.database = database
        t.query = query
        mk_logical_plan(t).make()
        
    def do_query_cmd(self, database, cq):
        if cq.lower().startswith("select"):
            c = client(database, cq, 1)
        else:
            c = client(database, cq, 0)
        c.proc()

    def check_metadata_table(self, database):
        info_center().create_metadata_table(database)

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
    """
    dload = dloader(None)
    for i in range(62, 94):
        node = "cloko%s" % string.zfill(str(i),3)
        dload.n_info.append(node)
        dload.p_info[node] = node
        dload.client[random.randint(1000*i + 1, 1000*i + 1000)] = node
        dload.client[random.randint(1000*i + 1, 1000*i + 1000)] = node
    dload.tag = 1
    dload.fashion = conf.ROUND_ROBIN_FASHION
    dload.row_sep = "NULL"
    dload.start()
    print dload.n_client
    print dload.proc_req_msg("cloko091", 63840, None)
    print dload.proc_req_msg("cloko068", 58098, None)
    print dload.proc_req_msg("cloko082", 75646, None)
    print dload.proc_req_msg("cloko067", 119755, None)
    print dload.proc_req_msg("cloko069", 106178, None)
    print dload.proc_req_msg("cloko079", 88624, None)
    print dload.proc_req_msg("cloko064", 68588, None)
    print dload.proc_req_msg("cloko067", 116450, None)
    print dload.proc_req_msg("cloko070", 94358, None)
    print dload.proc_req_msg("cloko065", 110307, None)
    print dload.proc_req_msg("cloko089", 95720, None)
    print dload.proc_req_msg("cloko084", 92421, None)
    print dload.proc_req_msg("cloko065", 93425, None)
    """
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
    
if __name__=="__main__":
    m_paraLite_ins = m_paraLite()
    try:
        m_paraLite_ins.start()
        
        """
        if os.path.exists(m_paraLite_ins.userconfig.temp_dir):
            os.system("rm -rf %s" % m_paraLite_ins.userconfig.temp_dir)
        """
    except KeyboardInterrupt, e:
        if log is not None:
            logging("KeyboardInterrupt")
        m_paraLite_ins.safe_kill_master()
    except Exception, e:
        traceback.print_exc()
    
    
    


    
