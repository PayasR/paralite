from socket import *
import os, sys, config, time, cPickle, base64, string, random, re, io, shlex
import cStringIO, dload_client, pwd, traceback
from subprocess import *
import cqueue
"""
This class is performing the function of group by operator.
input: column names from the input data file. e.g. [table1.attr1, table1.attr2, table2.attr1, ...]
output: column names to output data file. e.g. [table1.attr1, table2.attr1, ...]
split_key: the attribute that is grouped
func: the calculation based on group operator, e.g. count, sum, avg...
source: the source data file for this class
dest: the output files
"""

LOG_DIR = None
TEMP_DIR = None
log = None

def getCurrentTime():
    return time.strftime('%Y-%m-%d-%H-%M-%S',time.localtime(time.time()))

def logging(s):
    log.write("%s: %s\n" % (getCurrentTime(), s))
    log.flush()
    
arguments = sys.argv[1]
conf = config.config()
class group_by:
    def __init__(self):
        # Four tuple avg('test')--> (True=if avg will be output,
        #'test', 3=the pos in input, 4=the pos in output)
        self.avg = (False, None, -1, -1)
        self.count = (False, None, -1, -1)
        self.max = (False, None, -1, -1)
        self.min = (False, None, -1, -1)
        self.sum = (False, None, -1, -1)
        self.total = (False, None, -1, -1)
        self.group_key = (False, None, -1, -1)
        # these variables are appointed by master
        self.dest = None
        self.attrs = {}
        self.output = []
        self.input = []
        self.split_key = None
        self.name = None
        self.cqid = None
        self.id = None
        self.status = None
        self.cost_time = 0
        #self.my_port = None
        self.node = (None, 0)  # (node, port)
        #self.p_port = []
        self.p_node = {}       # {port, node}
        self.num_of_children = 0
        self.key = None # the grouped key
        self.client = (None, 0)
        self.master_name = None
        self.master_port = 0
        self.output_dir = None
        self.db_col_sep = None
        self.db_row_sep = None
        self.dest_db = None
        self.dest_table = None
        self.client_sock = None
        self.fashion = None
        self.hash_key = None
        self.hash_key_pos = 0
        """
        if the child of this operator is sql-node, the output should be
        "table.attr1, avg(table.attr2), sum..."
        if not, the output should be "table.attr1, table.attr2, ..."
        """
        self.is_child_sql = False

    def parse_args(self, msg):
        s = string.replace(msg, '*', '\n')
        b1 = base64.decodestring(s)
        dic = cPickle.loads(b1)
        self.cqid = dic[conf.CQID]
        self.id = dic[conf.OPID]
        self.key = dic[conf.KEY]
        self.dest = dic[conf.DEST]
        self.attrs = dic[conf.ATTRS]
        self.output = dic[conf.OUTPUT]
        if dic.has_key(conf.SPLIT_KEY) == 1: self.split_key = dic[conf.SPLIT_KEY]
#        if dic.has_key(conf.P_PORT): self.p_port = dic[conf.P_PORT]
        self.name = dic[conf.NAME]
        self.input = dic[conf.INPUT]
#        self.my_port = dic[conf.MY_PORT]
        self.num_of_children = dic[conf.NUM_OF_CHILDREN]
        self.is_child_sql = dic[conf.IS_CHILD_SQL]
        self.master_name = dic[conf.MASTER_NAME]
        self.master_port = string.atoi(dic[conf.MASTER_PORT])
        if dic.has_key(conf.DEST_CLIENT_NAME) == 1:
            self.client = (dic[conf.DEST_CLIENT_NAME], dic[conf.DEST_CLIENT_PORT])
        self.output_dir = dic[conf.OUTPUT_DIR]
        self.p_node = dic[conf.P_NODE]
        self.node = dic[conf.NODE]
        self.db_col_sep = str(dic[conf.DB_COL_SEP])
        self.db_row_sep = str(dic[conf.DB_ROW_SEP])
        if dic.has_key(conf.CLIENT_SOCK):
            self.client_sock = dic[conf.CLIENT_SOCK]
        if conf.DEST_DB in dic:
            self.dest_db = dic[conf.DEST_DB]
            self.dest_table = dic[conf.DEST_TABLE]
        if self.dest == conf.DATA_TO_DB:
            self.fashion = dic[conf.FASHION]
            if self.fashion == conf.HASH_FASHION:
                self.hash_key = dic[conf.HASH_KEY]
                self.hash_key_pos = dic[conf.HASH_KEY_POS]
        global LOG_DIR, TEMP_DIR
        LOG_DIR = dic[conf.LOG_DIR]
        TEMP_DIR = dic[conf.TEMP_DIR]

        
    def shell_sort(self, data_list, data):
        s1 = time.time()*1000
        key_pos = self.group_key[2]
        if self.attrs.has_key(self.key) == False:
            key_type = conf.STRING
        else:
            key_type = self.attrs[self.key]
        if key_type != conf.INT and key_type != conf.FLOAT:
            if self.db_col_sep == "|":
                cmd = "sort -k%d -t\|" % (key_pos + 1)
            else:
                cmd = "sort -k%d -t%s" % (key_pos + 1, str(self.db_col_sep))
            args = shlex.split(cmd)
        else:
            if self.db_col_sep == "|":            
                cmd = "sort -nk%d -t\|" % (key_pos + 1)
            else:
                cmd = "sort -nk%d -t%s" % (key_pos + 1, str(self.db_col_sep))
            args = shlex.split(cmd)
        p1 = Popen(args, stdin=PIPE, stdout=PIPE)
        for eachdata in data_list:
            p1.stdin.write(eachdata.data)
        p1.stdin.close()
        for line in p1.stdout:
            data.append(string.strip(line))
        e1 = time.time()*1000

    def quick_sort(self, data_list, data):
        c = cStringIO.StringIO()
        i = 0
        key_pos = self.group_key[2]
        if self.attrs.has_key(self.key) == False:
            key_type = conf.STRING
        else:
            key_type = self.attrs[self.key]
        if key_type != conf.INT or key_type != conf.FLOAT:
            para = "-s"
        else:
            para = "-n"            
        for data in data_list:
            did = string.atoi(data.id)
            c.write(data.data)
        data = c.getvalue().split(self.db_row_sep)
        # delete the last null element
        if data[len(data) - 1] == '':
            data.pop()
        self.i_quick_sort(data, 0, len(data) - 1, key_pos, para)
    
    def i_quick_sort(self, numbers, left, right, pos, para):
        sep = self.db_col_sep
        if para == '-n':
            if left< right:
                middle = numbers[(left+right)/2]
                i= left -1
                j = right + 1
                while True:
                    while True:
                        i += 1
                        d1 = string.atoi(numbers[i].split(sep)[pos])
                        d2 = string.atoi(middle.split(sep)[pos])
                        if not (d1 < d2 and i < right):
                            break
                    while True:
                        j -=1
                        d1 = string.atoi(numbers[j].split(sep)[pos])
                        d2 = string.atoi(middle.split(sep)[pos])
                        if not (d1 > d2 and j>0):
                            break
                    if i >= j:
                        break
                    self.swap(numbers, i,j)
                self.i_quick_sort(numbers, left, i - 1, pos, para)
                self.i_quick_sort(numbers, j + 1, right, pos, para)
        elif para == '-s':
            if left < right:
                middle = numbers[(left+right)/2]
                i= left -1
                j = right + 1
                while True:
                    while True:
                        i += 1
                        d1 = numbers[i].split(sep)[pos]
                        d2 = middle.split(sep)[pos]
                        if not (d1 < d2 and i < right):
                            break
                    while True:
                        j -=1
                        d1 = numbers[j].split(sep)[pos]
                        d2 = middle.split(sep)[pos]
                        if not (d1 > d2 and j>0):
                            break
                    if i >= j:
                        break
                    self.swap(numbers, i,j)
                self.i_quick_sort(numbers, left, i - 1, pos, para)
                self.i_quick_sort(numbers, j + 1, right, pos, para)
            
    def swap(self, numbers, i, j):
        temp = numbers[i]
        numbers[i] = numbers[j]
        numbers[j] = temp
        
    """
    data_list is a list that each element is also a list contain data,
    the first line is the attributes
    the second line is the string of data which will be changed to a list
    """
    def sort_base_proc(self, data_list):
        logging("proc data START")
        start = time.time()*1000
        flag = 0
        output_tuple={}
        for i in range(len(self.output)):
            output_tuple[i] = []
        if self.parse_func():
            return
        data = []
        if len(self.db_col_sep) == 1 and self.db_row_sep == "\n":
            logging("sort data: shell")
            self.shell_sort(data_list, data)
        else:
            logging("sort data: quick sort")
            self.quick_sort(data_list, data)
        logging("sort data FINISH : record # = %s" % (len(data)))
        if self.sum[0]:
            key_pos = self.group_key[3]
            out_pos = self.sum[3]
            in_pos = self.sum[2]
            old_word = ''
            for i in range(0, len(data)):
                record = data[i]
                words = record.split(self.db_col_sep)
                new_word = words[key_pos]
                if old_word=='' or new_word!=old_word:
                    if old_word!= '':
                        output_tuple[key_pos].append(old_word)
                        output_tuple[out_pos].append(str(func_value))
                    func_value = float(words[in_pos])
                    old_word = new_word
                else:
                    func_value += float(words[in_pos])
            output_tuple[key_pos].append(old_word)
            output_tuple[out_pos].append(str(func_value))
            flag = 1
        if self.avg[0]:
            logging("AGGREGATE function: avg")            
            key_pos = self.group_key[3]
            out_pos = self.avg[3]
            in_pos = self.avg[2]
            old_word = ''
            for i in range(0, len(data)):
                record = data[i]
                words = record.split(self.db_col_sep)
                new_word = words[key_pos]
                if old_word=='' or new_word!=old_word:
                    if old_word!= '':
                        if not flag:
                            output_tuple[key_pos].append(old_word)
                        output_tuple[out_pos].append(func_value/float(row_count))
                    row_count = 1
                    func_value = float(words[in_pos])
                    old_word = new_word
                else:
                    row_count += 1
                    func_value += float(words[in_pos])
            if not flag:
                output_tuple[key_pos].append(old_word)
            output_tuple[out_pos].append(str(func_value/float(row_count)))
            flag = 1
        if self.count[0]:
            logging("AGGREGATE function: count")
            out_pos = self.count[3]
            key_pos = self.group_key[3]
            in_pos = self.count[2]
            row_count = 0
            if in_pos == -1:
                if self.count[1] == '*':
                    old_word = ''
                    for i in range(0, len(data)):
                        record = data[i]
                        words = record.split(self.db_col_sep)
                        new_word = words[key_pos]
                        if old_word=='' or new_word!=old_word:
                            if old_word!= '':
                                if not flag:
                                    output_tuple[key_pos].append(old_word)
                                output_tuple[out_pos].append(row_count)
                            row_count = 1
                            old_word = new_word
                        else:
                            row_count += 1
                else:
                    _pos = self.input.index(self.count[1])
                    old_word = ''
                    for i in range(0, len(data)):
                        record = data[i]
                        words = record.split(self.db_col_sep)
                        new_word = words[key_pos]
                        if old_word=='' or new_word!=old_word:
                            if old_word!= '':
                                if not flag:
                                    output_tuple[key_pos].append(old_word)
                                output_tuple[out_pos].append(row_count)
                            row_count = 1
                            old_word = new_word
                        else:
                            if words[_pos]:
                                row_count += 1
            else:
                old_word = ''
                logging(self.db_col_sep)
                for i in range(0, len(data)):
                    try:
                        record = data[i]
                        words = record.split(self.db_col_sep)
                        logging(len(words))
                        new_word = words[key_pos]
                        if old_word=='' or new_word!=old_word:
                            if old_word!= '':
                                if not flag:
                                    output_tuple[key_pos].append(old_word)
                                output_tuple[out_pos].append(row_count)
                            row_count = string.atoi(words[in_pos])
                            old_word = new_word
                        else:
                            row_count += string.atoi(words[in_pos])
                    except:
                        logging(traceback.format_exc())
                        return
            if not flag and old_word != "":
                output_tuple[key_pos].append(old_word)
            if row_count != 0:
                output_tuple[out_pos].append(str(row_count))
            flag = 1
        if self.max[0]:
            key_pos = self.group_key[3]
            out_pos = self.max[3]
            in_pos = self.max[2]
            old_word = ''
            for i in range(0, len(data)):
                record = data[i]
                words = record.split(self.db_col_sep)
                new_word = words[key_pos]
                if old_word=='' or new_word!=old_word:
                    if old_word!= '':
                        if not flag:
                            output_tuple[key_pos].append(old_word)
                        output_tuple[out_pos].append(max_value)
                    max_value = float(words[in_pos])
                    old_word = new_word
                else:
                    value = float(words[in_pos])
                    if value > max_value:
                        max_value = value
            if not flag:
                output_tuple[key_pos].append(old_word)
            output_tuple[out_pos].append(str(max_value))
            flag = 1
        if self.min[0]:
            key_pos = self.group_key[3]
            out_pos = self.min[3]
            in_pos = self.min[2]
            old_word = ''
            for i in range(0, len(data)):
                record = data[i]
                words = record.split(self.db_col_sep)
                new_word = words[key_pos]
                if old_word=='' or new_word!=old_word:
                    if old_word!= '':
                        if not flag:
                            output_tuple[key_pos].append(old_word)
                        output_tuple[out_pos].append(min_value)
                    min_value = float(words[in_pos])
                    old_word = new_word
                else:
                    value = float(words[in_pos])
                    if value < min_value:
                        min_value = value
            if not flag:
                output_tuple[key_pos].append(old_word)
            output_tuple[out_pos].append(str(min_value))
            flag = 1
        if self.total[0]:
            key_pos = self.group_key[3]
            out_pos = self.total[3]
            in_pos = self.total[2]
            old_word = ''
            for i in range(0, len(data)):
                record = data[i]
                words = record.split(self.db_col_sep)
                new_word = words[key_pos]
                if old_word=='' or new_word!=old_word:
                    if old_word!= '':
                        if not flag:
                            output_tuple[key_pos].append(old_word)
                        output_tuple[out_pos].append(func_value)
                    func_value = float(words[in_pos])
                    old_word = new_word
                else:
                    func_value += float(words[in_pos])
            if not flag:
                output_tuple[key_pos].append(old_word)
            output_tuple[out_pos].append(str(func_value))
            flag = 1
        result = cStringIO.StringIO()
        len_dic = len(output_tuple.keys())
        len_list = len(output_tuple[0])
        row_sep = self.db_row_sep
        if self.db_row_sep == "\n":
            row_sep = "\n"
        for i in range(len_list):
            s = cStringIO.StringIO()
            """
            for j in range(len_dic):
                s += '%s%s' % (output_tuple[j][i], self.db_col_sep)
            """
            for j in range(len_dic):
                s.write(str(output_tuple[j][i]))
                if j < len_dic - 1:
                    s.write(self.db_col_sep)
            result.write(s.getvalue())
            result.write("\n")
            s.close()
        self.status = conf.FINISH
        end = time.time()*1000
        self.cost_time = end-start
        return result

    def quick_sort(self, numbers, left, right, pos, para):
        sep = conf.SEPERATOR
        if para == '-n':
            if left< right:
                middle = numbers[(left+right)/2]
                i= left -1
                j = right + 1
                while True:
                    while True:
                        i += 1
                        d1 = string.atoi(numbers[i].split(sep)[pos])
                        d2 = string.atoi(middle.split(sep)[pos])
                        if not (d1 < d2 and i < right):
                            break
                    while True:
                        j -=1
                        d1 = numbers[j].split(sep)[pos]
                        d2 = middle.split(sep)[pos]
                        if not (d1 > d2 and j>0):
                            break
                    if i >= j:
                        break
                    self.swap(numbers, i,j)
                self.quick_sort(numbers, left, i - 1, pos, para)
                self.quick_sort(numbers, j + 1, right, pos, para)
        else:
            if left< right:
                middle = numbers[(left+right)/2]
                i= left -1
                j = right + 1
                while True:
                    while True:
                        i += 1
                        d1 = numbers[i].split(sep)[pos]
                        d2 = middle.split(sep)[pos]
                        if not (d1 < d2 and i < right):
                            break
                    while True:
                        j -=1
                        d1 = numbers[j].split(sep)[pos]
                        d2 = middle.split(sep)[pos]
                        if not (d1 > d2 and j>0):
                            break
                    if i >= j:
                        break
                    self.swap(numbers, i,j)
                self.quick_sort(numbers, left, i - 1, pos, para)
                self.quick_sort(numbers, j + 1, right, pos, para)
                
    def swap(self, numbers, i, j):
        temp = numbers[i]
        numbers[i] = numbers[j]
        numbers[j] = temp
    
    def parse_func(self):
        flag = 0
        out_pos = self.output.index(self.key) 
        in_pos = self.input.index(self.key)
        self.group_key = (True, self.key, in_pos, out_pos)
        for out in self.output:
            m = re.match("(.*)\((.*)\)", out)
            if not m:
                continue
            func_name = m.group(1)
            func_attr = m.group(2)
            out_pos = self.output.index(out)
            if func_name == 'count':
                """
                Count is special because it may has two different types of parameter.
                (1) * : if in_pos==-1: then it just needs to count the number of rows
                        else: in each row, there is already a colum to show the number
                              of current rows, it needs to count all these numbers 
                (2) attr: if in_pos == -1: it needs to count the number of rows that
                          the attr is not NULL
                          else: it is the same with (1)
                """
                if self.is_child_sql:
                    in_pos = self.input.index(out)
                else: 
                    in_pos = -1
                self.count = (True, func_attr, in_pos, out_pos)
            else:
                if self.is_child_sql:
                    in_pos = self.input.index(out)
                else: 
                    in_pos = self.input.index(func_attr)
                if func_name == 'avg':
                    self.avg = (True, func_attr, in_pos, out_pos)
                elif func_name == 'min':
                    self.min = (True, func_attr, in_pos, out_pos)
                elif func_name == 'max':
                    self.max = (True, func_attr, in_pos, out_pos)
                elif func_name == 'sum':
                    self.sum = (True, func_attr, in_pos, out_pos)
                elif func_name == 'total':
                    self.total = (True, func_attr, in_pos, out_pos)
                else:
                    print '%s is not supported' % (func_name)
                    flag = 1
                    break
        return flag            

    def distribute(self, data):
        sep = conf.SEPERATOR
        if self.dest == conf.DATA_TO_CLIENTS:
            # This is final result, store them to a file
            dest_file = conf.OUTPUT_DIR + self.dest
            file_handler = io.FileIO(dest_file, 'wb', closefd=True)
            bw = io.BufferedWriter(file_handler, buffer_size=65536)
            for row in data:
                bw.write(row)
                bw.write('\n')
            bw.close()
            addr = (conf.MASTER_NAME, string.atoi(conf.MASTER_PORT))
            sock = socket(AF_INET, SOCK_STREAM)
            sock.connect(addr)
            msg = '%s:%s:%s:%s' % (conf.ACK, conf.DATA_NODE, self.cqid, gethostname())
            sock.send('%10s%s' % (len(msg), msg))
        elif self.dest == conf.DATA_TO_ANO_OP:
            # This is intermediate data, pipeline it
            num_of_dest = len(self.p_node)
            if num_of_dest == 1:
                port = self.p_node.keys()[0]
                node = self.p_node[port]
                queue = cqueue.cqueue(node, port)
                queue.connect()
                # The first message is the output attrs of this sql
                msg = ''
                for out in self.output:
                    msg += '%s%s' % (out, sep)
                queue.put(msg)
                queue.put(data.getvalue())
                queue.close()
            elif num_of_dest > 1:
                queue_list = []
                """
                for p in self.p_port:
                    queue = cqueue(p.split(':')[0],string.atoi(p.split(':')[1]))
                    queue.connect()
                    # The first message is the output attrs of this sql
                    msg = ''
                    for out in self.output:
                        msg += '%s%s' % (out, sep)
                    queue.put(msg)
                    queue_list.append(queue)
                """
                for port in self.p_node:
                    queue = cqueue(self.p_node[port],port)
                    queue.connect()
                    # The first message is the output attrs of this sql
                    msg = ''
                    for out in self.output:
                        msg += '%s%s' % (out, sep)
                    queue.put(msg)
                    queue_list.append(queue)

                if self.split_key != None:
                    # partition data in hash fashion
                    split_key = self.split_key
                    if columns[0].find('.') == -1 and len(split_key.split('.')) == 2:
                        pos = columns.index(split_key.split('.')[1])
                    else:
                        pos = columns.index(split_key)
                    for row in rs:
                        partition_num = abs(hash(row[pos])) % num_of_dest
                        queue = queue_list[partition_num]
                        msg = ''
                        for r in data:
                            msg += '%s%s' % (str(r), sep)
                        queue.put(msg)
                else:
                    # partitioning data in range fashion
                    for i in range(len(data)):
                        partition_num = i % num_of_dest
                        queue = queue_list[partition_num]
                        msg = ''
                        for r in data:
                            msg += '%s%s' % (str(r), sep)
                        queue.put(msg)
                for queue in queue_list:
                    queue.close()
        elif self.dest == conf.DATA_TO_ONE_CLIENT:
            sock = socket(AF_INET, SOCK_STREAM)
            sock.connect(self.client)
            sock.send('%10sDATA' % (4))
            sock.send("%10s%s" % (len(data.getvalue()), data.getvalue()))
            sock.close()
            self.notify_to_master()
        elif self.dest == conf.DATA_TO_DB:
            """
            out_name = "%s%sgroup_%s" % (LOG_DIR, os.sep, self.cqid)
            f = open(out_name, "wb")
            f.write(data.getvalue())
            f.close()
            """
            col_sep = self.db_col_sep
            row_sep = self.db_row_sep
            master = (self.master_name, self.master_port)
            dload_client.dload_client().load_internal(master, self.cqid, gethostname(),self.dest_db, self.dest_table, data, 1, self.fashion, self.hash_key, self.hash_key_pos, col_sep, row_sep, col_sep, False, "0", LOG_DIR)
            self.notify_to_master()
            #os.remove(out_name)
        else:
            random_num = random.randint(0, len(self.client_sock) - 1)
            addr = self.client_sock[random_num]
            sock = socket(AF_INET, SOCK_STREAM)
            sock.connect(addr)
            d = string.strip(data.getvalue())
            sock.send("%10s%s" % (len(d), d))
            sock.close()
            self.notify_to_master()

    def notify_to_master(self):
        addr = (self.master_name, self.master_port)
        sock = socket(AF_INET, SOCK_STREAM)
        sock.connect(addr)
        sep = conf.SEP_IN_MSG
        msg = '%s%s%s%s%s%s%s' % (conf.ACK, sep, conf.DATA_NODE, sep, self.cqid, sep, gethostname())
        sock.send('%10s%s' % (len(msg), msg))
        
    def start(self):
        global LOG_DIR, log
        start = time.time()*1000
        self.parse_args(arguments)
        log = open("%s/groupby-%s-%s.log" % (LOG_DIR, gethostname(), getCurrentTime()), "wb")
        host = self.node[0]
        port = self.node[1]
        queue = cqueue.cqueue(host, port)
        queue.listen()
        """
        data_list = [data1, data2, data3...]
        data1: data1.id, data1.data(string)
        """
        data_list = queue.get(self.num_of_children)
        queue.close()
        logging("GET SOURCE DATA")
        proc_start = time.time()*1000
        result = self.sort_base_proc(data_list)
        logging("GET GROUPED DATA")
        proc_end = time.time()*1000
        self.distribute(result)
        dis_end = time.time()*1000
        logging("--------gourp node %s is finished--------" % (self.id))
        
def test():
    data_list = []
    data_list.append('5|sdf|sdflkj|')
    data_list.append('2|df|sdflkj|')
    data_list.append('1|sdf|sdflkj|')
    data_list.append('10|sdf|sdflkj|')
    data_list.append('17|sdf|sdflkj|')
    data_list.append('7|sdf|sdflkj|')
    data_list.append('4|sdf|sdflkj|')
    data_list.append('6|sdf|sdflkj|')
    group_by().sort(data_list, 0)
    
def main():
    proc = group_by()
    proc.start()

    
if __name__=="__main__":
#    test()
    main()
