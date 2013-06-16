from socket import *
import os
import sys
import time
import cPickle
import base64
import string
import random
import re
import shlex
import cStringIO
import traceback
import logging
from subprocess import *

# set the home direcotry of paraLite into system's path
sys.path.append(os.path.abspath(os.path.join(sys.path[0], os.path.pardir)))

import conf
import cqueue
import dload_client
from lib import newparser
from lib.logger import ParaLiteLog
from lib import ioman, ioman_base

def es(s):
    sys.stderr.write(s)
    sys.stderr.close()

def ws(s):
    sys.stdout.write(s)
    sys.stdout.close()

def get_max_size():
    """get the free memory size
    """
    p = Popen(shlex.split("free -m"), stdout=PIPE)
    stdout_list = p.communicate()[0].split('\n')
    s = string.atoi(stdout_list[1].split()[3].strip())*1024*1024
    return s

class GroupbyOp:
    
    MAX_SIZE = get_max_size()
    
    def __init__(self):
        self.expr = []         # store the structure of parsed expression
        self.my_group_key = (False, None, -1, -1)
        # these variables are appointed by master
        self.function = []   # ["count(*)", "sum(a)", "avg(a+b)",...]        
        self.dest = None
        self.attrs = {}
        self.output = []
        self.input = []
        self.split_key = None
        self.name = None
        self.cqid = None
        self.opid = None
        self.status = None
        self.cost_time = 0
        self.my_port = 0
        self.local_addr = None
        self.p_node = {}       # {port, node}
        self.num_of_children = 0
        self.group_key = [] # the grouped key
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
        self.is_checkpoint = None
        self.log_dir = None
        self.temp_dir = None
        self.limit = -1
        self.distinct = False
        """
        if the child of this operator is sql-node, the output should be
        table.attr1, avg(table.attr2), sum...
        
        if not, the output should be
        table.attr1, table.attr2, ...
        """
        self.is_child_sql = False

        self.iom = ioman.mk_ioman()
        self.is_running = True
        self.result = {}
        self.result_type = 0
        self.replica_result = {} # the result data for a failed node
        self.failed_node = []
        self.source_data = [] # store the data sending from children
        self.total_size = 0
        self.total_time = 0
        self.cur_jobid = None
        self.job_list = []  # store jobid
        self.partition_num = 0 

    def parse_args(self, msg):
        b1 = base64.decodestring(msg)
        dic = cPickle.loads(b1)

        for key in dic.keys():
            value = dic[key]
            if hasattr(self, key):
                setattr(self, key, value)
        if not os.path.exists(self.temp_dir): os.makedirs(self.temp_dir)        


    def parse_func(self):
        if self.is_child_sql:
            self.parse_func_2()
        else:
            self.parse_func_1()
        
    def parse_func_2(self):
        """
        parse each expression to get useful information: input is different
        input -->      ***[key, count(*), sum(a), sum(a+b*c)]***
        output -->     [key, count(*), sum(a) + 1, avg(a+b*c)]
        expression --> ['count(*)', 'sum(a)', 'avg(a+b*c)']
        ==> [ 
             ['count', 1,       0, '*',       1,  0],
             ['sum',   2,       0, 'a',       2,  1, [1], lambda _a:_a+1 ],
             ['sum',   3,       0, 'a+b*c',   3,  0]
            ]
            0 --> function name
            1 --> the position in the input
            2 --> the argument is an airthmatical operation or not
            3 --> the argument
            4 --> the position in the output
            5 --> is an outer airthmatical operation or not
            6 --> the positions of arguments for the outer operation in the output
            7 --> the outer operation
        """
        flag = 0
        out_pos = []
        in_pos = []
        for key in self.group_key:
            out_pos.append(self.output.index(key))
            in_pos.append(self.input.index(key))
        self.my_group_key = (True, self.group_key, in_pos, out_pos)
        # if there is avg function, convert it to sum and count
        avg_pos = []
        for fun in self.function:
            if fun.find("avg(") != -1:
                avg_pos.append(self.function.index(fun))
        if avg_pos != []:
            if "count(*)" not in self.function:
                self.function.append("count(*)")
        # sometimes, a result column has more than one function, then the pos in
        # the self.expr and self.expression is not the same, this counter is to 
        # count the pos of a function in self.expr and pos_map shows the mapping
        # of these two kinds of postions.
        fun_counter = 0 
        pos_map = {}
        for expr_num in range(len(self.function)):
            expr = self.function[expr_num]
            pos_map[expr_num] = fun_counter

            # parse "sum(a) + 1" --> ['sum(a)', '+', '1']
            if expr.find("count(*)") != -1:
                expr = expr.replace("count(*)", "count(a_a)")
                
            _expr = newparser.parse_column_expr(expr)
            
            #  to describe the pos of elements in the outer arithmetic operation
            outer_ao_pos = [] 
            new_expr = expr
            new_args = []
            for ele in _expr:
                if re.match("(.*)\((.*)\)", ele):
                    parsed_expr = []            
                    new_expr = new_expr.replace(ele, "_col%s" % str(fun_counter))
                    new_args.append("_col%s" % str(fun_counter))
                    func_name = ele[0:ele.find("(")]
                    if func_name not in conf.GENERAL_FUNC:
                        return False, "ParaLite cannot support aggregation function %s" % func_name
                    if func_name == "avg":
                        ele = ele.replace("avg", "sum")
                    fun_counter += 1
                    func_attr = ele[ele.find("(") + 1 : ele.rfind(")")]
                    parsed_expr.append(func_name)
                    if func_attr == "a_a": 
                        func_attr = "*"
                        ele = ele.replace("a_a", "*")
                        expr = expr.replace("count(a_a)", "count(*)")
                        opexpr = [func_attr]
                    pos_in_input = self.input.index(ele)
                    parsed_expr.append(pos_in_input)
                    parsed_expr.append(0)
                    parsed_expr.append(ele)
                    if expr in self.output: parsed_expr.append(self.output.index(expr))
                    else: parsed_expr.append(-1)
                    self.expr.append(parsed_expr)
                    outer_ao_pos.append(fun_counter - 1)
                elif re.search('^[a-zA-Z][a-zA-Z0-9_.]*$', ele):
                    # column element: a, b. it should be one of the group key
                    pos = self.group_key.index(ele)
                    new_expr = new_expr.replace(ele, ele.replace(".", "_"))
                    new_args.append(ele.replace(".", "_"))
                    outer_ao_pos.append(pos)
                else:
                    # other operator element: + - * / ^
                    continue
            cur_pos = pos_map[expr_num]

            if cur_pos >= len(self.expr):
                # the exception that select sum(a), count(*), avg(a) ... does not need
                # to do anything for avg(a)
                continue
            if len(_expr) == 1:
                self.expr[cur_pos].append(0)
            else:
                self.expr[cur_pos].append(1)
                self.expr[cur_pos].append(outer_ao_pos)

                tempexpr = new_expr
                tempargs = ",".join(new_args)
                for eacharg in new_args:
                    newarg = eacharg.replace(".", "_")
                    tempexpr = tempexpr.replace(eacharg, newarg)
                    tempargs = tempargs.replace(eacharg, newarg)
                self.expr[cur_pos].append(
                    eval("lambda %s:%s" % (tempargs, tempexpr)))
        self.pos_map = pos_map
        return flag            

    def parse_func_1(self):
        """
        parse each expression to get useful information:
        input -->      [key, a, b, c]
        output -->     [key, count(*), sum(a) + 1, avg(a+b*c)]
        expression --> ['count(*)', 'sum(a)', 'avg(a+b*c)']
        ==> [ 
             ['count', -1,      0, '*',                  1, 0],
             ['sum',   1,       0, 'a',                  2, 1, [1], lambda _a:_a+1 ],
             ['sum',   [1,2,3], 1, lambda a,b,c:a+b*c,   3, 0]
            ]
        """
        flag = 0
        out_pos = []
        in_pos = []
        for key in self.group_key:
            out_pos.append(self.output.index(key))
            in_pos.append(self.input.index(key))
        self.my_group_key = (True, self.group_key, in_pos, out_pos)
        # if there is avg function, convert it to sum and count
        avg_pos = []
        for fun in self.function:
            if fun.find("avg(") != -1:
                avg_pos.append(self.function.index(fun))
        if avg_pos != []:
            if "count(*)" not in self.function:
                self.function.append("count(*)")
        
        # sometimes, a result column has more than one function, then the pos in
        # the self.expr and self.expression is not the same, this counter is to 
        # count the pos of a function in self.expr and pos_map shows the mapping
        # of these two kinds of postions.
        fun_counter = 0 
        pos_map = {}
        for expr_num in range(len(self.function)):
            expr = self.function[expr_num]
            pos_map[expr_num] = fun_counter

            # parse "sum(a) + 1" --> ['sum(a)', '+', '1']
            if expr.find("count(*)") != -1:
                expr = expr.replace("count(*)", "count(a_a)")
                
            _expr = newparser.parse_column_expr(expr)
            
            #  to describe the pos of elements in the outer arithmetic operation
            outer_ao_pos = [] 
            new_expr = expr
            new_args = []
            for ele in _expr:
                if re.match("(.*)\((.*)\)", ele):
                    parsed_expr = []            
                    new_expr = new_expr.replace(ele, "_col%s" % str(fun_counter))
                    new_args.append("_col%s" % str(fun_counter))
                    # aggregate element: sum(a)
                    func_name = ele[0:ele.find("(")]
                    if func_name not in conf.GENERAL_FUNC:
                        return False, "ParaLite cannot support aggregation function %s" % func_name
                    if func_name == "avg":
                        ###func_name = "sum"
                        ele = ele.replace("avg", "sum")
                    fun_counter += 1
                    func_attr = ele[ele.find("(") + 1 : ele.rfind(")")]
                    parsed_expr.append(func_name)
                    if func_attr == "a_a": 
                        func_attr = "*"
                        expr = expr.replace("count(a_a)", "count(*)")
                        opexpr = [func_attr]
                    else:
                        opexpr = newparser.parse_column_expr(func_attr)
                    if len(opexpr) == 1:
                        # only a regular argument, the pos in the input string has two
                        # cases: (1), local aggregation is done: 
                        #                 input = [key, sum, count...]
                        #        (2), local aggregation is not done: 
                        #                 input = [key, col1, col2]
                        arg = opexpr[0]
                        if func_name == "count":
                            parsed_expr.append(-1)
                        else:
                            if arg in self.input: parsed_expr.append(self.input.index(arg))
                            elif expr in self.input: parsed_expr.append(self.input.index(expr))
                            else: parsed_expr.append(-1)
                        parsed_expr.append(0)
                        parsed_expr.append(opexpr[0])
                    else: 
                        # argument with airthmatical operation
                        pos = []
                        args = []
                        for var in opexpr:
                            if re.search('^[a-zA-Z][a-zA-Z0-9_.]*$',var):
                                pos.append(self.input.index(var))
                                args.append(var)
                        parsed_expr.append(pos)
                        parsed_expr.append(1)
                        
                        # replace . in func with _
                        tempargs = ",".join(args)
                        for eacharg in args:
                            newarg = eacharg.replace(".", "_")
                            func_attr = func_attr.replace(eacharg, newarg)
                            tempargs = tempargs.replace(eacharg, newarg)
                        ao = eval(("lambda %s:%s" % (tempargs, func_attr)))
                        
                        parsed_expr.append(ao)
                    if expr in self.output: parsed_expr.append(self.output.index(expr))
                    else: parsed_expr.append(-1)
                    self.expr.append(parsed_expr)
                    outer_ao_pos.append(fun_counter - 1)
                elif re.search('^[a-zA-Z][a-zA-Z0-9_.]*$', ele):
                    # column element: a, b. it should be one of the group key
                    pos = self.group_key.index(ele)
                    outer_ao_pos.append(pos)
                else:
                    # other operator element: + - * / ^
                    continue
            cur_pos = pos_map[expr_num]
            if cur_pos >= len(self.expr):
                # the exception that select sum(a), count(*), avg(a) ... does not need
                # to do anything for avg(a)
                continue
            if len(_expr) == 1:
                self.expr[cur_pos].append(0)
            else:
                self.expr[cur_pos].append(1)
                self.expr[cur_pos].append(outer_ao_pos)
                
                tempexpr = new_expr
                tempargs = ",".join(new_args)
                for eacharg in new_args:
                    newarg = eacharg.replace(".", "_")
                    tempexpr = tempexpr.replace(eacharg, newarg)
                    tempargs = tempargs.replace(eacharg, newarg)
                    
                self.expr[cur_pos].append(eval(
                    ("lambda %s:%s" % (tempargs, tempexpr))))
        self.pos_map = pos_map
        return flag            

    def hash_based_aggregate(self, data_list):
        hash_table = {} # key = group_key: values=[result_expr1, result_expr2,...]
        for data in data_list:
            for row in data.split(self.db_row_sep):
                if row == "":
                    continue
                cols = row.split(self.db_col_sep)
                group_key = self.db_col_sep.join(cols[s] for s in self.my_group_key[2])
                    

                for i in range(len(self.expr)):
                    expression = self.expr[i]
                    if expression[0] == "count":
                        """
                        Count has two different types of parameter.
                        (1) * : then it just needs to count the number of rows
                        (2) attr: it needs to count the number of rows that
                                  the attr is not NULL
                        """
                        if expression[1] == -1:                        
                            if expression[3] == "*":
                                delta_value = 1
                            else:
                                attr_value = cols[self.input.index(expression[3])]
                                if attr_value != None or attr_value != "":
                                    delta_value = 1
                                else: delta_value = 0
                        else:
                            delta_value = string.atoi(cols[expression[1]])
                            
                        if group_key in hash_table:
                            old_value = hash_table[group_key][i]
                            hash_table[group_key][i] += delta_value
                        else: 
                            hash_table[group_key] = [0]*len(self.expr)
                            hash_table[group_key][i] = delta_value
                            
                    elif expression[0] == "avg":
                        # actually there is no situation that avg appears here
                        if expression[2] == 1:
                            # airthematic operation
                            ao = expression[3]
                            delta_value = eval("ao(%s)" % ",".join(cols[s] for s in expression[1]))
                        else:
                            delta_value = float(cols[expression[1]])
                        if group_key in hash_table:
                            old_value = hash_table[group_key][i]
                            hash_table[group_key][i] += delta_value
                        else:
                            hash_table[group_key] = [0]*len(self.expr)
                            hash_table[group_key][i] = delta_value
                            
                    elif expression[0] == "max":
                        if expression[2] == 1:
                            # airthematic operation
                            ao = expression[3]
                            delta_value = eval("ao(%s)" % ",".join(cols[s] for s in expression[1]))
                        else:
                            delta_value = float(cols[expression[1]])
                        if group_key in hash_table:
                            old_value = hash_table[group_key][i]
                            if delta_value > old_value:
                                hash_table[group_key][i] = delta_value
                        else:
                            hash_table[group_key] = [0]*len(self.expr)
                            hash_table[group_key][i] = delta_value
                    elif expression[0] == "min":
                        if expression[2] == 1:
                            # airthematic operation
                            ao = expression[3]
                            delta_value = eval("ao(%s)" % ",".join(cols[s] for s in expression[1]))
                        else:
                            delta_value = float(cols[expression[1]])
                        if group_key in hash_table:
                            old_value = hash_table[group_key][i]
                            if delta_value < old_value:
                                hash_table[group_key][i] = delta_value
                        else:
                            hash_table[group_key] = [0]*len(self.expr)
                            hash_table[group_key][i] = delta_value
                    elif expression[0] == "sum":
                        if expression[2] == 1:
                            # airthematic operation
                            ao = expression[3]
                            delta_value = eval("ao(%s)" % ",".join(cols[s] for s in expression[1]))
                        else:
                            delta_value = float(cols[expression[1]])
                        if group_key in hash_table:
                            old_value = hash_table[group_key][i]
                            hash_table[group_key][i] += delta_value
                        else: 
                            hash_table[group_key] = [0]*len(self.expr)
                            hash_table[group_key][i] = delta_value
                    elif expression[0] == "total":
                        if expression[2] == 1:
                            # airthematic operation
                            ao = expression[3]
                            delta_value = eval("ao(%s)" % ",".join(float(cols[s]) for s in expression[1]))
                        else:
                            delta_value = cols[expression[1]]
                        if group_key in hash_table:
                            old_value = hash_table[group_key][i]
                            hash_table[group_key][i] += delta_value
                        else: 
                            hash_table[group_key] = [0]*len(self.expr)
                            hash_table[group_key][i] = delta_value
                        
                    else:
                        error = "ParaLite cannot support %s" % (expression[0])
                        return error
        # store output into buffer
        # group key following by all exressions or only expressions
        result_list = []
        result = cStringIO.StringIO()
        has_avg = False
        for texpr in self.expr:
            if texpr[0] == "avg":
                has_avg = True
                break

        if not has_avg:
            # there is no avg function
            for key in hash_table:
                value = hash_table[key]
                one_record = [0]*len(self.output)
                # output keys
                for p in range(len(self.my_group_key[3])):
                    key_out_pos = self.my_group_key[3][p]
                    one_record[key_out_pos] = key.split(self.db_col_sep)[p]
                # output values
                for fun_num in range(len(self.function)):
                    pos_in_expr = self.pos_map[fun_num]
                    expr = self.expr[pos_in_expr]
                    out_pos = expr[4]
                    if out_pos == -1:
                        continue
                    if expr[5] == 0:
                        # no outer ao
                        final_value = value[pos_in_expr]
                    else:
                        ao = expr[7]
                        final_value = eval(
                            "ao(%s)" % ",".join(str(value[s]) for s in expr[6]))
                    one_record[out_pos] = final_value
                #result.write(self.db_col_sep.join(str(s) for s in one_record))
                #result.write(self.db_row_sep)
                result_list.append(one_record)
        else:
            # there is avg function
            for key in hash_table:
                value = hash_table[key]
                one_record = [0]*len(self.output)
                # output keys
                for p in range(len(self.my_group_key[3])):
                    key_out_pos = self.my_group_key[3][p]
                    one_record[key_out_pos] = key.split(self.db_col_sep)[p]

                # set the right avg value instead of the original sum value
                for expr in self.expr:
                    if expr[0] == "avg":
                        out_pos = expr[4]
                        count_pos = self.pos_map[self.function.index("count(*)")]
                        pos_in_expr = self.expr.index(expr)
                        final_value = float(value[pos_in_expr]) / value[count_pos]
                        value[pos_in_expr] = final_value
                        
                # calculate the outer operation and output data
                for fun_num in range(len(self.function)):
                    pos_in_expr = self.pos_map[fun_num]
                    expr = self.expr[pos_in_expr]
                    out_pos = expr[4]
                    if out_pos == -1:
                        continue
                    if expr[5] == 0:
                        # no outer ao
                        final_value = value[pos_in_expr]
                    else:
                        ao = expr[7]
                        final_value = eval("ao(%s)" % ",".join(str(value[s]) for s in expr[6]))
                    one_record[out_pos] = final_value
                #result.write(self.db_col_sep.join(str(s) for s in one_record))
                #result.write(self.db_row_sep)
                result_list.append(one_record)

        sep = self.db_col_sep
        if self.dest == conf.DATA_TO_ANO_OP and self.partition_num > 1:
            data_part_list = []
            for i in range(self.partition_num):
                data_part_list.append(cStringIO.StringIO())
            key_pos = []
            for key in self.split_key:
                key_pos.append(self.output.index(key))
                
            t_size = 0
            for re in result_list:
                if re == "":
                    continue
                part_id = abs(hash(
                    sep.join(str(re[p]) for p in key_pos))) % self.partition_num
                tdata = sep.join(str(ss) for ss in re) + self.db_row_sep
                t_size += len(tdata)
                data_part_list[part_id].write(tdata)
            del result_list
            return conf.MULTI_BUFFER, data_part_list, t_size
        else:
            csio = cStringIO.StringIO()
            for re in result_list:
                if re == "":
                    continue
                csio.write(sep.join(str(ss) for ss in re))
                csio.write(self.db_row_sep)
            del result_list
            return conf.SINGLE_BUFFER, [csio], len(csio.getvalue())

    def distribute_data(self):
        whole_data = cStringIO.StringIO()
        for i in self.result:
            for csio in self.result[i]:
                d = string.strip(csio.getvalue())
                whole_data.write(d)
                if d != "":
                    whole_data.write("\n")
                del csio
            
        if self.distinct or self.limit != -1:
            data_list = whole_data.getvalue().split(self.db_row_sep)
            del whole_data
        
            if self.distinct:
                data_list = set(data_list)
            if self.limit != -1:
                data_list = data_list[:self.limit]

            data = cStringIO.StringIO()
            data.write(self.db_row_sep.join(str(s) for s in data_list))
            del data_list
        else:
            data = whole_data

        if self.dest == conf.DATA_TO_ONE_CLIENT:
            # send data to a random client
            random_num = random.randint(0, len(self.client_sock) - 1)
            addr = self.client_sock[random_num]
            sock = socket(AF_INET, SOCK_STREAM)
            sock.connect(addr)
            data_s = data.getvalue()
            sock.send("%10s%s" % (len(data_s), data_s))
            re = sock.recv(10)
            assert re == "OK"
            sock.close()

        elif self.dest == conf.DATA_TO_DB:
            self.data = data
            col_sep = self.db_col_sep
            row_sep = self.db_row_sep
            master = (self.master_name, self.master_port)
            ParaLiteLog.debug("Load data start:")
            # send request to the master
            t_size = len(data.getvalue())
            sep = conf.SEP_IN_MSG
            tag = conf.LOAD_FROM_API
            if row_sep is None or row_sep == "\n":
                temp_sep = "NULL"
            else:
                temp_sep = row_sep
            msg = sep.join(
                str(s) for s in [conf.REQ, self.cqid, gethostname(), 
                                 self.my_port, self.dest_db, self.dest_table,
                                 t_size, tag, self.fashion, temp_sep, "0"])
            so_master = socket(AF_INET, SOCK_STREAM)
            so_master.connect(master)
            so_master.send("%10s%s" % (len(msg),msg))
            so_master.close()

    def start(self):
        try:
            # start socket server to listen all connections
            ch = self.iom.create_server_socket(AF_INET,
                                               SOCK_STREAM, 100, ("", self.my_port)) 
            n, self.my_port = ch.ss.getsockname()
            ParaLiteLog.debug("listen on port : %s ..." % str(self.my_port))
            
            # start socket server for local connections
            self.local_addr = "/tmp/paralite-local-addr-groupby-%s-%s-%s" % (
                gethostname(), self.cqid, self.opid)
            if os.path.exists(self.local_addr): os.remove(self.local_addr)
            self.iom.create_server_socket(AF_UNIX,
                                          SOCK_STREAM, 10, self.local_addr) 
            
            # register local port to the master
            self.register_to_master(self.cqid, self.opid, gethostname(), self.my_port)
            ParaLiteLog.debug("reg to master: FINISH")
            
            while self.is_running:
                ev = self.next_event(None)
                if isinstance(ev, ioman_base.event_accept):
                    self.handle_accept(ev)
                if isinstance(ev, ioman_base.event_read):
                    if ev.data != "":
                        self.handle_read(ev)

            ParaLiteLog.info("--groupby node %s on %s is finished--" % (self.opid,
                                                                    gethostname()))

        except KeyboardInterrupt, e:
            self.report_error("ParaLite receives a interrupt signal and then will close the process\n")
            ParaLiteLog.info("--groupby node %s on %s is finished--" % (self.opid,
                                                                        gethostname()))
        except Exception, e1:
            ParaLiteLog.debug("ERROR : %s" % traceback.format_exc())
            self.report_error("ERROR : %s" % traceback.format_exc())

    def handle_read(self, event):
        message = event.data[10:]

        sep = conf.SEP_IN_MSG
        m = message.split(sep)
        try:        
            if m[0] == conf.JOB_ARGUMENT:
                self.parse_args(m[1])

                ParaLiteLog.info("parsed structure : \n%s" % str(self.expr))
                ParaLiteLog.info("parse arguments: FINISH")

                if self.is_checkpoint is not None and self.is_checkpoint == conf.CHECKPOINT:
                    # this is a recovery operator
                    # init the persisted result data
                    ParaLiteLog.debug("recovery data: START")
                    self.recovery_data(self.result, gethostname())
                    ParaLiteLog.debug("recovery data: FINISH")
                    self.send_rs_info_to_master(0, 0)

                else:
                    self.parse_func()
                    # delete all temporary files for this operator
                    os.system("rm -f %s/%s_%s" % (self.temp_dir, "groupby", self.opid))


            elif m[0] == conf.JOB:
                ParaLiteLog.debug("MESSAGE: %s" % message)                
                self.cur_jobid = m[1]
                self.job_list.append(m[1])
                
            elif m[0] == conf.DATA:
                data_id = string.strip(m[1][0:2])
                data = m[1][2:]
                self.source_data.append(data)

                # aggregate data
                if not self.is_data_ready(self.source_data, self.num_of_children):
                    return

                ParaLiteLog.debug("****GROUP DATA****: start")
                s = 0
                for data in self.source_data:
                    s += len(data)
                ParaLiteLog.debug("source data size : %s" % s)
                s_time = time.time()
                rs_type, rs, t_size = self.hash_based_aggregate(self.source_data)
                ParaLiteLog.debug("****GROUP DATA****: finish")
                
                del self.source_data
                self.total_size += t_size
                self.source_data = []
                
                # store the result of one job to the final result
                if len(rs) == 1:
                    if self.dest == conf.DATA_TO_ANO_OP or self.dest == conf.DATA_TO_DB:
                        # dest is AGGR op or ORDER op, use 0 as the key
                        if 0 not in self.result:
                            self.result[0] = rs
                        else:
                            self.result[0].append(rs[0])
                    else:
                        # dest is UDX op, use jobid as the key
                        self.result[string.atoi(self.cur_jobid)] = rs
                        if self.is_checkpoint == 1:
                            self.write_data_to_disk(self.cur_jobid, rs[0].getvalue())

                else:
                    # use partid as the key
                    for i in range(len(rs)):
                        if i not in self.result:
                            self.result[i] = [rs[i]]
                        else:
                            self.result[i].append(rs[i])

                # check if the whole data exceeds the LIMITATION
                if rs_type != conf.MULTI_FILE:
                    if self.total_size > self.MAX_SIZE:
                        for dataid in self.result:
                            data = ""
                            for d in self.result[dataid]:
                                data += d
                            self.write_data_to_disk(dataid, data)
                        self.result_type = conf.MULTI_FILE
                
                e_time = time.time()
                self.total_time += (e_time - s_time)
                self.send_status_to_master(self.cur_jobid, conf.PENDING)
                    
            elif m[0] == conf.JOB_END:
                ParaLiteLog.debug("MESSAGE: %s" % message)                
                # all jobs are finished
                self.send_rs_info_to_master(self.total_size, self.total_time)
                
                # distribute data
                if self.dest == conf.DATA_TO_ONE_CLIENT:
                    self.distribute_data()
                    self.send_status_to_master(" ".join(self.job_list), conf.ACK)
                    self.is_running = False
                else self.dest == conf.DATA_TO_DB:
                    self.distribute_data()

            elif m[0] == conf.DATA_PERSIST:
                # if the data is requried to be persisted or not
                ParaLiteLog.debug("MESSAGE: %s" % message)
                self.process_ck_info(m)

            elif m[0] == conf.DATA_REPLICA:
                # message --> DATA_REPLICA:DATANODE:DATAID:DATA
                datanode, dataid = m[1:3]
                f_name = self.get_file_name_by_data_id(gethostname(), dataid)
                fr = open(f_name, "wa")
                fr.write(m[3])
                fr.close()

            elif m[0] == conf.DATA_DISTRIBUTE:
                ParaLiteLog.debug("MESSAGE: %s" % message)                
                # send a part of data to the next operator
                # DATA_DISTRIBUTE:partition_num:destnode
                part_id, destnode = m[1:]
                data = self.get_data_by_part_id(self.result, string.atoi(part_id))
                
                # DATA message includes: type:id+data
                # the first 2 chars represents the opid
                msg = sep.join([conf.DATA, "%2s%s" % (self.opid, data)])
                if destnode == gethostname():
                    # use local socket
                    addr = self.p_node[destnode][1]
                    t = AF_UNIX
                else:
                    addr = (destnode, self.p_node[destnode][0])
                    t = AF_INET
                self.send_data_to_node(msg, t, addr)
                ParaLiteLog.debug(
                    "send data susscufully   %s %s --> %s" % (
                        self.opid, gethostname(), destnode))

            elif m[0] == conf.DLOAD_REPLY:
                reply = sep.join(m[1:])
                ParaLiteLog.info("receive the information from the master")
                ParaLiteLog.debug(reply)
                dload_client.dload_client().load_internal_buffer(
                    reply, self.dest_table, self.data, self.fashion, 
                    self.hash_key, self.hash_key_pos, self.db_col_sep, 
                    self.db_row_sep, self.db_col_sep, False, "0", self.log_dir)

                # send END_TAG to the master
                client_id = "0"
                msg = sep.join([conf.REQ, conf.END_TAG, gethostname(), client_id])
                so_master = socket(AF_INET, SOCK_STREAM)
                so_master.connect((self.master_name, self.master_port))
                so_master.send("%10s%s" % (len(msg), msg))
                so_master.close()
                ParaLiteLog.debug("sending to master: %s" % (conf.END_TAG))
                ParaLiteLog.debug("----- dload client finish -------")

            elif message == conf.DLOAD_END_TAG:
                ParaLiteLog.debug("---------import finish---------")
                self.send_status_to_master(" ".join(self.job_data), conf.ACK)
                self.is_running = False

            elif m[0] == conf.DATA_END:
                ParaLiteLog.debug("MESSAGE: %s" % message)                
                # all data is dipatched to the parent nodes
                self.send_status_to_master(" ".join(self.job_list), conf.ACK)
                ParaLiteLog.debug("notify ACK to master")                    
                self.is_running = False
            
            elif m[0] == conf.EXIT:
                self.is_running = False

            elif m[0] == conf.NODE_FAIL:
                ParaLiteLog.debug("MESSAGE: %s" % message)
                # message --> NODE_FAIL:FAILED_NODE:REPLICA_NODE
                failed_node, replica_node = m[1:3]
                self.failed_node.append(failed_node)
                if replica_node == gethostname():
                    # load replica data for the failed node
                    self.recovery_data(self.replica_result, replica_node)

        except Exception, e:
            es("ERROR: in groupby.py : %s" % traceback.format_exc())
            ParaLiteLog.info(traceback.format_exc())
            self.is_running = False
            self.no_error = False

    def recovery_data(self, result, node):
        if self.dest == conf.DATA_TO_ANO_OP and num_of_dest > 1:
            for partid in result:
                f_name = self.get_file_name_by_data_id(node, partid)
                result[partid] = [f_name]
        else:
            for jobid in self.job_list:
                f_name = self.get_file_name_by_data_id(node, jobid)
                result[jobid] = [f_name]

    def process_ck_info(self, message):
        # message --> DATA_PERSIST:CHECKPOINT[:REPLICA_NODE:REPLICA_PORT]
        is_ck = message[1]
        self.is_checkpoint = is_ck
        if is_ck == conf.CHECKPOINT:
            replica_addr = (message[2], string.atoi(message[3]))
            if self.result_type != conf.MULTI_FILE:
                for dataid in self.result:
                    ds = ""
                    for data in self.result[dataid]:
                        ds += data.getvalue()
                    self.write_data_to_disk(dataid, ds)

                    f_name = self.get_file_name_by_data_id(gethostname(), dataid)
                    cmd = "scp %s %s:%s" % (f_name, replica_addr[0], f_name)
                    os.system(cmd)
                    
            else:
                for dataid in self.result:
                    f = open(self.result[dataid], "rb")
                    while True:
                        ds = f.read(self.max_size)
                        if not ds:
                            break
                        msg = conf.SEP_IN_MSG.join(
                            [conf.DATA_REPLICA, gethostname(), str(dataid), ds])
                        self.send_data_to_node(msg, AF_INET, replica_addr)

    def write_data_to_disk(self, data_id, data):
        """When data is out of memory or data is requried to be checkpointed, ParaLite
        writes data to files.
        @param data_id   job id for UDX parent or partition number for other parents
        @param data      
        """
        file_name = self.get_file_name_by_data_id(gethostname(), data_id)
        f = open(file_name, "wa")
        f.write(data)
        f.close()

    def get_file_name_by_data_id(self, node, data_id):
        # file_name: opname_opid_hostname_dataid
        return "%s/%s_%s_%s_%s" % (
            self.temp_dir, "groupby", self.opid, node, data_id)

    def send_data_to_node(self, msg, tp, addr):
        sock = socket(tp, SOCK_STREAM)
        try:
            sock.connect(addr)
            sock.send("%10s%s" % (len(msg),msg))
            sock.close()
        except Exception, e:
            raise(Exception(e))

    def get_data_by_part_id(self, result, part_id):
        ParaLiteLog.debug("partition number : %s" % len(self.p_node))
        if self.dest != conf.DATA_TO_ANO_OP or self.dest == conf.DATA_TO_ANO_OP and len(self.p_node) == 1:
            # part id is the job id
            rs = ""
            for dataid in self.result:
                for data in self.result[dataid]:
                    if isinstance(data, str):
                        # data is stored in file
                        f = open(data, "rb")
                        rs += f.read()
                        f.close()
                    else:
                        # data is stored in buffer
                        rs += data.getvalue()
            return rs
        
        rs = ""
        for part in result[part_id]:
            if isinstance(part, str):
                # data is stored in file
                f = open(part, "rb")
                rs += f.read()
                f.close()
            else:
                # data is stored in buffer
                rs += part.getvalue()
        return rs
    
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

    def handle_accept(self, event):
        event.new_ch.flag = conf.SOCKET_OUT
        event.new_ch.buf = cStringIO.StringIO()
        event.new_ch.length = 0

    def register_to_master(self, cqid, opid, node, port):
        sep = conf.SEP_IN_MSG
        msg = sep.join([conf.REG, conf.DATA_NODE, cqid, opid, gethostname(),
                        str(self.my_port), self.local_addr])
        ParaLiteLog.debug(
            "MASTER_NODE: %s  MASTER_PORT: %s" % (
                self.master_name, self.master_port))
        addr = (self.master_name, self.master_port)
        sock = socket(AF_INET, SOCK_STREAM)
        try:
            sock.connect(addr)
        except Exception, e:
            ParaLiteLog.error("Error in register_to_master: %s" % traceback.format_exc())
            if e.errno == 4:
                sock.connect(addr)
                
        sock.send('%10s%s' % (len(msg), msg))
        sock.close()

    def send_rs_info_to_master(self, total_size, total_time):
        # RS:DATANODE:cqid:opid:node:port:rs_type:partition_num:total_size:total_time
        addr = (self.master_name, self.master_port)
        sock = socket(AF_INET, SOCK_STREAM)
        sock.connect(addr)
        sep = conf.SEP_IN_MSG
        m = [conf.RS, conf.DATA_NODE, self.cqid, self.opid, gethostname(),
             str(self.my_port),str(self.result_type), 
             str(self.partition_num), str(total_size), str(total_time)]
        msg = sep.join(m)
        sock.send('%10s%s' % (len(msg), msg))

    def send_status_to_master(self, jobid, status):
        addr = (self.master_name, self.master_port)
        sock = socket(AF_INET, SOCK_STREAM)
        sock.connect(addr)
        sep = conf.SEP_IN_MSG
        # status:JOB:cqid:opid:jobid:hostname:port
        m = [status, conf.JOB, self.cqid, self.opid, jobid,
             gethostname(), str(self.my_port)]
        msg = sep.join(m)
        sock.send('%10s%s' % (len(msg), msg))

    def is_data_ready(self, source_data, num_of_s):
        if len(source_data) != num_of_s - len(self.failed_node):
            return False
        return True

    def report_error(self, err):
        sock = socket(AF_INET, SOCK_STREAM)
        sock.connect((self.master_name, self.master_port))
        sock.send("%10s%s" % (len(err), err))
        sock.close()
        
def test():
    ins = GroupbyOp()
    ins.db_row_sep = "\n"
    ins.db_col_sep = "|"
    ins.group_key = ["T.key", "T.key1"]
    ins.output = [ "max(T.c) + 10", "2*avg(T.a+T.b) + avg(T.c)", "T.key", "T.key1"]
    ins.function = ["max(T.c) + 10", "2*avg(T.a+T.b) + avg(T.c)"]
    
    data_list = []
    data1 = []
    data1.append('aaa|ccc|3|4|7|5')
    data1.append('aaa|ddd|8|3|2|8')
    data1.append('bbb|ccc|4|9|42|3')
    data1.append('bbb|ddd|1|9|15|4')
    print data1
    data_list.append("\n".join(data1))
    
    # ############
    ins.input = ["T.key", "T.key1", "T.a", "T.b", "T.c", "count(*)"]
    ins.is_child_sql = False    
    # #############
    
    # ############
    #ins.input = ["T.key", "T.key1", "max(T.c)", "sum(T.a+T.b)", "sum(T.c)", "count(*)"]
    #ins.is_child_sql = True
    # ###########

    print "input:\t%s" % ins.input
    print "output:\t%s" % ins.output
    print "function:\t%s" % ins.function

    ins.parse_func()
    for e in ins.expr:
        print e
    print "=============="
    print ins.group_key
    t, result, size = ins.hash_based_aggregate(data_list)
    print result[0].getvalue()
    
def main():
    if len(sys.argv) != 7:
        sys.exit(1)
    proc = GroupbyOp()
    proc.master_name = sys.argv[1]
    proc.master_port = string.atoi(sys.argv[2])
    proc.cqid = sys.argv[3]
    proc.opid = sys.argv[4]
    proc.my_port = string.atoi(sys.argv[5])
    proc.log_dir = sys.argv[6]
    if not os.path.exists(proc.log_dir): os.makedirs(proc.log_dir)
    cur_time = time.strftime('%Y-%m-%d-%H-%M-%S',time.localtime(time.time()))
    ParaLiteLog.init("%s/groupby-%s-%s-%s.log" % (
        proc.log_dir, gethostname(), proc.cqid, proc.opid),
                     logging.DEBUG)
    ParaLiteLog.info("--groupby node %s on %s is started" % (proc.opid, gethostname()))
    proc.start()
    
if __name__=="__main__":
    #test()
    main()

