import sys
import string
import cPickle
import base64
import random
import traceback
import pwd
import time
import os
import cStringIO
import subprocess
import re
import shlex
import logging
from socket import *

# set the home direcotry of paraLite into system's path
sys.path.append(os.path.abspath(os.path.join(sys.path[0], os.path.pardir)))

import dload_client
import conf
from lib.logger import ParaLiteLog
from lib import ioman, ioman_base
from lib import newparser

def es(s):
    sys.stderr.write(s)

def get_max_size():
    """get the free memory size
    """
    p = subprocess.Popen(shlex.split("free -m"), stdout=subprocess.PIPE)
    stdout_list = p.communicate()[0].split('\n')
    s = string.atoi(stdout_list[1].split()[3].strip())*1024*1024
    return s

class AggregateOp:
    
    MAX_SIZE = get_max_size()
    
    def __init__(self):
        self.opid = None
        self.cqid = None
        self.key = None
        self.dest = None
        self.input = []
        self.output = []
        self.client_sock = None
        self.my_port = None
        self.expression = None
        self.function = None
        self.master_name = None
        self.master_port = 0
        self.name = None
        self.partition_num = 0
        self.status = None
        self.db_col_sep = None
        self.db_row_sep = None
        self.dest_db = None
        self.dest_table = None
        self.fashion = None
        self.hash_key = None
        self.hash_key_pos = 0
        self.num_of_children = 0
        self.is_child_sql = False
        self.node = (None, 0)  # (node, port)
        self.log_dir = None
        self.temp_dir = None
        self.limit = -1
        self.distinct = False

        self.expr = []
        self.iom = ioman.mk_ioman()
        self.is_running = True
        self.replica_result = {} # the result data for a failed node
        self.failed_node = []
        self.result = {}
        self.result_type = 0
        self.source_data = []
        self.total_size = 0
        self.total_time = 0
        self.cur_jobid = None
        
    def parse_args(self, msg):
        b1 = base64.decodestring(msg)
        dic = cPickle.loads(b1)
        
        for key in dic.keys():
            value = dic[key]
            if hasattr(self, key):
                setattr(self, key, value)
        if not os.path.exists(self.temp_dir): os.makedirs(self.temp_dir)

        # convert function into lower case
        for i in range(len(self.function)):
            func = self.function[i]
            self.function[i] = func[0:func.find("(")].lower() + func[func.find("("):]

        self.parse_func()

    def parse_func(self):
        if self.is_child_sql:
            self.parse_func_2()
        else:
            self.parse_func_1()

    def parse_func_1(self):
        """
        parse each expression to get useful information:
        input -->      [a, b, c]
        output -->     [count(*), sum(a) + 1, avg(a+b*c)]
        expression --> ['count(*)', 'sum(a)', 'avg(a+b*c)']
        ==> [ 
             ['count', -1,      0, '*',                  1, 0],
             ['sum',   1,       0, 'a',                  2, 1, [1], lambda _a:_a+1 ],
             ['sum',   [1,2,3], 1, lambda a,b,c:a+b*c,   3, 0]
            ]
        """
        flag = 0
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
                    pos = self.input.index(ele)
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
        return True, None
        

    def parse_func_2(self):
        """
        parse each expression to get useful information: input is different
        input -->      ***[count(*), sum(a), sum(a+b*c)]***
        output -->     ***[count(*), sum(a) + 1, avg(a+b*c)]***
        expression --> ['count(*)', 'sum(a)', 'avg(a+b*c)']
        ==> [ 
             ['count', 1,       0, '*',       1,  0],
             ['sum',   2,       0, 'a',       2,  1, [1], lambda _a:_a+1 ],
             ['sum',   3,       0, 'a+b*c',   3,  0]
            ]
        """
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
        return True, None

        
    def aggregate(self, data_list):
        result = [0]*len(self.expr)
        for data in data_list:
            for row in data.split(self.db_row_sep):
                if row == "":
                    continue
                cols = row.split(self.db_col_sep)
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
                        result[i] += delta_value
                        
                    elif expression[0] == "max":
                        if expression[2] == 1:
                            # airthematic operation
                            ao = expression[3]
                            delta_value = eval("ao(%s)" % ",".join(cols[s] for s in expression[1]))
                        else:
                            delta_value = float(cols[expression[1]])
                        old_value = result[i]
                        if delta_value > old_value:
                            result[i] = delta_value

                    elif expression[0] == "avg":
                        # actually there is no situation that avg appears here
                        if expression[2] == 1:
                            # airthematic operation
                            ao = expression[3]
                            delta_value = eval("ao(%s)" % ",".join(cols[s] for s in expression[1]))
                        else:
                            delta_value = float(cols[expression[1]])
                        result[i] += delta_value

                    elif expression[0] == "min":
                        if expression[2] == 1:
                            # airthematic operation
                            ao = expression[3]
                            delta_value = eval("ao(%s)" % ",".join(cols[s] for s in expression[1]))
                        else:
                            delta_value = float(cols[expression[1]])
                        
                        old_value = result[i]
                        if delta_value < old_value:
                            result[i] = delta_value

                    elif expression[0] == "sum":
                        if expression[2] == 1:
                            # airthematic operation
                            ao = expression[3]
                            delta_value = eval("ao(%s)" % ",".join(cols[s] for s in expression[1]))
                        else:
                            delta_value = float(cols[expression[1]])
                        result[i] += delta_value

                    elif expression[0] == "total":
                        if expression[2] == 1:
                            # airthematic operation
                            ao = expression[3]
                            delta_value = eval("ao(%s)" % ",".join(float(cols[s]) for s in expression[1]))
                        else:
                            delta_value = cols[expression[1]]
                        result[i] += delta_value
                        
                    else:
                        error = "ParaLite cannot support %s" % (expression[0])
                        return None, None, None

        has_avg = False
        for texpr in self.expr:
            if texpr[0] == "avg":
                has_avg = True
                break

        result_record = [0]*len(self.output)
        if not has_avg:
            # there is no avg function
            # output values
            for fun_num in range(len(self.function)):
                pos_in_expr = self.pos_map[fun_num]
                expr = self.expr[pos_in_expr]
                out_pos = expr[4]
                if out_pos == -1:
                    continue
                if expr[5] == 0:
                    # no outer ao
                    final_value = result[pos_in_expr]
                else:
                    ao = expr[7]
                    final_value = eval(
                        "ao(%s)" % ",".join(str(result[s]) for s in expr[6]))
                result_record[out_pos] = final_value
        else:
            # there is avg function
            # output values
            # for fun_num in range(len(self.function)):
            #     func = self.function[fun_num]
            #     func_name = func.split("(")[0]
            #     if func_name == "avg":
            #         pos_in_expr = self.pos_map[fun_num]
            #         expr = self.expr[pos_in_expr]
            #         out_pos = expr[4]
            #         if out_pos == -1:
            #             continue
            #         count_pos = self.pos_map[self.function.index("count(*)")]
            #         final_value = float(result[pos_in_expr]) / result[count_pos]
            #         if expr[5] == 1:
            #             ao = expr[7]
            #             final_value = eval("ao(%s)" % final_value)
            #     else:
            #         pos_in_expr = self.pos_map[fun_num]
            #         expr = self.expr[pos_in_expr]
            #         out_pos = expr[4]
            #         if out_pos == -1:
            #             continue
            #         if expr[5] == 0:
            #             # no outer ao
            #             final_value = result[pos_in_expr]
            #         else:
            #             ao = expr[7]
            #             final_value = eval(
            #                 "ao(%s)" % ",".join(str(result[s]) for s in expr[6]))
            #     result_record[out_pos] = final_value

            # set the right avg value instead of the original sum value
            for expr in self.expr:
                if expr[0] == "avg":
                    out_pos = expr[4]
                    count_pos = self.pos_map[self.function.index("count(*)")]
                    pos_in_expr = self.expr.index(expr)
                    final_value = float(result[pos_in_expr]) / result[count_pos]
                    result[pos_in_expr] = final_value

            # calculate the outer operation and output data
            for fun_num in range(len(self.function)):
                pos_in_expr = self.pos_map[fun_num]
                expr = self.expr[pos_in_expr]
                out_pos = expr[4]
                if out_pos == -1:
                    continue
                if expr[5] == 0:
                    # no outer ao
                    final_value = result[pos_in_expr]
                else:
                    ao = expr[7]
                    final_value = eval(
                        "ao(%s)" % ",".join(str(result[s]) for s in expr[6]))
                result_record[out_pos] = final_value

        csio = cStringIO.StringIO()
        csio.write(self.db_col_sep.join(str(s) for s in result_record))
        return conf.SINGLE_BUFFER, [csio], len(csio.getvalue())
    
    def notify_to_master(self):
        addr = (self.master_name, self.master_port)
        sock = socket(AF_INET, SOCK_STREAM)
        sock.connect(addr)
        sep = conf.SEP_IN_MSG
        msg = '%s%s%s%s%s%s%s' % (conf.ACK, sep, conf.DATA_NODE, sep, self.cqid, sep, gethostname())
        sock.send('%10s%s' % (len(msg), msg))
        sock.close()
        
    def start(self):
        try:
            # start socket server to listen all connections
            ch = self.iom.create_server_socket(AF_INET,
                                               SOCK_STREAM, 100, ("", self.my_port)) 
            n, self.my_port = ch.ss.getsockname()
            ParaLiteLog.debug("listen on port : %s ..." % str(self.my_port))
            
            # start socket server for local connections
            self.local_addr = "/tmp/paralite-local-addr-orderby-%s-%s-%s" % (
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

            ParaLiteLog.info("--orderby node %s on %s is finished--" % (self.opid,
                                                                    gethostname()))

        except KeyboardInterrupt, e:
            self.report_error("ParaLite receives a interrupt signal and then will close the process\n")
            ParaLiteLog.info("--orderby node %s on %s is finished--" % (self.opid,
                                                                        gethostname()))
            sys.exit(1)
        except Exception, e1:
            ParaLiteLog.debug(traceback.format_exc())
            self.report_error(traceback.format_exc())
            sys.exit(1)

    def handle_read(self, event):
        message = event.data[10:]
        m = message.split(conf.SEP_IN_MSG)
        try:        
            if m[0] == conf.JOB_ARGUMENT:
                self.parse_args(m[1])
                ParaLiteLog.info("parse arguments: FINISH")

            elif m[0] == conf.JOB:
                self.cur_jobid = m[1]
                
            elif m[0] == conf.DATA:
                data_id = string.strip(m[1][0:2])
                data = m[1][2:]
                self.source_data.append(data)
                # sort data
                if not self.is_data_ready(self.source_data, self.num_of_children):
                    return

                s_time = time.time()
                rs_type, rs, t_size = self.aggregate(self.source_data)
                del self.source_data
                self.total_size += t_size
                self.source_data = {}
                
                # store the result of one job to the final result
                for i in range(len(rs)):
                    if i not in self.result:
                        self.result[i] = [rs[i]]
                    else:
                        self.result[i].append(rs[i])
                
                if rs_type != conf.MULTI_FILE:
                    # check if the whole data exceeds the LIMITATION
                    if self.total_size + t_size > self.MAX_SIZE:
                        self.write_data_to_disk()
                        self.result_type = conf.MULTI_FILE
                    self.total_size += t_size
                e_time = time.time()
                self.total_time += (e_time - s_time)
                
                self.send_status_to_master(self.cur_jobid, conf.PENDING)
                    
            elif m[0] == conf.JOB_END:
                # all jobs are finished
                self.send_rs_info_to_master(self.total_size, self.total_time)
                
                # distribute data
                if self.dest == conf.DATA_TO_DB or self.dest == conf.DATA_TO_ONE_CLIENT:
                    ParaLiteLog.debug("dest = %s" % self.dest)                    
                    self.distribute_data()
                    self.send_status_to_master(self.cur_jobid, conf.ACK)
                    self.is_running = False

            elif m[0] == conf.DATA_PERSIST:
                # if the data is requried to be persisted or not
                if m[1] == conf.CHECKPOINT:
                    self.write_data_to_disk()

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
            es("ERROR: %s" % traceback.format_exc())
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

    def distribute_data(self):
        whole_data = cStringIO.StringIO()
        for i in self.result:
            for csio in self.result[i]:
                d = string.strip(csio.getvalue())
                whole_data.write(d)
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
            col_sep = self.db_col_sep
            row_sep = self.db_row_sep
            master = (self.master_name, self.master_port)
            dload_client.dload_client().load_internal(
                master, self.cqid, gethostname(),self.dest_db,
                self.dest_table, data, 1, self.fashion,
                self.hash_key, self.hash_key_pos, col_sep,
                row_sep, col_sep, False, "0", self.log_dir)

    def handle_accept(self, event):
        event.new_ch.flag = conf.SOCKET_OUT
        event.new_ch.buf = cStringIO.StringIO()
        event.new_ch.length = 0

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

    def is_data_ready(self, source_data, num_of_s):
        if len(source_data) != num_of_s - len(self.failed_node):
            return False
        return True

    def send_rs_info_to_master(self, total_size, total_time):
        # RS:DATANODE:cqid:opid:rs_type:partition_num:total_size:total_time
        addr = (self.master_name, self.master_port)
        sock = socket(AF_INET, SOCK_STREAM)
        sock.connect(addr)
        sep = conf.SEP_IN_MSG
        m = [conf.RS, conf.DATA_NODE, self.cqid, self.opid, gethostname(),
             str(self.my_port), str(self.result_type), 
             str(self.partition_num), str(total_size), str(total_time)]
        msg = sep.join(m)
        sock.send('%10s%s' % (len(msg), msg))
        

    def register_to_master(self, cqid, opid, node, port):
        sep = conf.SEP_IN_MSG
        msg = sep.join([conf.REG, conf.DATA_NODE, cqid, opid, gethostname(),
                        str(self.my_port), self.local_addr])
        ParaLiteLog.debug("MASTER_NODE: %s  MASTER_PORT: %s" % (self.master_name, self.master_port))
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

    def report_error(self, err):
        sock = socket(AF_INET, SOCK_STREAM)
        sock.connect((self.master_name, self.master_port))
        sock.send("%10s%s" % (len(err), err))
        sock.close()

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

def main():
    if len(sys.argv) != 7:
        sys.exit(1)
    proc = AggregateOp()
    proc.master_name = sys.argv[1]
    proc.master_port = string.atoi(sys.argv[2])
    proc.cqid = sys.argv[3]
    proc.opid = sys.argv[4]
    proc.my_port = string.atoi(sys.argv[5])
    proc.log_dir = sys.argv[6]
    if not os.path.exists(proc.log_dir): os.makedirs(proc.log_dir)
    cur_time = time.strftime('%Y-%m-%d-%H-%M-%S',time.localtime(time.time()))
    ParaLiteLog.init(
        "%s/aggregate-%s-%s.log" % (proc.log_dir, gethostname(), cur_time),
        logging.DEBUG)
    ParaLiteLog.info(
        "--aggregate node %s on %s is started" % (proc.opid, gethostname()))
    proc.start()
        
def test():
    ins = AggregateOp()
    ins.db_row_sep = "\n"
    ins.db_col_sep = "|"
    ins.output = ["max(T.c) + 10", "avg(T.c+T.b) + avg(T.c)"]
    ins.function = ["max(T.c) + 10", "avg(T.c+T.b) + avg(T.c)"]
    
    data_list = []
    data1 = []
    data1.append('3|4|4|8')
    data1.append('8|3|6|9')
    data1.append('4|9|8|2')
    data1.append('1|9|3|1')
    print data1
    data_list.append("\n".join(data1))
    
    # ############
    ins.input = ["T.a", "T.b", "T.c"]
    ins.is_child_sql = False    
    # #############
    
    # ############
    #ins.input = ["max(T.c)", "sum(T.a+T.b)", "sum(T.c)", "count(*)"]
    #ins.is_child_sql = True
    # ###########

    print "input:\t%s" % ins.input
    print "output:\t%s" % ins.output
    print "function:\t%s" % ins.function

    ins.parse_func()
    for e in ins.expr:
        print e
    print "=============="

    t, result, size = ins.aggregate(data_list)
    print result[0].getvalue()

if __name__=="__main__":
    #test()
    main()
    
