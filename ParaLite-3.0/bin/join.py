import sys
import time
import string
import os
import cPickle
import base64
import re
import io
import shlex
import cStringIO
import random
import logging
import traceback
from socket import *
from subprocess import *
import threading
# set the home direcotry of paraLite into system's path
sys.path.append(os.path.abspath(os.path.join(sys.path[0], os.path.pardir)))

import dload_client
import conf
import cqueue
from lib.logger import ParaLiteLog
from lib import ioman, ioman_base
from lib import newparser

def es(s):
    sys.stderr.write("%s%s\n" % (conf.CHILD_ERROR, s))

def ws(s):
    sys.stdout.write("%s%s\n" % (conf.CHILD_OUTPUT, s))

def get_max_size():
    """get the free memory size
    """
    p = Popen(shlex.split("free -m"), stdout=PIPE)
    stdout_list = p.communicate()[0].split('\n')
    s = string.atoi(stdout_list[1].split()[3].strip())*1024*1024
    return s

class JoinOp:
    
    MAX_SIZE = get_max_size()
    
    def __init__(self):
        self.expression = None
        self.dest = None
        self.attrs = {}
        self.output = []
        self.iom = ioman.mk_ioman()
        self.is_running = True
        self.result = {}
        self.result_type = 0
        self.replica_result = {} # the result data for a failed node
        self.failed_node = []
        self.source_data = {} # store the data sending from children {opid:[data]}
        self.total_size = 0
        self.total_time = 0
        self.cur_jobid = None
        self.job_list = []  # store jobid
        self.job_data = {}  # {jobid : data_size}
        self.threads = []
        
        # input is a dic, key is op id and value is input
        self.prediction = []
        self.input = {}
        self.split_key = None
        self.partition_num = 0
        self.cqid = None
        self.client = (None, 0)
        self.master_name = None
        self.master_port = 0
        self.limit = -1
        self.distinct = False
        self.node = (None, 0)
        self.my_port = 0
        self.local_addr = None
        self.p_node = {}
        self.id = None
        self.name = None
        self.num_of_children = 0
        self.status = 0
        self.cost_time = 0
        # each element is a nine tuple 
        #('table1', 'attr1', 'opid1', 'in_pos1', 'op', 'table2', 'attr2', 'opid2','in_pos2')
        self.filter = (None, None, None, 0, None, None, None, None, 0)
        # key is opid, value is the position of attributes that needs to be outputted
        self.out_pos = []
        # store the type of each argument (e.g. x.a, y.b for x.a>y.b)
        
        # store the return value,input and output of filter functions for the
        # other prediction
        self.other_filter = [] #[[input, output, return value]]
        self.key_types = {}
        self.output_dir = None
        self.client_sock = None
        self.dest_db = None
        self.dest_table = None
        self.db_col_sep = None
        self.db_row_sep = None
        self.fashion = None
        self.hash_key = None
        self.hash_key_pos = 0
        self.is_checkpoint = None
        self.log_dir = None
        self.temp_dir = None
        
    def parse_args(self, msg):
        b1 = base64.decodestring(msg)
        dic = cPickle.loads(b1)  
        for key in dic.keys():
            value = dic[key]
            if hasattr(self, key):
                setattr(self, key, value)
        if not os.path.exists(self.temp_dir): os.makedirs(self.temp_dir)

    def parse_others(self):
        m = re.search("([a-zA-Z0-9_]+).([a-zA-Z0-9_ ]+)(>|<|>=|<=|==|=|!=|<>)([ a-zA-Z0-9_]+).([a-zA-Z0-9_]+)", self.expression)
        if not m:
            return 0
        table1 = string.strip(m.group(1))
        attr1 = string.strip(m.group(2))
        table2 = string.strip(m.group(4))
        attr2 = string.strip(m.group(5))
        """
        # self.input is a map:
        <operator_id, [attr1, attr2]>
        """
        ta1 = table1 + '.'+attr1
        ta2 = table2 + '.' + attr2

        for key in self.input:
            i = self.input[key]
            if ta1 in i:
                in_pos1 = i.index(ta1)
                opid1 = key
                self.key_types[key] = self.attrs[ta1]
            elif ta2 in i:
                in_pos2 = i.index(ta2)
                opid2 = key
                ParaLiteLog.debug("key : %s  ta2 : %s" % (key, ta2))
                ParaLiteLog.debug("attrs : %s " % (self.attrs))
                ParaLiteLog.debug("key types : %s " % (self.key_types))
                
                self.key_types[key] = self.attrs[ta2]
        operator = m.group(3)
        self.filter = (table1, attr1, opid1, in_pos1, operator, table2, attr2, opid2, in_pos2)

        # check the postition of output attr. Since there may be more than one
        # predictions, the output for the first join prediction should be the
        # the final result attributes and all related ones in other predictions
        whole_input = []
        whole_input += self.input[self.filter[2]]
        whole_input += self.input[self.filter[7]]
        ParaLiteLog.debug("whole input %s" % whole_input)
        ParaLiteLog.debug("prediction %s" % self.prediction)
        ParaLiteLog.debug("output %s" % self.output)
        
        # set the input and output for predictions if there are more than one
        for i in range(len(self.prediction)):
            self.other_filter.append([[],[],None])
        i = len(self.prediction)-1
        while i > 0:
            output = []
            if i == len(self.prediction)-1:
                for out in self.output:
                    output.append(out)
            else:
                for out in self.other_filter[i+1][1]:
                    output.append(out)
            # parse the predict string and get all related columns
            expr = newparser.parse_prediction_expr(self.prediction[i])
            for var in expr:
                if re.search('^[a-zA-Z][a-zA-Z0-9_.]*$',var):
                    if var not in output:
                        output.append(var)
            self.other_filter[i][0] = output
            self.other_filter[i-1][1] = output
            i -= 1

        self.other_filter[len(self.prediction) - 1][1] = self.output
        self.other_filter[0][0] = whole_input
        if len(self.prediction) > 1:
            self.other_filter[0][1] = self.other_filter[1][0]
        else:
            self.other_filter[0][1] = self.output

        # set the return value of filter functions for predictions
        # if there are more than one
        i = len(self.prediction)-1
        while i > 0:
            expr = newparser.parse_prediction_expr(self.prediction[i])
            new_prediction = self.prediction[i]
            for var in expr:
                if re.search('^[a-zA-Z][a-zA-Z0-9_.]*$',var):
                    pos = self.other_filter[i][0].index(var)
                    if self.attrs[var.strip()] == conf.INT:
                        new_prediction = new_prediction.strip().replace(
                            var, "string.atoi(a[%s])" % pos)
                    elif self.attrs[var.strip()] in [conf.FLOAT, conf.REAL]:
                        new_prediction = new_prediction.strip().replace(
                            var, "float(a[%s])" % pos)
                    else:
                        new_prediction = new_prediction.strip().replace(
                            var, "a[%s]" % pos)
            if new_prediction.find("=") != -1:
                if new_prediction.find(">=") == -1 and new_prediction.find("<=") == -1:
                    new_prediction = new_prediction.replace("=", "==")
            self.other_filter[i][2] = new_prediction
            i -= 1
        output_pos = []
        for o in self.other_filter[0][1]:
            output_pos.append(whole_input.index(o))
        self.out_pos = output_pos    
                
        return 1

    def shell_sort(self, data_dic, sorted_data_dic):
        sep = self.db_col_sep
        if self.db_col_sep == "|":
            sep = "\|"
        proc_dic = {}
        for key in self.key_types.keys():
            key_type = self.key_types[key]
            if self.filter[2] == key:
                key_pos = self.filter[3]
            else:
                key_pos = self.filter[8]
            if key_type != conf.INT and key_type != conf.FLOAT:
                cmd = "sort -k%d,%d -t%s" % ((key_pos + 1),(key_pos + 1), sep)
            else:
                cmd = "sort -nk%d,%d -t%s" % ((key_pos + 1), (key_pos + 1), sep)
            args = shlex.split(cmd)
            p = Popen(args, stdin = PIPE, stdout = PIPE)
            for data in data_dic[key]:
                p.stdin.write(data)
            p.stdin.close()
            sorted_data_dic[key] = []
            for line in p.stdout:
                sorted_data_dic[key].append(string.strip(line))

    def hash_join(self, data_dic):
        """Hash join data

        @param data_dic   it is a dictionary. Key is the id of one of its children and
        value is a list of data coming from different process
        """
        ParaLiteLog.debug("**********HASH JOIN**********")
        hash_table = {}
        # construct a hash table of the first table
        key = self.filter[2]
        key_pos = self.filter[3]
        s_time = time.time()
        for data in data_dic[key]:
            lines = data.split(self.db_row_sep)
            for line in lines:
                if line == "":
                    continue
                cols = line.split(self.db_col_sep)
                join_key = cols[key_pos]
                if join_key not in hash_table:
                    hash_table[join_key] = []
                if len(cols) == 1:
                    # only one column which is a key, so we only keep one record
                    hash_table[join_key] = [cols]
                else:
                    hash_table[join_key].append(cols)
        e = time.time()
        ParaLiteLog.debug("construct first table cost %s" % (e-s_time))
        
        # retrieve the second table to check if it is in the hash table
        key = self.filter[7]
        key_pos = self.filter[8]
        num_of_dest = self.partition_num
        re = []
        sep = self.db_col_sep
        
        if self.dest == conf.DATA_TO_ANO_OP and self.partition_num > 1:
            assert self.split_key is not None
            whole_input = []
            whole_input += self.input[self.filter[2]]
            whole_input += self.input[self.filter[7]]
            t_key_pos = []
            for t_key in self.split_key:
                t_key_pos.append(whole_input.index(t_key))
                
            sep = self.db_col_sep
            data_part_list = []
            for i in range(self.partition_num):
                data_part_list.append(cStringIO.StringIO())
                
            s_time = time.time()
            for data in data_dic[key]:
                lines = data.split(self.db_row_sep)
                for line in lines:
                    if line == "":
                        continue
                    cols = line.split(self.db_col_sep)
                    join_key = cols[key_pos]
                    if join_key in hash_table:
                        for first_table in hash_table[join_key]:
                            whole_attr = first_table + cols  
                            s = sep.join(whole_attr[pos] for pos in self.out_pos)
                            part_id = abs(hash(
                                sep.join(str(whole_attr[p]) for p in t_key_pos))) % num_of_dest
                            data_part_list[part_id].write(s + self.db_row_sep)
            e = time.time()
            ParaLiteLog.debug("sencod table cost %s" % (e-s_time))
            re = data_part_list
            re_type = conf.MULTI_BUFFER
            
        else:
            result = cStringIO.StringIO()
            for data in data_dic[key]:
                lines = data.split(self.db_row_sep)
                for line in lines:
                    if line == "":
                        continue
                    cols = line.split(self.db_col_sep)
                    join_key = cols[key_pos]
                    if join_key in hash_table:
                        for first_table in hash_table[join_key]:
                            whole_attr = first_table + cols  
                            s = sep.join(whole_attr[pos] for pos in self.out_pos)
                            result.write(s + self.db_row_sep)
            re = [result]
            re_type = conf.MULTI_BUFFER

        if len(self.other_filter) > 1:
            re, t_size = self.process_other_prediction(re)
        else:
            t_size = 0
            for r in re:
                t_size += len(r.getvalue())
        
        del hash_table

        return re_type, re, t_size
        
    def sort_data(self, data_dic, sorted_data_dic):
        for key in self.key_types.keys():
            # get all data (data may not ends with the separator)
            wd = ""
            for data in data_dic[key]:
                wd += data
            del data_dic[key]
            data_list = wd.strip().split(self.db_row_sep)
            # delete the last null element
            if data_list[-1] == '':
                data_list.pop()
                
            key_type = self.key_types[key]
            if self.filter[2] == key:
                key_pos = self.filter[3]
            else:
                key_pos = self.filter[8]
            s = time.time()

            if key_type != conf.INT and key_type != conf.REAL and key_type != conf.FLOAT:
                data_list.sort(key = lambda l: l.split(self.db_col_sep)[key_pos])
            else:
                data_list.sort(
                    key = lambda l: string.atoi(l.split(self.db_col_sep)[key_pos]))
            e = time.time()
            ParaLiteLog.debug("sort cost %s" % (e-s))
            sorted_data_dic[key] = data_list
            del data_list

    def join(self, data_dic):
        operator = self.filter[4]

        if operator == '=' or operator == '==':
            re_type, re, t_size = self.hash_join(data_dic)
            return re_type, re, t_size
        else:
            return self.sort_join(data_dic)
        
    def sort_join(self, data_dic):
        ParaLiteLog.debug("**********SORT JOIN********")
        # shell sort can only support one-character separator and single-line record
        ParaLiteLog.debug("**SORT DATA: start***")
        sorted_data_dic = {}

        self.sort_data(data_dic, sorted_data_dic)


        # if len(self.db_col_sep) == 1 and self.db_row_sep == "\n":
        #     self.shell_sort(data_dic, sorted_data_dic)
        # else:
        #     self.quick_sort(data_dic, sorted_data_dic)
        
        ParaLiteLog.debug("**SORT DATA: finish***")
        sort_end = time.time()

        data1 = sorted_data_dic[self.filter[2]]
        data2 = sorted_data_dic[self.filter[7]]

        re = []
        
        re_type = None
        if self.dest == conf.DATA_TO_ANO_OP and self.partition_num > 1:
            data_part_list = []
            for i in range(self.partition_num):
                data_part_list.append(cStringIO.StringIO())
            self.sort_join_2(data1, data2, data_part_list)
            re = data_part_list
            re_type = conf.MULTI_BUFFER
        else:
            result = cStringIO.StringIO()
            self.sort_join_1(data1, data2, result)
            re = [result]
            re_type = conf.SINGLE_BUFFER
        
        re, t_size = self.process_other_prediction(re)
        del data1, data2
        
        return re_type, re, t_size

    def process_other_prediction(self, re):
        # sometimes, there are more than one predications. The first one should
        # be handled by joining data while others only needs to be satisfied for
        # each result record.
        new_result = []
        t_size = 0
        for result in re:
            temp = []
            for line in result.getvalue().strip().split(self.db_row_sep):
                if line == "":
                    continue
                temp.append(line.strip().split(self.db_col_sep))
            new_result.append(temp)

        for i in range(1, len(self.other_filter)):
            # define a filter function
            t_filter = compile(
                "import string\ndef f(a):\n\treturn %s" % self.other_filter[i][2], "<string>", "exec")
            exec t_filter in locals()

            new_new_result = []
            for r in new_result:
                new = filter(f, r)
                new_new_result.append(new)

            del new_result
            new_result = new_new_result

        # adjust the final output: if there are more than one prediction,
        # the final output may be different from the output of final prediction
        re = []
        if len(self.other_filter) > 1 and len(self.output) != len(self.other_filter[-1][0]):
            out_pos = []
            for out in self.output:
                out_pos.append(self.other_filter[-1][1].index(out))

            for r in new_result:
                csio = cStringIO.StringIO()
                for re_re in r:
                    csio.write(self.db_col_sep.join(re_re[pos] for pos in out_pos))
                    csio.write(self.db_row_sep)
                re.append(csio)
                t_size += len(csio.getvalue())
        else:
            for r in new_result:
                csio = cStringIO.StringIO()
                for re_re in r:
                    csio.write(self.db_col_sep.join(re_re))
                    csio.write(self.db_row_sep)
                re.append(csio)
                t_size += len(csio.getvalue())
        self.status = conf.FINISH
        return re, t_size
            
    def sort_join_1(self, data1, data2, result):
        """ join data, the result does not need to be hash partitioned
        """
        operator = self.filter[4]
        sep = self.db_col_sep
        count_1 = 0
        count_2 = 0

        if operator == '=' or operator == '==':
            key_type = self.key_types[self.filter[2]]

            if key_type == conf.INT:
                while True:
                    if count_1 == len(data1) or count_2 == len(data2):
                        break
                    attr1 = data1[count_1].split(sep)
                    attr2 = data2[count_2].split(sep)
            
                    arg1 = string.atoi(attr1[self.filter[3]])
                    arg2 = string.atoi(attr2[self.filter[8]])

                    if arg1 == arg2:
                        whole_attr = attr1 + attr2
                        s = sep.join(whole_attr[pos] for pos in self.out_pos)
                        result.write(s + self.db_row_sep)
                        s_pos = count_2
                        while True:
                            s_pos += 1
                            if s_pos == len(data2):
                                break
                            attr2 = data2[s_pos].split(sep)
                            arg2 = string.atoi(attr2[self.filter[8]])
                            if arg1 != arg2:
                                break

                            whole_attr = attr1 + attr2
                            s = sep.join(whole_attr[pos] for pos in self.out_pos)
                            result.write(s + self.db_row_sep)

                        count_1 += 1
                    elif arg1 < arg2:
                        count_1 += 1
                    else:
                        count_2 += 1
            elif key_type == conf.FLOAT or key_type == conf.REAL:
                while True:
                    if count_1 == len(data1) or count_2 == len(data2):
                        break
                    attr1 = data1[count_1].split(sep)
                    attr2 = data2[count_2].split(sep)
                    arg1 = float(attr1[self.filter[3]])
                    arg2 = float(attr2[self.filter[8]])

                    if arg1 == arg2:
                        whole_attr = attr1 + attr2
                        s = sep.join(whole_attr[pos] for pos in self.out_pos)
                        result.write(s + self.db_row_sep)
                        s_pos = count_2
                        while True:
                            s_pos += 1
                            if s_pos == len(data2):
                                break
                            attr2 = data2[s_pos].split(sep)
                            arg2 = float(attr2[self.filter[8]])
                            if arg1 != arg2:
                                break

                            whole_attr = attr1 + attr2
                            s = sep.join(whole_attr[pos] for pos in self.out_pos)
                            result.write(s + self.db_row_sep)

                        count_1 += 1
                    elif arg1 < arg2:
                        count_1 += 1
                    else:
                        count_2 += 1
            else:
                while True:
                    if count_1 == len(data1) or count_2 == len(data2):
                        break
                    attr1 = data1[count_1].split(sep)
                    attr2 = data2[count_2].split(sep)
                    arg1 = attr1[self.filter[3]]
                    arg2 = attr2[self.filter[8]]

                    if arg1 == arg2:
                        whole_attr = attr1 + attr2
                        s = sep.join(whole_attr[pos] for pos in self.out_pos)
                        result.write(s + self.db_row_sep)
                        s_pos = count_2
                        while True:
                            s_pos += 1
                            if s_pos == len(data2):
                                break
                            attr2 = data2[s_pos].split(sep)
                            arg2 = string.atoi(attr2[self.filter[8]])
                            if arg1 != arg2:
                                break

                            whole_attr = attr1 + attr2
                            s = sep.join(whole_attr[pos] for pos in self.out_pos)
                            result.write(s + self.db_row_sep)

                        count_1 += 1
                    elif arg1 < arg2:
                        count_1 += 1
                    else:
                        count_2 += 1

        elif operator == '<>' or operator == '!=':
            for i in range(len(data1)):
                attr1 = data1[i].split(sep)
                for j in range(1, len(data2)):
                    attr2 = data2[j].split(sep)
                    arg1 = attr1[self.filter[3]]
                    arg2 = attr2[self.filter[8]]
                    if arg1 == arg2:
                        continue
                    else:
                        whole_attr = attr1 + attr2
                        s = sep.join(whole_attr[pos] for pos in self.out_pos)
                        result.write(s + self.db_row_sep)
                        
        elif operator == '>':
            while True:
                if count_1 == len(data1) or count_2 == len(data2):
                    break
                line1 = data1[count_1]
                line2 = data2[count_2]
                attr1 = line1.split(sep)
                attr2 = line2.split(sep)
                arg1 = attr1[self.filter[3]]
                arg2 = attr2[self.filter[8]]
                if arg1 > arg2:
                    count_2 += 1
                else:
                    """
                    Once count_1 will be changed, <A[count_1], B[0..count_2-1]> 
                    shoule be outputted because all elements (index is smaller 
                    than count_2) in B are smalled than A[count_1]
                    """
                    for i in range(count_2):
                        attr = data2[i].split(sep)

                        whole_attr = attr1 + attr
                        s = sep.join(whole_attr[pos] for pos in self.out_pos)
                        result.write(s + self.db_row_sep)

                    if arg1 == arg2:
                        count_1 += 1
                        count_2 += 1
                    if arg1 < arg2:
                        count_1 += 1
            if count_1 != len(data1):
                for i in range(count_1, len(data1)):
                    attr1 = data1[i].split(sep)
                    for j in range(len(data2)):
                        attr2 = data2[j].split(sep)
                        
                        whole_attr = attr1 + attr2
                        s = sep.join(whole_attr[pos] for pos in self.out_pos)
                        result.write(s + self.db_row_sep)
                        
        elif operator == '>=':
            while True:
                if count_1 == len(data1) or count_2 == len(data2):
                    break
                line1 = data1[count_1]
                line2 = data2[count_2]
                attr1 = line1.split(sep)
                attr2 = line2.split(sep)
                arg1 = attr1[self.filter[3]]
                arg2 = attr2[self.filter[8]]
                if arg1 > arg2:
                    count_2 += 1
                else:
                    if arg1 == arg2:
                        """
                        the diffrence between '>' is when arg1 == arg2, B[count_2]
                        should be included
                        """
                        for i in range(count_2 + 1):
                            attr = data2[i].split(sep)

                            whole_attr = attr1 + attr
                            s = sep.join(whole_attr[pos] for pos in self.out_pos)
                            result.write(s + self.db_row_sep)
                            
                        count_1 += 1
                        count_2 += 1
                    if arg1 < arg2:
                        for i in range(count_2):
                            attr = data2[i].split(sep)

                            whole_attr = attr1 + attr
                            s = sep.join(whole_attr[pos] for pos in self.out_pos)
                            result.write(s + self.db_row_sep)

                        count_1 += 1
            if count_1 != len(data1):
                for i in range(count_1, len(data1)):
                    attr1 = data1[i].split(sep)
                    for j in range(len(data2)):
                        attr2 = data2[j].split(sep)

                        whole_attr = attr1 + attr
                        s = sep.join(whole_attr[pos] for pos in self.out_pos)
                        result.write(s + self.db_row_sep)
                        
        elif operator == '<':
            while True:
                if count_1 == len(data1) or count_2 == len(data2):
                    break
                line1 = data1[count_1]
                line2 = data2[count_2]
                attr1 = line1.split(sep)
                attr2 = line2.split(sep)
                arg1 = attr1[self.filter[3]]
                arg2 = attr2[self.filter[8]]
                if arg1 > arg2:
                    count_2 += 1
                else:
                    if arg1 < arg2:
                        """
                        all elements in B whose index are equal or larger than 
                        count_2 should be outputted
                        """
                        for i in range(count_2, len(data2)):
                            attr = data2[i].split(sep)

                            whole_attr = attr1 + attr
                            s = sep.join(whole_attr[pos] for pos in self.out_pos)
                            result.write(s + self.db_row_sep)
                            
                        count_1 += 1
                    else:
                        """
                        all elements in B whose index are larger than count_2 
                        should be outputted
                        """
                        for i in range(count_2 + 1, len(data2)):
                            attr = data2[i].split(sep)

                            whole_attr = attr1 + attr
                            s = sep.join(whole_attr[pos] for pos in self.out_pos)
                            result.write(s + self.db_row_sep)
                            
                        count_1 += 1
                        count_2 += 1

        elif operator == '<=':
            while True:
                if count_1 == len(data1) or count_2 == len(data2):
                    break
                line1 = data1[count_1]
                line2 = data2[count_2]
                attr1 = line1.split(sep)
                attr2 = line2.split(sep)
                arg1 = attr1[self.filter[3]]
                arg2 = attr2[self.filter[8]]
                if arg1 > arg2:
                    count_2 += 1
                else:
                    """
                    all elements in B whose index are equal or larger than count_2 
                    should be outputted
                    """
                    for i in range(count_2, len(data2)):
                        attr = data2[i].split(sep)

                        whole_attr = attr1 + attr
                        s = sep.join(whole_attr[pos] for pos in self.out_pos)
                        result.write(s + self.db_row_sep)
                        
                    if arg1 < arg2:
                        count_1 += 1
                    else:
                        count_1 += 1
                        count_2 += 1

    def sort_join_2(self, data1, data2, data_part_list):
        """ join data, the result needs to be hash partitioned
        """
        # get the position of hash key in the output
        assert self.split_key is not None
        whole_input = []
        whole_input += self.input[self.filter[2]]
        whole_input += self.input[self.filter[7]]
        key_pos = []
        for key in self.split_key:
            key_pos.append(whole_input.index(key))

        ParaLiteLog.debug("split key = %s" % str(self.split_key))
        ParaLiteLog.debug("output = %s" % str(self.output))
        ParaLiteLog.debug("out pos = %s" % str(self.out_pos))
        
        operator = self.filter[4]
        sep = self.db_col_sep
        count_1 = 0
        count_2 = 0
        num_of_dest = self.partition_num
        
        if operator == '=' or operator == '==':
            
            key_type = self.key_types[self.filter[2]]
            if key_type == conf.INT:
                while True:
                    if count_1 == len(data1) or count_2 == len(data2):
                        break
                    attr1 = data1[count_1].split(sep)
                    attr2 = data2[count_2].split(sep)
                    arg1 = string.atoi(attr1[self.filter[3]])
                    arg2 = string.atoi(attr2[self.filter[8]])

                    if arg1 == arg2:
                        whole_attr = attr1 + attr2
                        s = sep.join(whole_attr[pos] for pos in self.out_pos)
                        part_id = abs(hash(
                            sep.join(str(whole_attr[p]) for p in key_pos))) % num_of_dest

                        data_part_list[part_id].write(s + self.db_row_sep)

                        s_pos = count_2
                        while True:
                            s_pos += 1
                            if s_pos == len(data2):
                                break
                            attr2 = data2[s_pos].split(sep)
                            arg2 = string.atoi(attr2[self.filter[8]])
                            if arg1 != arg2:
                                break

                            whole_attr = attr1 + attr2
                            s = sep.join(whole_attr[pos] for pos in self.out_pos)
                            part_id = abs(hash(sep.join(str(whole_attr[p]) for p in key_pos))) % num_of_dest
                            data_part_list[part_id].write(s + self.db_row_sep)

                        count_1 += 1
                    elif arg1 < arg2:
                        count_1 += 1
                    else:
                        count_2 += 1
            elif key_type == conf.FLOAT or key_type == conf.REAL:
                while True:
                    if count_1 == len(data1) or count_2 == len(data2):
                        break
                    attr1 = data1[count_1].split(sep)
                    attr2 = data2[count_2].split(sep)
                    arg1 = float(attr1[self.filter[3]])
                    arg2 = float(attr2[self.filter[8]])

                    if arg1 == arg2:
                        whole_attr = attr1 + attr2
                        s = sep.join(whole_attr[pos] for pos in self.out_pos)
                        part_id = abs(hash(
                            sep.join(str(whole_attr[p]) for p in key_pos))) % num_of_dest

                        data_part_list[part_id].write(s + self.db_row_sep)

                        s_pos = count_2
                        while True:
                            s_pos += 1
                            if s_pos == len(data2):
                                break
                            attr2 = data2[s_pos].split(sep)
                            arg2 = float(attr2[self.filter[8]])
                            if arg1 != arg2:
                                break

                            whole_attr = attr1 + attr2
                            s = sep.join(whole_attr[pos] for pos in self.out_pos)
                            part_id = abs(hash(sep.join(str(whole_attr[p]) for p in key_pos))) % num_of_dest
                            data_part_list[part_id].write(s + self.db_row_sep)

                        count_1 += 1
                    elif arg1 < arg2:
                        count_1 += 1
                    else:
                        count_2 += 1
            else:
                while True:
                    if count_1 == len(data1) or count_2 == len(data2):
                        break
                    attr1 = data1[count_1].split(sep)
                    attr2 = data2[count_2].split(sep)
                    arg1 = attr1[self.filter[3]]
                    arg2 = attr2[self.filter[8]]

                    if arg1 == arg2:
                        whole_attr = attr1 + attr2
                        s = sep.join(whole_attr[pos] for pos in self.out_pos)
                        part_id = abs(hash(
                            sep.join(str(whole_attr[p]) for p in key_pos))) % num_of_dest

                        data_part_list[part_id].write(s + self.db_row_sep)

                        s_pos = count_2
                        while True:
                            s_pos += 1
                            if s_pos == len(data2):
                                break
                            attr2 = data2[s_pos].split(sep)
                            arg2 = attr2[self.filter[8]]
                            if arg1 != arg2:
                                break

                            whole_attr = attr1 + attr2
                            s = sep.join(whole_attr[pos] for pos in self.out_pos)
                            part_id = abs(hash(sep.join(str(whole_attr[p]) for p in key_pos))) % num_of_dest
                            data_part_list[part_id].write(s + self.db_row_sep)

                        count_1 += 1
                    elif arg1 < arg2:
                        count_1 += 1
                    else:
                        count_2 += 1
                
        elif operator == '<>' or operator == '!=':
            for i in range(len(data1)):
                attr1 = data1[i].split(sep)
                for j in range(1, len(data2)):
                    attr2 = data2[j].split(sep)
                    arg1 = attr1[self.filter[3]]
                    arg2 = attr2[self.filter[8]]
                    if arg1 == arg2:
                        continue
                    else:
                        whole_attr = attr1 + attr2
                        s = sep.join(whole_attr[pos] for pos in self.out_pos)
                        part_id = abs(hash(
                            sep.join(str(whole_attr[p]) for p in key_pos))) % num_of_dest
                        data_part_list[part_id].write(s + self.db_row_sep)
                        
        elif operator == '>':
            while True:
                if count_1 == len(data1) or count_2 == len(data2):
                    break
                line1 = data1[count_1]
                line2 = data2[count_2]
                attr1 = line1.split(sep)
                attr2 = line2.split(sep)
                arg1 = attr1[self.filter[3]]
                arg2 = attr2[self.filter[8]]
                if arg1 > arg2:
                    count_2 += 1
                else:
                    """
                    Once count_1 will be changed, <A[count_1], B[0..count_2-1]> 
                    shoule be outputted because all elements (index is smaller 
                    than count_2) in B are smalled than A[count_1]
                    """
                    for i in range(count_2):
                        attr = data2[i].split(sep)

                        whole_attr = attr1 + attr
                        s = sep.join(whole_attr[pos] for pos in self.out_pos)
                        part_id = abs(hash(
                            sep.join(str(whole_attr[p]) for p in key_pos))) % num_of_dest
                        data_part_list[part_id].write(s + self.db_row_sep)

                    if arg1 == arg2:
                        count_1 += 1
                        count_2 += 1
                    if arg1 < arg2:
                        count_1 += 1
            if count_1 != len(data1):
                for i in range(count_1, len(data1)):
                    attr1 = data1[i].split(sep)
                    for j in range(len(data2)):
                        attr2 = data2[j].split(sep)

                        whole_attr = attr1 + attr2
                        s = sep.join(whole_attr[pos] for pos in self.out_pos)
                        part_id = abs(hash(
                            sep.join(str(whole_attr[p]) for p in key_pos))) % num_of_dest
                        data_part_list[part_id].write(s + self.db_row_sep)
                        
        elif operator == '>=':
            while True:
                if count_1 == len(data1) or count_2 == len(data2):
                    break
                line1 = data1[count_1]
                line2 = data2[count_2]
                attr1 = line1.split(sep)
                attr2 = line2.split(sep)
                arg1 = attr1[self.filter[3]]
                arg2 = attr2[self.filter[8]]
                if arg1 > arg2:
                    count_2 += 1
                else:
                    if arg1 == arg2:
                        """
                        the diffrence between '>' is when arg1 == arg2, B[count_2]
                        should be included
                        """
                        for i in range(count_2 + 1):
                            attr = data2[i].split(sep)

                            whole_attr = attr1 + attr
                            s = sep.join(whole_attr[pos] for pos in self.out_pos)
                            part_id = abs(hash(
                                sep.join(str(whole_attr[p]) for p in key_pos))) % num_of_dest
                            data_part_list[part_id].write(s + self.db_row_sep)
                            
                        count_1 += 1
                        count_2 += 1
                    if arg1 < arg2:
                        for i in range(count_2):
                            attr = data2[i].split(sep)

                            whole_attr = attr1 + attr
                            s = sep.join(whole_attr[pos] for pos in self.out_pos)
                            part_id = abs(hash(
                                sep.join(str(whole_attr[p]) for p in key_pos))) % num_of_dest
                            data_part_list[part_id].write(s + self.db_row_sep)

                        count_1 += 1
            if count_1 != len(data1):
                for i in range(count_1, len(data1)):
                    attr1 = data1[i].split(sep)
                    for j in range(len(data2)):
                        attr2 = data2[j].split(sep)

                        whole_attr = attr1 + attr2
                        s = sep.join(whole_attr[pos] for pos in self.out_pos)
                        part_id = abs(hash(
                            sep.join(str(whole_attr[p]) for p in key_pos))) % num_of_dest
                        data_part_list[part_id].write(s + self.db_row_sep)
                        

        elif operator == '<':
            while True:
                if count_1 == len(data1) or count_2 == len(data2):
                    break
                line1 = data1[count_1]
                line2 = data2[count_2]
                attr1 = line1.split(sep)
                attr2 = line2.split(sep)
                arg1 = attr1[self.filter[3]]
                arg2 = attr2[self.filter[8]]
                if arg1 > arg2:
                    count_2 += 1
                else:
                    if arg1 < arg2:
                        """
                        all elements in B whose index are equal or larger than 
                        count_2 should be outputted
                        """
                        for i in range(count_2, len(data2)):
                            attr = data2[i].split(sep)

                            whole_attr = attr1 + attr
                            s = sep.join(whole_attr[pos] for pos in self.out_pos)
                            part_id = abs(hash(
                                sep.join(str(whole_attr[p]) for p in key_pos))) % num_of_dest
                            data_part_list[part_id].write(s + self.db_row_sep)
                            
                        count_1 += 1
                    else:
                        """
                        all elements in B whose index are larger than count_2 
                        should be outputted
                        """
                        for i in range(count_2 + 1, len(data2)):
                            attr = data2[i].split(sep)

                            whole_attr = attr1 + attr
                            s = sep.join(whole_attr[pos] for pos in self.out_pos)
                            part_id = abs(hash(
                                sep.join(str(whole_attr[p]) for p in key_pos))) % num_of_dest
                            data_part_list[part_id].write(s + self.db_row_sep)
                            
                        count_1 += 1
                        count_2 += 1

        elif operator == '<=':
            while True:
                if count_1 == len(data1) or count_2 == len(data2):
                    break
                line1 = data1[count_1]
                line2 = data2[count_2]
                attr1 = line1.split(sep)
                attr2 = line2.split(sep)
                arg1 = attr1[self.filter[3]]
                arg2 = attr2[self.filter[8]]
                if arg1 > arg2:
                    count_2 += 1
                else:
                    """
                    all elements in B whose index are equal or larger than count_2 
                    should be outputted
                    """
                    for i in range(count_2, len(data2)):
                        attr = data2[i].split(sep)
                        
                        whole_attr = attr1 + attr
                        s = sep.join(whole_attr[pos] for pos in self.out_pos)
                        part_id = abs(hash(
                            sep.join(str(whole_attr[p]) for p in key_pos))) % num_of_dest
                        data_part_list[part_id].write(s + self.db_row_sep)
                        
                    if arg1 < arg2:
                        count_1 += 1
                    else:
                        count_1 += 1
                        count_2 += 1


    def quick_sort(self, data_dic, sorted_data_dic):
        proc_dic = {}
        c = [cStringIO.StringIO(), cStringIO.StringIO()]
        i = 0
        for key in self.key_types.keys():
            key_type = self.key_types[key]
            if self.filter[2] == key:
                key_pos = self.filter[3]
            else:
                key_pos = self.filter[8]
            if key_type != conf.INT or key_type != conf.FLOAT:
                para = "-s"
            else:
                para = "-n"
            for data in data_dic[key]:
                c[i].write(data)
            sorted_list = c[i].getvalue().split(self.db_row_sep)
            # delete the last null element
            if sorted_list[len(sorted_list) - 1] == '':
                sorted_list.pop()
            self.i_quick_sort(sorted_list, 0, len(sorted_list) - 1, key_pos, para)
            sorted_data_dic[key] = sorted_list
            i += 1
        
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
        
    def send_rs_info_to_master(self, total_size, total_time):
        # RS:DATANODE:cqid:opid:node:port:rs_type:partition_num:total_size:total_time
        addr = (self.master_name, self.master_port)
        sock = socket(AF_INET, SOCK_STREAM)
        sock.connect(addr)
        sep = conf.SEP_IN_MSG
        job_size = base64.encodestring(cPickle.dumps(self.job_data))        
        m = [conf.RS, conf.DATA_NODE, self.cqid, self.opid, gethostname(),
             str(self.my_port),str(self.result_type), 
             str(self.partition_num), str(total_size), str(total_time), job_size]
        msg = sep.join(m)
        sock.send('%10s%s' % (len(msg), msg))
        
    def distribute_data(self):
        whole_data = cStringIO.StringIO()
        for i in self.result:
            for csio in self.result[i]:
                d = string.strip(csio.getvalue())
                if len(d) == 0:
                    continue
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

        columns = self.output
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

            ParaLiteLog.info("proc_select: load data start")
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

            # Start socket server for local connections
            self.local_addr = "/tmp/paralite-local-addr-join-%s-%s-%s" % (
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

            ParaLiteLog.info("--join node %s on %s is finished--" % (self.opid,
                                                                    gethostname()))

        except KeyboardInterrupt, e:
            self.report_error("ParaLite receives a interrupt signal and then will close the process\n")
            ParaLiteLog.info("--join node %s on %s is finished--" % (self.opid,
                                                                     gethostname()))
            sys.exit(1)
        except Exception, e1:
            ParaLiteLog.debug(traceback.format_exc())
            self.report_error(traceback.format_exc())
            sys.exit(1)


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
        if len(source_data) != 2:
            return False
        for opid in source_data:
            if len(source_data[opid]) < num_of_s - len(self.failed_node):
                return False
        return True
    
    def report_error(self, err):
        sock = socket(AF_INET, SOCK_STREAM)
        sock.connect((self.master_name, self.master_port))
        sock.send("%10s%s" % (len(err), err))
        sock.close()

    def get_data_by_part_id(self, result, part_id):
        rs = ""
        if part_id not in result:
            return rs
        
        for part in result[part_id]:
            if isinstance(part, str):
                # data is stored in file
                f_name = self.get_file_name_by_data_id(gethostname(), part_id)
                f = open(f_name, "rb")
                rs += f.read()
                f.close()
            else:
                # data is stored in buffer
                rs += part.getvalue()
        return rs

    def process_ck_info(self, message):
        # message --> DATA_PERSIST:CHECKPOINT[:REPLICA_NODE:REPLICA_PORT]
        is_ck = message[1]
        self.is_checkpoint = is_ck
        if is_ck == conf.CHECKPOINT:
            replica_addr = (message[2], string.atoi(message[3]))
            if self.result_type != conf.MULTI_FILE:
                for dataid in self.result:
                    ParaLiteLog.debug("dataid %s" % dataid)
                    ds = ""
                    for data in self.result[dataid]:
                        ds += data.getvalue()
                    self.write_data_to_disk(dataid, ds)
                    msg = conf.SEP_IN_MSG.join(
                        [conf.DATA_REPLICA, gethostname(), str(dataid), ds])
                    
                    f_name = self.get_file_name_by_data_id(gethostname(), dataid)
                    cmd = "scp %s %s:%s" % (f_name, replica_addr[0], f_name)
                    ParaLiteLog.debug("CDM: %s" % cmd)
                    os.system(cmd)
                    #self.send_data_to_node(msg, AF_INET, replica_addr)
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
            self.temp_dir, "join", self.opid, node, data_id)

    def send_data_to_node(self, msg, tp, addr):
        sock = socket(tp, SOCK_STREAM)
        try:
            sock.connect(addr)
            sock.send("%10s%s" % (len(msg),msg))
            sock.close()
        except Exception, e:
            raise(Exception(e))

    def recovery_data(self, result, node):
        if self.dest == conf.DATA_TO_ANO_OP and num_of_dest > 1:
            for partid in result:
                f_name = self.get_file_name_by_data_id(node, partid)
                result[partid] = [f_name]
        else:
            for jobid in self.job_list:
                f_name = self.get_file_name_by_data_id(node, jobid)
                result[jobid] = [f_name]

    def handle_read(self, event):
        message = event.data[10:]
        sep = conf.SEP_IN_MSG
        m = message.split(sep)
        try:        
            if m[0] == conf.JOB_ARGUMENT:
                self.parse_args(m[1])
                ParaLiteLog.info("parse arguments: FINISH")
                # init the persisted result data
                if self.is_checkpoint is not None and self.is_checkpoint == conf.CHECKPOINT:
                    ParaLiteLog.debug("recovery data: START")
                    # this is a recovery operator
                    self.recovery_data(self.result, gethostname())
                    ParaLiteLog.debug("recovery data: FINISH")
                    self.send_rs_info_to_master(0, 0)
                else:
                    self.parse_others()
                    # delete all temporary files for this operator
                    os.system("rm -f %s/%s_%s" % (self.temp_dir, "join", self.opid))

            elif m[0] == conf.JOB:
                ParaLiteLog.debug("**********JOB %s**********" % m[1])
                self.cur_jobid = m[1]
                # FAULT TOLERANCE:
                if self.cur_jobid in self.job_list:
                    # this is a failed job, we should first delete the old result value
                    if self.dest == conf.DATA_TO_ANO_OP and self.partition_num > 1:
                        for partid in self.result:
                            pos = self.job_list.index(self.cur_jobid)
                            self.result[partid][pos] = ""
                    else:
                        self.result[self.cur_jobid] = ""
                
            elif m[0] == conf.DATA:
                data_id = string.strip(m[1][0:2])
                ParaLiteLog.debug("MESSAGE: DATA from operator %s" % (data_id))
                data = m[1][2:]
                if data_id not in self.source_data:
                    self.source_data[data_id] = [data]
                else:
                    self.source_data[data_id].append(data)

                # join data
                if not self.is_data_ready(self.source_data, self.num_of_children):
                    return

                s = 0
                for d in self.source_data:
                    for data in self.source_data[d]:
                        s += len(data)
                ParaLiteLog.debug("the source data size : %s" % s)
                s_time = time.time()
                rs_type, rs, t_size = self.join(self.source_data)
                ParaLiteLog.debug("*****JOIN DATA*****: finish")
                
                ParaLiteLog.debug("len of rs: %s" % len(rs))
                
                # FAULT TOLERANCE:
                if self.cur_jobid in self.job_data:
                    # this is a failed job
                    if self.dest == conf.DATA_TO_ANO_OP and self.partition_num > 1:
                        for partid in self.result:
                            pos = self.job_list.index(self.cur_jobid)
                            self.result[partid][pos] = rs[partid]
                    else:
                        self.result[self.cur_jobid] = rs
                    self.send_status_to_master(self.cur_jobid, conf.PENDING)
                    return

                del self.source_data
                self.total_size += t_size
                self.job_data[self.cur_jobid] = t_size
                self.job_list.append(self.cur_jobid)
                self.source_data = {}

                # store the result of one job to the final result
                if len(rs) == 1:
                    if self.dest == conf.DATA_TO_ANO_OP or self.dest == conf.DATA_TO_DB:
                        # dest is AGGR op or ORDER op, use 0 as the key
                        if 0 not in self.result:
                            self.result[0] = rs
                        else:
                            self.result[0].append(rs[0])
                        if self.is_checkpoint == 1:
                            self.write_data_to_disk(0, rs[0].getvalue())
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
                    if self.is_checkpoint == 1:
                        for i in range(len(rs)):
                            self.write_data_to_disk(i, rs[i].getvalue())

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
                ParaLiteLog.debug("sending PENDING to the master")                    
            elif m[0] == conf.JOB_END:
                ParaLiteLog.debug("MESSAGE: %s" % message)
                # all jobs are finished
                self.send_rs_info_to_master(self.total_size, self.total_time)
                
                # distribute data
                if self.dest == conf.DATA_TO_ONE_CLIENT:
                    self.distribute_data()
                    self.send_status_to_master(" ".join(self.job_list), conf.ACK)
                    self.is_running = False
                elif self.dest == conf.DATA_TO_DB:
                    self.distribute_data()

            elif m[0] == conf.DATA_PERSIST:
                ParaLiteLog.debug("MESSAGE: %s" % message)
                # if the data is requried to be persisted or not
                self.process_ck_info(m)
                
            elif m[0] == conf.DATA_REPLICA:
                # message --> DATA_REPLICA:DATANODE:DATAID:DATA
                datanode, dataid = m[1:3]
                f_name = self.get_file_name_by_data_id(gethostname(), dataid)
                fr = open(f_name, "wa")
                fr.write(m[3])
                fr.close()
                        
            elif m[0] == conf.DATA_DISTRIBUTE:
                # send a part of data to the next operator
                # DATA_DISTRIBUTE:partition_num:destnode
                ParaLiteLog.debug("MESSAGE: %s" % message)                
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

            elif m[0] == conf.DATA_DISTRIBUTE_UDX:
                # send data to udx client
                # m[1:] = worker.id:(node:port | addr):size
                
                if len(m) == 5:
                    w_id = m[1]
                    addr = (m[2], string.atoi(m[3]))
                    t = AF_INET
                    bk = string.atoi(m[4])
                elif len(m) == 4:
                    w_id = m[1]
                    addr = m[2]
                    t = AF_UNIX
                    bk = string.atoi(m[3])
                data = self.get_data_by_blocksize(bk)
                if not data:
                    # if we don't send something here, udx will not send KAL
                    # again, and then they will not receive data again, the whole
                    # process will be blocked for ever
                    msg = sep.join([conf.DATA, "EMPTY"])
                else:
                    msg = sep.join([conf.DATA, data])
                self.send_data_to_node(msg, t, addr)

            elif m[0] == conf.DLOAD_REPLY:
                reply = sep.join(m[1:])
                ParaLiteLog.info("receive the information from the master")
                ParaLiteLog.debug(reply)
                
                if len(self.data.getvalue()) != 0:
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
                # all data is dipatched to the parent nodes
                self.send_status_to_master(" ".join(self.job_list), conf.ACK)
                ParaLiteLog.debug("notify ACK to master")                    
                self.is_running = False
                
            elif message == conf.END_TAG:
                self.send_status_to_master(" ".join(self.job_list), conf.ACK)
                self.is_running = False
                
            elif message == conf.EXIT:
                self.is_running = False
                
            elif m[0] == conf.NODE_FAIL:
                ParaLiteLog.debug("MESSAGE: %s" % message)
                # message --> NODE_FAIL:FAILED_NODE:REPLICA_NODE
                failed_node, replica_node = m[1:3]
                self.failed_node.append(failed_node)
                if replica_node != "" and replica_node == gethostname():
                    # load replica data for the failed node
                    self.recovery_data(self.replica_result, replica_node)
                ParaLiteLog.debug("Finish to handle node failure message")
            
        except Exception, e:
            es(traceback.format_exc())
            ParaLiteLog.info(traceback.format_exc())
            self.is_running = False
            self.no_error = False

        
def test():
    ins = join()
    ins.sort()

def main():
    
    if len(sys.argv) != 7:
        sys.exit(1)
    proc = JoinOp()
    proc.master_name = sys.argv[1]
    proc.master_port = string.atoi(sys.argv[2])
    proc.cqid = sys.argv[3]
    proc.opid = sys.argv[4]
    proc.my_port = string.atoi(sys.argv[5])
    proc.log_dir = sys.argv[6]
    if not os.path.exists(proc.log_dir): os.makedirs(proc.log_dir)
    cur_time = time.strftime('%Y-%m-%d-%H-%M-%S',time.localtime(time.time()))
    ParaLiteLog.init("%s/join-%s-%s-%s.log" % (
        proc.log_dir, gethostname(), proc.cqid, proc.opid),
                     logging.DEBUG)
    ParaLiteLog.info("--join node %s on %s is started" % (proc.opid, gethostname()))
    proc.start()

def test():
    join_in = JoinOp()

if __name__=="__main__":
    #test()
    main()

