import sys
import string
import cStringIO
import Queue
import threading
import base64
import random
import cPickle
import time
import os
import traceback
import shlex
import re
import logging
from socket import *
from subprocess import *

# set the home direcotry of paraLite into system's path
sys.path.append(os.path.abspath(os.path.join(sys.path[0], os.path.pardir)))

import dload_client
import conf
from lib.logger import ParaLiteLog
from lib import ioman, ioman_base

# the maximum data which can be stored in memory
DATA_MAX_SIZE = 1024*1024*1024*2  # 2GB

def es(s):
    sys.stderr.write("%s%s\n" % (conf.CHILD_ERROR, s))

def ws(s):
    sys.stdout.write("%s%s\n" % (conf.CHILD_OUTPUT, s))

def recv_bytes(so, n):
    A = []
    while n > 0:
        x = so.recv(n)
        if x == "": break
        A.append(x)
        n = n - len(x)
    return string.join(A, "")

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
        self.output_column = None

class UDXWrapper:
    def __init__(self, udx, pre_udx, sub_udx):
        self.udx = udx
        self.pre_udx = pre_udx
        self.sub_udx = sub_udx
        self.pid = 0
        self.process = None
        self.state = conf.READY
        self.incomingqueue = Queue.Queue()
        self.outgoingqueue = None
        self.is_data_done = False   # True if all data is gotten
        self.is_output_done = False # True if all data is calculated
        self.no_output_now = True
        self.output = None
        self.dest = None
        self.db_col_sep = None

    def listen_inqueue(self):
        try:
            flag = 0
            while True:
                cstrbuf = self.incomingqueue.get()
                ParaLiteLog.info("UDX--%s ---get data %s" % (self.udx.cmd_line.split()[0],
                                                    len(cstrbuf)))
                if cstrbuf == conf.FINISH:
                    ParaLiteLog.info("%s : get FINISH" % (self.udx.cmd_line.split()[0]))
                    self.finish = True
                    break
                elif cstrbuf == "EMPTY":
                    self.state = conf.IDLE
                    continue
                self.proc_data(cstrbuf)
            ParaLiteLog.info("%s : proc data finished" % (self.udx.cmd_line.split()[0]))
            self.outgoingqueue.put(conf.FINISH)
            if self.state == conf.READY or self.state == conf.IDLE or self.state == conf.WAIT:
                self.state = conf.FINISH
        except Exception, e:
            report_error(traceback.format_exc(), self.master_name, self.master_port)
            sys.exit(1)

    def proc_data(self, oldstr):
        ParaLiteLog.info("start to process data")
        self.state = conf.BUSY
        newstr = None
        if self.pre_udx is None:
            # udx_executor has already formatted the data
            newstr = oldstr
        else:
            newstr = oldstr
            # just replace the col and row delimiter
            pre_row_sep = self.pre_udx.output_row_delimiter
            pre_col_sep = self.pre_udx.output_col_delimiter
            pre_record_sep = self.pre_udx.output_record_delimiter
            cur_row_sep = self.udx.input_row_delimiter
            cur_col_sep = self.udx.input_col_delimiter
            cur_record_sep = self.udx.input_record_delimiter
            if pre_record_sep == conf.NULL:
                if pre_row_sep != cur_row_sep:
                    newstr = newstr.replace(pre_row_sep, cur_row_sep)
            else:
                if pre_record_sep != cur_row_sep:
                    newstr = newstr.replace(pre_record_sep, cur_row_sep)
            if pre_col_sep != cur_col_sep:
                newstr = newstr.replace(pre_col_sep, cur_col_sep)

        if self.udx.input == conf.STDIN:
            try:
                cmdline = string.strip(self.udx.cmd_line)
                pid = 0
                arg = shlex.split(cmdline)
                ParaLiteLog.info(cmdline)
                p = Popen(arg, bufsize = -1, stdin = PIPE, stdout = PIPE, stderr = PIPE)
                self.pid = p.pid
                ParaLiteLog.info("%s --- %s : start the command" % (self.udx.cmd_line.split()[0],
                                                           str(self.pid))) 
                lines = newstr
                out,err = p.communicate(lines)
                ParaLiteLog.info("%s --- finish the command " % (self.udx.cmd_line.split()[0]))
                self.output = out
                ParaLiteLog.debug(out)
                if err:
                    ParaLiteLog.info(err)
            except Exception, e:
                if e.errno == 2:
                    report_error("ERROR: %s \nPlease check you UDX is right" % " ".join(str(s) for s in e.args), self.master_name, self.master_port)
                ParaLiteLog.info(traceback.format_exc())
                self.state = conf.FAIL
        else:
            """
            Since multiple clients execute this command line at the same and these
            clients may be located on the same node, so it is necessary to distingush
            the input and output file name for each client.
            """
            new_input_name = "%s-%s-%s" % (self.udx.input, random.randint(1, 5000),
                                           random.randint(5000, 10000))
            if os.path.exists(new_input_name): os.remove(new_input_name)
            rf = open(new_input_name, "wb")
            rf.write(string.strip(newstr))
            rf.close()
            if self.udx.output == conf.STDOUT:
                new_cmd_line = self.udx.cmd_line.replace(self.udx.input, new_input_name)
                args = shlex.split(new_cmd_line)
                p = Popen(args, stdout=PIPE, stderr=PIPE)
                self.pid = p.pid
                out,err = p.communicate()
                self.output = out
                if err:
                    ParaLiteLog.info(err)
            else:
                new_output_name = "%s-%s-%s" % (self.udx.output,
                                                random.randint(1, 5000),
                                                random.randint(5000,10000))
                if os.path.exists(new_output_name): os.remove(new_output_name)
                new_cmd_line = self.udx.cmd_line.replace(
                    self.udx.input, new_input_name).replace(self.udx.output,
                                                            new_output_name)
                args = shlex.split(new_cmd_line)
                p = Popen(args, stdout = PIPE, stderr = PIPE)
                self.pid = p.pid
                out, err = p.communicate()
                if err:
                    ParaLiteLog.info(err)
                self.output = open(new_output_name, "rb").read()
                if os.path.exists(new_output_name): os.remove(new_output_name)
            if os.path.exists(new_input_name): os.remove(new_input_name)
        if self.is_data_done: self.state = conf.FINISH
        else: self.state = conf.IDLE
        if not self.output.endswith(self.udx.output_row_delimiter):
            self.output += self.udx.output_row_delimiter
        if self.output.strip() == "":
            ParaLiteLog.info("%s --- put data %s" % (self.udx.cmd_line.split()[0], 0))
            return
            
        self.outgoingqueue.put(self.output)
        ParaLiteLog.info("%s --- put data %s" % (self.udx.cmd_line.split()[0], len(self.output)))
        
    def put(self, data):
        self.incomingqueue.put(data)

    def get(self):
        result = outgoingqueue.get()
        return result

    def get_state(self):
        return self.state

    def listen_state(self):
        while True:
            if self.state == conf.FINISH:
                break
            if self.pid == 0:
                if self.is_data_done:
                    self.state = conf.FINISH
                    break
                time.sleep(1)
                continue
            """
            The process is Idel only when
            (1) there is not output data coming
            (2) cpu_usage is 0
            TODO: find a way to check if there is no output or not
            """
            if self.no_output_now:
                old_state = conf.BUSY
                cpu_usage = cpuer.get_cpu_local(self.pid)
                if cpu_usage == None:
#                    if old_state == conf.BUSY:
                    if self.is_data_done:
                        self.state = conf.FINISH
                elif cpu_usage == 0:
                    old_state = self.state
                    if old_state == conf.BUSY or old_state == conf.READY or old_state == conf.IDLE:
                    #if old_state == conf.BUSY:
                        if self.is_data_done:
                            self.state = conf.FINISH
                        else:
                            self.state = conf.IDLE
                else:
                    if self.state != conf.FINISH:
                        self.state = conf.BUSY
                time.sleep(1)

    def listen_cmd_output(self):
        """
        TODO: (1) object in outgoingqueue should be a StringIO instance
              (2) whether the output is stopped or not should be informed 
        while True:
            line = self.process.stdout.readline()
            self.outgoingqueue.put(line)
            if self.state == conf.FINISH:
                break
        for line in self.process.stdout:
            self.outgoingqueue.put(line)
        self.outgoingqueue.put(conf.END_TAG)
        """
        """
        The output of the cmd line may not be produced line by line because of
        buffer. So we first store them in an instance of StringIO, then put them
        into output_dic line by line.
        """
        c = cStringIO.StringIO()
        while True:
            line = self.process.stdout.readline()
            c.write(line)
            if self.state == conf.FINISH:
                break
        for line in self.process.stdout:
            c.write(line)
        self.output = c.getvalue()
        """
        l = c.getvalue().split("\n")
        self.output_dic[self.key_pos] = l
        # in list, \n is ""                                    
        if l[len(l)-1] == "\n" or l[len(l)-1] == "":
            l.pop()
        """

    def close(self):
        self.is_data_done = True
                
    def start(self):
        """
        Listen to the incomingQueue, get data, convert data 
        and then start the cmdline. When these data are processed successfully (a
        result file is produced), wrapper sends a kal message to the master and 
        waits new data. It ends when the flag is_data_done becomes true or "FINISH" 
        is received.
        """
        inqueue_listener = threading.Thread(target=self.listen_inqueue)
        inqueue_listener.setDaemon(True)
        inqueue_listener.start()

        """
        listen to the states of claculation
        It ends when self.state becomes Idle and is_data_done == True
        TODO: now we don't need to scan the state every second. 
        DATA processing: Once we receive data, we start a process (udx) to process it.
        After it is done, we close the process and wait for the next data.
        """

        #status_listener = threading.Thread(target=self.listen_state)
        #status_listener.setDaemon(True)
        #status_listener.start()

        inqueue_listener.join()
        ParaLiteLog.info("inqueue listener finished")
        #status_listener.join()
        #ParaLiteLog.info("status listener finished")
        """
        process the output file --> transform the format --> load to db or output to
        stdin (for testing, we store them into file)
        """
        """
        if self.udx.output != STDOUT:
            try:
                f = open(TEMP_RESULT_FILE, "rb")
                while True:
                    line = f.readline()
                    if not line:
                        break
                    self.output_dic[self.key_pos].append(line)
                out = self.format_final_result()

            except:
                traceback.print_exc()
        """
        if os.path.exists(self.udx.output):
            os.remove(self.udx.output)
        if os.path.exists(self.udx.input):
            os.remove(self.udx.input)

class UDXOp:
    def __init__(self):
        self.opid = None
        self.cqid = None
        self.id = None           # used to distinct each worker, here we use port
        self.udxes = []
        self.p_node = {}
        self.is_recv_data = False
        self.node = gethostname()
        self.my_port = 0
        self.limit = -1
        self.in_queue = Queue.Queue()
        self.out_queue = Queue.Queue()
        self.state = conf.READY
        self.iom = ioman.mk_ioman()
        self.is_running = True
        self.total_size = 0
        self.total_time = 0
        self.partition_num = 0
        self.result_type = conf.SINGLE_BUFFER
        self.is_checkpoint = False
        
        self.s_time = 0
        self.sub_s_time = 0  # the start time for each part of data
        self.pid = 0
        self.master_name = None
        self.master_port = 0
        self.is_local_socket_close = 0
        self.wrappers = []
        self.wrapper_thds = []
        self.is_last_op = False
        self.db_col_sep = None
        self.db_row_sep = None
        self.dest = None
        self.dest_db = None
        self.dest_table = None
        self.table = None
        self.client_sock = None
        self.data_size = 0  # the size of each part of data
        self.is_data_done = False
        self.output_list = []
        self.final_output = cStringIO.StringIO()  # store small data
        self.final_output_file = None   # store large data
        self.ready = True
        self.fashion = None
        self.hash_key = None
        self.hash_key_pos = 0
        self.input = []
        self.output = []
        self.key = None
        self.key_pos = []
        self.threads = {}  # {thread_name : thread}
        self.p_node = {}
        self.split_key = None
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
        for key in self.key:
            self.key_pos.append(self.input.index(key))

        if self.is_checkpoint == conf.CHECKPOINT:
            # this is a recovery operator
            # init the persisted result data
            for i in self.partition_num:
                f_name = self.get_file_name_by_part_id(i)
                self.result[i] = [f_name]

    def reg_to_master(self):
        try:
            addr = (self.master_name, self.master_port)
            sock = socket(AF_INET, SOCK_STREAM)
            sock.settimeout(300)
            sock.connect(addr)
            sep = conf.SEP_IN_MSG
            msg = sep.join([conf.REG, conf.DATA_NODE, self.cqid, self.opid,
                            self.node, str(self.my_port), self.local_addr, self.id])
            sock.send('%10s%s' % (len(msg), msg))
            sock.close()
        except Exception, e:
            ParaLiteLog.debug("ERROR in register_to_master: %s" % traceback.format_exc())

    def listen_local(self):
        workers = []
        sock = socket(AF_UNIX, SOCK_STREAM)
        try:
            if os.path.exists(self.local_addr):
                os.unlink(self.local_addr)
            ParaLiteLog.info("local socket is %s" % (self.local_addr))
            sock.bind(self.local_addr)
            sock.listen(5)
            while True:
                client, conn = sock.accept()
                head_len = string.atoi(recv_bytes(client, 10))
                head = recv_bytes(client, head_len)
                if head == 'END':
                    self.is_local_socket_close = 1
                    break
                worker = threading.Thread(target=self.recv_data, args = (client,))
                worker.setDaemon(True)
                worker.start()
                workers.append(worker)
            sock.close()
            for worker in workers:
                worker.join()
        except Exception, e:
            ParaLiteLog.info(traceback.format_exc())
            report_error(traceback.format_exc(), self.master_name, self.master_port)
            sys.exit(1)
            
    def listen_global(self):
        ParaLiteLog.info("self.my_port = %s" % self.my_port)
        workers = []
        addr = ('', self.my_port)
        sock = socket(AF_INET, SOCK_STREAM)
        try:
            sock.bind(addr)
            sock.listen(5)
            n, self.my_port = sock.getsockname()
            while True:
                client, conn = sock.accept()
                head_len = string.atoi(recv_bytes(client, 10))
                head = recv_bytes(client, head_len)
                if head == "END":
                    self.is_data_done = True # now this is useless
                    """
                    while True:
                        if self.ready: # wait for the data extraction ending
                            break
                        time.sleep(0.2)
                    """
                    self.in_queue.put(conf.FINISH)
                    break
                worker = threading.Thread(target=self.recv_data, args = (client,))
                worker.setDaemon(True)
                worker.start()
                workers.append(worker)
            sock.close()
            for worker in workers:
                worker.join()
        except Exception, e:
            ParaLiteLog.info(traceback.format_exc())
            report_error(traceback.format_exc(), self.master_name, self.master_port)
            sys.exit(1)

    def recv_data(self, client):
        try:
            while True:
                length_str = recv_bytes(client, 10)
                if length_str == "":
                    break
                length = string.atoi(length_str)
                self.data_size = length
                self.t_size += length
                message = recv_bytes(client, length)
                # for EMPTY message:
                if message == "EMPTY":
                    self.in_queue.put(message)
                    continue
                #self.ready = False
                data = self.extract_data_for_udx(message)
                #self.ready = True
                # after this block of data is processed, we send a reply back and wait for the next data
                self.in_queue.put(data)                
                client.send("%10s%s" % (len(conf.END_TAG), conf.END_TAG))
                self.sub_s_time = time.time()*1000
            client.close()
        except Exception, e:
            ParaLiteLog.info(traceback.format_exc())
            report_error(traceback.format_exc(), self.master_name, self.master_port)
            sys.exit(1)

    def extract_data_for_udx(self, data):
        """
        f = open("/home/ting/.paralite-log/1", "wb")
        f.write(data)
        f.close()
        """
        """
        The coming data may contains many columns, but only one column is used by udx
        Example: input = [docid, abs] F(abs) input_row_delimiter ":::::" input_record_delimiter NULL
                 DB_DEFAULT_ROW_SEP = "==="
        oldstr = 1|aaa===
                 2|bbb===
                 3|ccc===
        newstr = aaa:::::bbb:::::ccc
        Meanwhile, store others in output_dic and put each output_dic in output_list,
        once get a result data, pop the first dic from the list
        """
        count = 0
        output_dic = {}
        for i in range(len(self.output)):
            output_dic[i] = []
        cbuf = cStringIO.StringIO()
        in_row_sep = self.udxes[len(self.udxes) - 1].input_row_delimiter
        in_col_sep = self.udxes[len(self.udxes) - 1].input_col_delimiter
        # first delete the last sep
        if self.db_row_sep != "\n":
            data = data[0:data.rfind(self.db_row_sep)]
        if len(self.input) == 1:
            # there is only one column related
            if in_row_sep != self.db_row_sep:
                for dd in data.split(self.db_row_sep):
                    cbuf.write(string.strip(dd))
                    cbuf.write(in_row_sep)
            else:
                cbuf.write(data)
        else:
            if len(self.key_pos) == len(self.input):
                # all input are the arguments of UDX
                new_data = data
                t_sep = self.db_row_sep
                if t_sep != in_row_sep:
                    if t_sep != "\n":
                        """
                        in order to distribute data, we add a \n at the end of each record
                        if the db_row_sep is not \n
                        """
                        t_sep = "%s\n" % t_sep
                    new_data = data.replace(t_sep, in_row_sep)
                if len(self.key_pos) > 1 and self.db_col_sep != in_col_sep:
                    new_data = new_data.replace(self.db_col_sep, in_col_sep)
                cbuf.write(new_data)
            else:
                s = data.split(self.db_row_sep)
                l = len(s)
                l_key = len(self.key_pos)
                ParaLiteLog.info("RECORD_NUM = %s" % l)
                k = 0
                while k < l:
                    row = s[k]
                    if row == "" or row == '\n':
                        k += 1
                        continue
                    r = row.split(self.db_col_sep)
                    for i in range(len(r)):
                        if i in self.key_pos:
                            cbuf.write(string.strip(r[i]))
                            if i < self.key_pos[l_key-1]:
                                cbuf.write(in_col_sep)
                        else:
                            if i > self.key_pos[l_key - 1]:
                                output_dic[i-l_key+1].append(string.strip(r[i]))
                            else:
                                output_dic[i].append(string.strip(r[i]))
                    if k < l - 1:                            
                        cbuf.write(in_row_sep)
                    k += 1
        self.output_list.append(output_dic)
        """
        f = open("/home/ting/.paralite-log/2", "wb")
        f.write(cbuf.getvalue())
        f.close()
        """
        return cbuf.getvalue()
        

    def send_kal(self, state):
        addr = (self.master_name, self.master_port)
        sock = socket(AF_INET, SOCK_STREAM)
        try:
            sock.connect(addr)
        except Exception, e:
            if e.errno == 110:
                ParaLiteLog.info("send kal failed...")
                return False
        # msg = KAL:cqid:opid:id:status:speed:time
        cur_time = time.time()*1000
        use_time = cur_time - self.sub_s_time
        speed = float(self.data_size) / float(use_time) / float(1000)
        sep = conf.SEP_IN_MSG
        msg = sep.join([conf.KAL, self.cqid, self.opid, self.id,
                        state, str(speed), str(use_time)])
        sock.send('%10s%s' % (len(msg), msg))
        sock.close()
        return True

    def listen_output(self):
        try:
            while True:
                q = self.out_queue
                data = q.get()
                if data == conf.FINISH:
                    break
                output_dic = self.output_list.pop(0)
                output_dic[self.key_pos[0]] = data
                out = self.format_output(output_dic)
                self.total_size += len(out)
                """
                f = open("/tmp/paralite/3", "wb")
                f.write(out)
                f.close()
                """
                if len(self.final_output.getvalue()) + len(out) <= DATA_MAX_SIZE:
                    self.final_output.write(out)
                else:
                    if self.final_output_file is None:
                        self.final_output_file = "%s%sudx_%s_%s" % (TEMP_DIR, os.sep,
                                                                    self.cqid, self.opid)
                    fd = open(self.final_output_file, "wa")
                    fd.write(self.final_output.getvalue())
                    self.final_output.truncate(0)
                    fd.write(out)
                    fd.close()
        except Exception, e:
            ParaLiteLog.info(traceback.format_exc())
            report_error(" ".join((str(s) for s in e.args)), self.master_name, self.master_port)
            sys.exit(1)
            
    """
    --------------------------------------------------------------------
            select rowid, F(a) as (aa, bb) from x with F=... 
            output_row_delimiter '=====' output_col_delimiter ':::::'
    --------------------------------------------------------------------
    input: output_dic {0: [rowid1, rowid2...], 1:[all output of F]}
    the result format should be:    
    rowid1:::::aa1:::::bb1=====
    rowid2:::::aa2:::::bb2
    --------------------------------------------------------------------
   select rowid, F(a) as (aa, bb) from x with F=... output_row_delimiter NEW_LINE 
   output_col_delimiter ':::::' output_record_delimiter '====='
    --------------------------------------------------------------------
    input: output_dic {0: [rowid1, rowid2...], 1:[all output of F]}
    the result format should be:
    rowid1:::::aa1:::::bb1
    rowid1:::::aa2:::::bb2=====
    rowid2:::::aa3:::::bb3
    rowid2:::::aa4:::::bb4=====
    -----------------------------------------------------------------------
    select rowid, F(a, b) as (aa, bb) from x with F = ...
    input_col_delimiter ':::::' output_col_delimiter = ':::::' 
    -----------------------------------------------------------------------
    input: output_dic {0: [rowid1, rowid2...], 1:[all output of F]}
    the result format should be:
    rowid1:::::aa1:::::bb1
    rowid2:::::aa2:::::bb2

    NOTE: the order in self.udxes is the opposite of the real processing order.
    A(F(G(a))) :
    self.udxes = ["A", "F", "G"], but the processing chain is G->F->A
    """
    def format_output(self, output_dic):
        out = cStringIO.StringIO()
        pos_n = 0  # control of the point of non-key columns
        pos_k = 0  # control of the key column
        if len(output_dic) == 1:
            # the output is from the udx, return the output directly
            #out.write(output_dic[0])
            return output_dic[0]
        """
        find the pos of non-key attribute 
        """
        if self.key_pos[0] > 0:
            s_pos = 0
        else:
            s_pos = len(output_dic) - 1
        if self.udxes[0].output_col_delimiter != conf.NULL:
            col_sep = self.udxes[0].output_col_delimiter
        else:
            col_sep = self.db_col_sep
        record_sep = self.udxes[0].output_record_delimiter
        row_sep = self.udxes[0].output_row_delimiter
        udx_out_list = []
        if record_sep != "":
            udx_out_list_temp = output_dic[self.key_pos[0]].split(record_sep)
            for record in udx_out_list_temp:
                if record == "":
                    continue
                record_list = string.strip(record).split(row_sep)
                # delete the last element a = "aa==bb==" --> ["aa", "bb", ""]
                if record_list[len(record_list) - 1] == "":
                    record_list.pop(len(record_list) -1)
                udx_out_list.append(record_list)
            pos_k = 0
            pos_u = 0
            while pos_u < len(output_dic[s_pos]):
                re = udx_out_list[pos_u]
                pos_k = 0
                while pos_k < len(re):
                    record = ""
                    for i in range(len(output_dic)):
                        if i not in self.key_pos:
                            if i != len(output_dic) -1:
                                record += output_dic[i][pos_u] + col_sep
                            else:
                                record += output_dic[i][pos_u]
                        else:
                            if i != len(output_dic) -1:
                                record += re[pos_k] + col_sep
                            else:
                                record += re[pos_k]
                            pos_k += 1
                    out.write(record)
                    out.write(row_sep)
                pos_u += 1
        else:
            udx_out_list = output_dic[self.key_pos[0]].split(row_sep)
            if udx_out_list[len(udx_out_list) - 1] == "" and len(udx_out_list) > len(output_dic[s_pos]):
                udx_out_list.pop(len(udx_out_list) -1)
            ParaLiteLog.info("LENGTH of non-UDX: %s" % (len(output_dic[s_pos])))
            ParaLiteLog.info("LENGTH of UDX: %s" % (len(udx_out_list)))                
            pos_u = 0
            while pos_u < len(output_dic[s_pos]):
                re = udx_out_list[pos_u]
                record = ""
                for i in range(len(output_dic)):
                    if i not in self.key_pos:
                        if i!= len(output_dic) -1:
                            record += output_dic[i][pos_u] + col_sep
                        else:
                            record += output_dic[i][pos_u]
                    else:
                        if i!= len(output_dic) -1:
                            record += re + col_sep
                        else:
                            record += re
                out.write(record)
                out.write(row_sep)
                pos_u += 1
        if row_sep == conf.NEW_LINE or row_sep == conf.EMPTY_LINE:
            return "%s%s" % (string.strip(out.getvalue()), row_sep)
        else:
            return string.strip(out.getvalue())

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

    """
    final_result: a cStringIO.StringIO instance
    the output should go to:
    (1) another operator if UDX is the intermediate step
    (2) database if UDX is the final step and "output_column" is specified
    (3) files if UDX is the final step and "output_column" is not specified
    """
    def distribute_data(self):
        row_sep = self.udxes[0].output_row_delimiter
        if self.limit != -1:
            out = cStringIO.StringIO()
            out.write(row_sep.join(
                self.final_output.getvalue().split(row_sep)[:self.limit]))
        else:
            out = self.final_output
        del(self.final_output)
        
        if self.dest == conf.DATA_TO_DB:
            if self.final_output_file is not None:
                if len(out.getvalue()) > 0:
                    fd = open(self.final_output_file, "wa")
                    fd.write(out.getvalue())
                    fd.close()
                ParaLiteLog.info(
                    "Final result has size : %s and is stored in file" % os.path.getsize(
                        self.final_output_file))
                data = [self.final_output_file]
            else:
                ParaLiteLog.info(
                    "Final result has size : %s and is stored in buffer" % len(out.getvalue()))
                data = out

            self.data = data

            if self.udxes[0].output_col_delimiter != conf.NULL:
                col_sep = self.udxes[0].output_col_delimiter
            else:
                col_sep = self.db_col_sep

            ParaLiteLog.info("LOAD data: START")
            master = (self.master_name, self.master_port)
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
            
            if self.final_output_file is not None:
                if os.path.exists(self.final_output_file):
                    os.remove(self.final_output_file)
        elif self.dest == conf.DATA_TO_ONE_CLIENT:
            # send data to the client
            random_num = random.randint(0, len(self.client_sock) - 1)
            addr = self.client_sock[random_num]
            sock = socket(AF_INET, SOCK_STREAM)
            sock.connect(addr)
            if self.final_output_file is None:
                data = string.strip(out.getvalue())
                sock.send("%10s%s" % (len(data), data))
            else:
                fd = open(self.final_output_file, "rb")
                while True:
                    data = fd.read(DATA_MAX_SIZE)
                    if not data:
                        break
                    sock.send("%10s%s" % (len(data),data))
                fd.close()
            sock.close()
        elif self.dest == conf.DATA_TO_ANO_OP:
            # pipeline data to another op
            if self.final_output_file is None:
                data = string.strip(out.getvalue())
                ParaLiteLog.info("Final result has size : %s and is stored in buffer" % len(data))
                del(out)
            else:
                fd = open(self.final_output_file, "rb")
                data = fd.read()
                fd.close()
                ParaLiteLog.info("Final result has size : %s and is stored in file" % os.path.getsize(self.final_output_file))
                
            num_of_dest = len(self.p_node)
            if num_of_dest == 1:
                port = self.p_node.keys()[0]
                node = self.p_node[port]
                ParaLiteLog.info("connect to %s:%s" % (node, port))
                queue = cqueue(node, port)
                queue.connect()
                newid = self.id.split("_")[0]
                queue.put(newid)
                queue.put(data)
                queue.close()
            elif num_of_dest > 1:
                """
                send result to the next node:
                (1) store records of rs in a cStringIo instance whose partition 
                numbers are the same
                (2) send each part of data: id+data
                """
                queue_list = []
                columns = self.output
                if self.split_key != None:
                    # partition data in hash fashion
                    split_key = self.split_key
                    if columns[0].find('.') == -1:
                        pos = columns.index(split_key.split('.')[1])
                    else:
                        pos = columns.index(split_key)
                    data_part_dic = {}
                    for i in range(num_of_dest):
                        data_part_dic[i] = cStringIO.StringIO()
                    for row in data.split("\n"):
                        cols = row.split("|")
                        partition_num = abs(hash(cols[pos])) % num_of_dest
                        data_part_dic[partition_num].write(row)
                        data_part_dic[partition_num].write("\n")
                    del(data)
                    ParaLiteLog.info("start to pipeline data")
                    i = 0
                    for key in self.p_node.keys():
                        port = key
                        host = self.p_node[key]
                        ParaLiteLog.info("connect to %s : %s" % (host, port))
                        queue = cqueue(host, port)
                        queue.connect()
                        # we put the id of operator to identify each part of data
                        # but the id should not be the self.id (1_0), it should be 1
                        newid = str(self.id)
                        queue.put(newid)
                        queue.put(data_part_dic[i].getvalue())
                        data_part_dic[i].truncate(0)
                        queue.close()
                        i += 1

    def start_udx(self, wrapper):
        try:
            wrapper.start()
        except:
            traceback.print_exc()
            raise Exception("ddd", "aaa")
         
    def listen_status(self):
        try:
            first_wrap = self.wrappers[0]
            last_wrap = self.wrappers[len(self.wrappers)-1]
            while True:
                if last_wrap.get_state() == conf.FINISH:
                    self.state = conf.FINISH
                    break
                if first_wrap.get_state() == conf.IDLE:
                    if self.send_kal(conf.IDLE):
                        ParaLiteLog.debug("send KAL message")
                        if self.is_data_done:
                            first_wrap.state = conf.FINISH
                        else:
                            first_wrap.state = conf.WAIT
                for wrap in self.wrappers:
                    if wrap.get_state() == conf.FAIL:
                        self.state = conf.FAIL
                        break
                time.sleep(0.1)
        except Exception, e:
            ParaLiteLog.info(traceback.format_exc())
            report_error(" ".join(str(s) for s in e.args), self.master_name, self.master_port)
            sys.exit(1)
            
    def proc(self):
        try:
            ParaLiteLog.info("--------udx node %s on %s is started--------" % (
                self.opid, gethostname()))
            self.local_addr = "/tmp/paralite-local-addr-udx-%s-%s-%s-%s" % (
                gethostname(), self.cqid, self.opid, self.id)
            if os.path.exists(self.local_addr):
                os.remove(self.local_addr)
            """
            start a socket server, global_listener ends when it receives a "END" info 
            from the master and local_listener ends when it receives a "END" info from
            this process 
            """
            # start local socket server to listen all connections
            ch = self.iom.create_server_socket(AF_INET,
                                               SOCK_STREAM, 100, ("", self.my_port)) 
            n, self.my_port = ch.ss.getsockname()
            ParaLiteLog.debug("listen on port : %s ..." % str(self.my_port))

            # start socket server for local connections
            self.iom.create_server_socket(AF_UNIX,
                                          SOCK_STREAM, 10, self.local_addr) 
            
            
            # register this worker to master
            self.reg_to_master()
            ParaLiteLog.info("register_to_master : FINISH")

            while self.is_running:
                ev = self.next_event(None)
                if isinstance(ev, ioman_base.event_accept):
                    self.handle_accept(ev)
                if isinstance(ev, ioman_base.event_read):
                    if ev.data != "":
                        self.handle_read(ev)

            ParaLiteLog.info("--udx node %s on %s is finished--" % (self.opid,
                                                                    gethostname()))

        except KeyboardInterrupt, e:
            report_error("ParaLite receives a interrupt signal and then will close the process\n", self.master_name, self.master_port)
            ParaLiteLog.info("--sql node %s on %s is finished--" % (self.opid,
                                                                    gethostname()))
            sys.exit(1)
        except Exception, e1:
            ParaLiteLog.debug(traceback.format_exc())
            report_error(traceback.format_exc(), self.master_name, self.master_port)
            sys.exit(1)


    def handle_read(self, event):
        message = event.data[10:]
        msg_sep = conf.SEP_IN_MSG
        m = message.split(msg_sep)
        try:        
            if m[0] == conf.JOB_ARGUMENT:
                self.parse_args(m[1])
                self.start_all_thread()
                ParaLiteLog.info("parse arguments: FINISH")
                
            elif m[0] == conf.DATA:
                if self.total_time == 0:
                    self.total_time = time.time()
                data = m[1]
                self.data_size = len(data)
                self.total_size += self.data_size
                format_data = self.extract_data_for_udx(data)
                self.in_queue.put(format_data)
                self.sub_s_time = time.time()*1000
                
            elif m[0] == conf.JOB_END:
                ParaLiteLog.debug("MESSAGE: %s" % message)                
                self.total_time = time.time() - self.total_time
                self.is_data_done = True
                self.in_queue.put(conf.FINISH)
                self.join_all_thread()
                self.send_rs_info_to_master(self.total_size, self.total_time)
                # distribute data
                if self.dest == conf.DATA_TO_ONE_CLIENT:
                    self.distribute_data()
                    self.send_status_to_master(self.id, conf.ACK)
                    self.is_running = False
                elif self.dest == conf.DATA_TO_DB:
                    self.distribute_data()

            elif m[0] == conf.DATA_PERSIST:
                ParaLiteLog.debug("MESSAGE: %s" % message)                
                # if the data is requried to be persisted or not
                if m[1] == conf.CHECKPOINT:
                    self.write_data_to_disk()

            elif m[0] == conf.DATA_DISTRIBUTE:
                ParaLiteLog.debug("MESSAGE: %s" % message)
                # send a part of data to the next operator
                # DATA_DISTRIBUTE:partition_num:destnode
                part_id, destnode = m[1:]
                data = self.get_data_by_part_id(self.result, string.atoi(part_id))
                
                # DATA message includes: type:id+data
                # the first 2 chars represents the opid
                msg = sep.join([conf.DATA, "%2s%s" % (self.opid, data)])
                self.send_data_to_node(msg, destnode, self.p_node)
                ParaLiteLog.debug(
                    "send data susscufully   %s %s --> %s" % (
                        self.opid, gethostname(), destnode))

            elif m[0] == conf.DLOAD_REPLY:
                reply = msg_sep.join(m[1:])
                ParaLiteLog.info("receive the information from the master")
                ParaLiteLog.debug(reply)

                if len(self.data.getvalue()) != 0:
                    dload_client.dload_client().load_internal_buffer(
                        reply, self.dest_table, self.data, self.fashion, 
                        self.hash_key, self.hash_key_pos, self.db_col_sep, 
                        self.db_row_sep, self.db_col_sep, False, "0", self.log_dir)

                # send END_TAG to the master
                client_id = "0"
                msg = msg_sep.join([conf.REQ, conf.END_TAG, gethostname(), client_id])
                so_master = socket(AF_INET, SOCK_STREAM)
                so_master.connect((self.master_name, self.master_port))
                so_master.send("%10s%s" % (len(msg), msg))
                so_master.close()
                ParaLiteLog.debug("sending to master: %s" % (conf.END_TAG))
                ParaLiteLog.debug("----- dload client finish -------")

            elif message == conf.DLOAD_END_TAG:
                ParaLiteLog.debug("---------import finish---------")
                self.send_status_to_master(self.id, conf.ACK)
                self.is_running = False

            elif m[0] == conf.DATA_END:
                ParaLiteLog.debug("MESSAGE: %s" % message)
                # all data is dipatched to the parent nodes
                self.send_status_to_master(self.id, conf.ACK)
                self.is_running = False
                
            elif message == conf.END_TAG:
                ParaLiteLog.debug(message)
                self.is_running = False
        except Exception, e:
            es(traceback.format_exc())
            ParaLiteLog.info(traceback.format_exc())
            self.is_running = False
            self.no_error = False

    def start_all_thread(self):
        try:
            """
            start all udxes
            NOTE: the order of wrappers is the opposite of the order of udx
            """
            i = len(self.udxes) - 1
            pre_udx = None
            while i >= 0:
                sub_udx = None
                cur_udx = self.udxes[i]
                if i > 0:
                    sub_udx = self.udxes[i - 1]
                wrapper = UDXWrapper(cur_udx, pre_udx, sub_udx)
                wrapper.db_col_sep = self.db_col_sep
                pre_udx = cur_udx
                self.wrappers.append(wrapper)
                i -= 1
                
            for i in range(len(self.wrappers)):
                wrap = self.wrappers[i]
                if i == 0: wrap.incomingqueue = self.in_queue
                if i == len(self.wrappers) - 1: wrap.outgoingqueue = self.out_queue
                else: wrap.outgoingqueue = self.wrappers[i + 1].incomingqueue
                udx_starter = threading.Thread(target=self.start_udx, args=(wrap,))
                udx_starter.setDaemon(True)
                udx_starter.start()
                self.wrapper_thds.append(udx_starter)
            start = time.time()
            
            """
            listen to the status of all udxes, if all of them are idle (or only the
            first one is idle), notify to the master
            """
            status_listener = threading.Thread(target=self.listen_status)
            status_listener.setDaemon(True)
            status_listener.start()
            self.threads["status_listener"] = status_listener
            """
            listen to the output data from the last udx
            """
            output_listener = threading.Thread(target=self.listen_output)
            output_listener.setDaemon(True)
            output_listener.start()
            self.threads["output_listener"] = output_listener
        except Exception, e:
            ParaLiteLog.debug(traceback.format_exc())
            raise(Exception(e))

    def join_all_thread(self):
        try:
            # join all udx wrapper threads
            for wrapper in self.wrappers:
                wrapper.close()
            for thd in self.wrapper_thds:
                thd.join()

            # join others
            self.out_queue.put(conf.FINISH)            
            for thd_name in self.threads:
                self.threads[thd_name].join()
                ParaLiteLog.debug("THREAD: %s finished" % thd_name)
            
        except Exception, e:
            ParaLiteLog.debug(traceback.format_exc())
            raise(Exception(e))

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

    def handle_accept(self, event):
        event.new_ch.flag = conf.SOCKET_OUT
        event.new_ch.buf = cStringIO.StringIO()
        event.new_ch.length = 0
        
    def next_event(self, t):
        while True:
            ParaLiteLog.debug("waiting.....")
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
                ParaLiteLog.debug(data_to_return)
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
                    ParaLiteLog.debug(data_to_return)
                    return ioman_base.event_read(ch, data_to_return, 0, ev.err)

    def kill_global_listener(self):
        sock = socket(AF_INET, SOCK_STREAM)
        sock.connect(('', self.my_port))
        msg = "END"
        sock.send("%10s%s" % (len(msg), msg))
        sock.close()

    def kill_local_listener(self):
        sock = socket(AF_UNIX, SOCK_STREAM)
        sock.connect(self.local_addr)
        msg = "END"
        sock.send("%10s%s" % (len(msg), msg))
        sock.close()

    def kill_status_listener(self):
        self.wrappers[0].state = conf.FAIL

    def kill_output_listener(self):
        self.out_queue.put(conf.FINISH)

    def kill_udx_wrapper(self):
        for wrap in self.wrappers:
            wrap.incomingqueue.put(conf.FINISH)
        
    def safe_exit(self):
        if "global_listener" in self.threads:
            self.kill_global_listener()
        if "local_listener" in self.threads:
            self.kill_local_listener()
        if "status_listener" in self.threads:
            self.kill_status_listener()
        if "output_listener" in self.threads:
            self.kill_output_listener()            
        if "udx_wrapper" in self.threads:
            self.kill_udx_wrapper()

            
def test():
    UDXWrapper().start()

def report_error(s, master_name, master_port):
    addr = (master_name, master_port)
    sock = socket(AF_INET, SOCK_STREAM)
    sock.connect(addr)
    msg = "ERROR: %s" % s
    sock.send("%10s%s" % (len(msg), msg))
    sock.close()

def main():
    if len(sys.argv) != 8:
        sys.exit(1)
        
    proc = UDXOp()
    proc.master_name = sys.argv[1]
    proc.master_port = string.atoi(sys.argv[2])
    proc.cqid = sys.argv[3]
    proc.opid = sys.argv[4]
    proc.my_port = string.atoi(sys.argv[5])
    proc.log_dir = sys.argv[6]
    proc.id = sys.argv[7]
    
    if not os.path.exists(proc.log_dir): os.makedirs(proc.log_dir)
    cur_time = time.strftime('%Y-%m-%d-%H-%M-%S',time.localtime(time.time()))
    ParaLiteLog.init("%s/udx-%s-%s-%s.log" % (proc.log_dir, gethostname(),
                                              cur_time, proc.id),
                     logging.DEBUG)
    ParaLiteLog.info("--udx node %s:%s on %s is started" % (proc.opid, proc.id, gethostname()))
    proc.proc()

    
if __name__ == "__main__":
    main()


