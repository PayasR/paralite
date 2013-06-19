import os
import sys
import time
import cPickle
import base64
import string
import random
import re
import cStringIO
import traceback
import logging
import shlex
import subprocess 
from socket import *

# set the home direcotry of paraLite into system's path
sys.path.append(os.path.abspath(os.path.join(sys.path[0], os.path.pardir)))

import dload_client
import conf
from lib.logger import ParaLiteLog
from lib import ioman, ioman_base

def es(s):
    sys.stderr.write("%s%s\n" % (conf.CHILD_ERROR, s))

def ws(s):
    sys.stdout.write("%s%s\n" % (conf.CHILD_OUTPUT, s))

def get_max_size():
    """get the free memory size
    """
    p = subprocess.Popen(shlex.split("free -m"), stdout=subprocess.PIPE)
    stdout_list = p.communicate()[0].split('\n')
    s = string.atoi(stdout_list[1].split()[3].strip())*1024*1024
    return s
    
class OrderbyOp:
    
    MAX_SIZE = get_max_size()
    
    def __init__(self):
        self.order_key = []
        self.order_type = []
        self.dest = None
        self.attrs = {}
        self.output = []
        self.input = []
        self.split_key = None
        self.name = None
        self.cqid = None
        self.opid = None
        self.procid = None
        self.status = None
        self.cost_time = 0
        self.node = (None, 0)  # (node, port)
        self.p_node = {}       # {port, node}
        self.my_port = 0
        self.local_addr = None
        self.num_of_children = {} #{opid:num}
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
        self.partition_num = 0 
        self.log_dir = None
        self.temp_dir = None
        self.is_checkpoint = None
        self.limit = -1
        self.distinct = False

        self.key_pos_in_input = []
        self.iom = ioman.mk_ioman()
        self.is_running = True
        self.result = {}
        self.result_type = 0
        self.source_data = []
        self.total_size = 0
        self.total_time = 0
        self.cur_jobid = None
        self.failed_node = []
        self.replica_result = {} # the result data for a failed node

    def parse_args(self, msg):
        b1 = base64.decodestring(msg)
        dic = cPickle.loads(b1)
        for key in dic.keys():
            value = dic[key]
            if hasattr(self, key):
                setattr(self, key, value)
        if not os.path.exists(self.temp_dir): os.makedirs(self.temp_dir)

        for key in self.order_key:
            self.key_pos_in_input.append(self.input.index(key))
            
        if self.is_checkpoint is not None and self.is_checkpoint == conf.CHECKPOINT:
            # this is a recovery operator
            # init the persisted result data
            for i in self.partition_num:
                f_name = self.get_file_name_by_part_id(i)
                self.result[i] = [f_name]

    def shell_sort(self, data_list, csio):
        s1 = time.time()*1000
        key_pos = self.key_pos_in_input[0]
        if self.attrs.has_key(self.order_key[0]) == False:
            key_type = conf.STRING
        else:
            key_type = self.attrs[self.order_key[0]]
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
        p1 = subprocess.Popen(args, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
        for eachdata in data_list:
            p1.stdin.write(eachdata)
        p1.stdin.close()
        for line in p1.stdout:
            csio.write(line.strip())
            csio.write(self.db_row_sep)
        e1 = time.time()*1000

    def quick_sort(self, data_list, csio):
        c = cStringIO.StringIO()
        i = 0
        key_pos = []
        for key in self.order_key:
            pos = self.input.index(key)
            key_pos.append(pos)
        if self.attrs.has_key(self.key) == False:
            key_type = conf.STRING
        else:
            key_type = self.attrs[self.key]
        if key_type != conf.INT or key_type != conf.FLOAT:
            para = "-s"
        else:
            para = "-n"            
        for data in data_list:
            c.write(data)
        data = c.getvalue().split(self.db_row_sep)
        # delete the last null element
        if data[len(data) - 1] == '':
            data.pop()
        self.i_quick_sort(data, 0, len(data) - 1, key_pos, para)
        csio.write(self.db_row_sep.join(data))
    
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
    
    def distinct_data(self, data_list):
        try:
            csio = cStringIO.StringIO()
            whole_data = ""
            for data in data_list:
                if data.strip() == "":
                    continue
                whole_data += data.strip() + self.db_row_sep
                del(data)
            if whole_data == "":
                return None, None, None
            whole_data = set(whole_data.strip().split(self.db_row_sep))
            csio.write(self.db_row_sep.join(whole_data))    
            return conf.SINGLE_BUFFER, [csio], len(csio.getvalue())
        except Exception, e:
            ParaLiteLog.debug(traceback.format_exc())
            self.report_error("ERROR in order_by.py : %s" % traceback.format_exc())
            return None, None, None

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
                
        if self.limit != -1:
            data_list = whole_data.getvalue().split(self.db_row_sep)[:self.limit]
            del whole_data
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
            
            # start socket server for local connections
            self.local_addr = "/tmp/paralite-local-addr-distinct-%s-%s-%s" % (
                gethostname(), self.cqid, self.opid)
            if os.path.exists(self.local_addr): os.remove(self.local_addr)
            self.iom.create_server_socket(AF_UNIX,
                                          SOCK_STREAM, 10, self.local_addr) 
            
            # register local port to the master
            self.register_to_master(
                self.cqid, self.opid, self.procid, gethostname(), self.my_port)
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
                ParaLiteLog.debug("MESSAGE: %s" % message)                
                self.cur_jobid = m[1]
                
            elif m[0] == conf.DATA:
                data_id = string.strip(m[1][0:2])
                data = m[1][2:]
                self.source_data.append(data)
                
                # sort data
                if not self.is_data_ready(self.source_data, self.num_of_children):
                    return

                ParaLiteLog.debug("****distinct DATA****: start" )
                s = 0
                for data in self.source_data:
                    s += len(data)
                ParaLiteLog.debug("source data size: %s" % s)                
                s_time = time.time()
                rs_type, rs, t_size = self.distinct_data(self.source_data)
                del self.source_data
                ParaLiteLog.debug("****distinct DATA****: finish" )
                
                if rs_type is None:
                    self.send_status_to_master(self.cur_jobid, conf.PENDING)
                    return
                    
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
                    if self.total_size > self.MAX_SIZE:
                        self.write_data_to_disk()
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
                    ParaLiteLog.debug("dest = %s" % self.dest)                    
                    self.distribute_data()
                    self.send_status_to_master(self.cur_jobid, conf.ACK)
                    self.is_running = False
                elif self.dest == conf.DATA_TO_DB:
                    self.distribute_data()
                    
            elif m[0] == conf.DATA_PERSIST:
                # if the data is requried to be persisted or not
                if m[1] == conf.CHECKPOINT:
                    self.write_data_to_disk()

            elif m[0] == conf.DLOAD_REPLY:
                sep = conf.SEP_IN_MSG
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
                self.send_status_to_master(" ".join(self.cur_jobid), conf.ACK)
                self.is_running = False

            elif m[0] == conf.EXIT:
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
        if len(source_data) != num_of_s[num_of_s.keys()[0]] - len(self.failed_node):
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
        

    def register_to_master(self, cqid, opid, procid, node, port):
        sep = conf.SEP_IN_MSG
        msg = sep.join([conf.REG, conf.DATA_NODE, cqid, opid, procid, gethostname(),
                        str(self.my_port), self.local_addr])
        ParaLiteLog.debug(
            "MASTER_NODE: %s  MASTER_PORT: %s" % (self.master_name, self.master_port))
        addr = (self.master_name, self.master_port)
        sock = socket(AF_INET, SOCK_STREAM)
        try:
            sock.connect(addr)
        except Exception, e:
            ParaLiteLog.error(
                "Error in register_to_master: %s" % traceback.format_exc())
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
        m = [status, conf.JOB, self.cqid, self.opid, self.procid, jobid,
             gethostname(), str(self.my_port)]
        msg = sep.join(m)
        sock.send('%10s%s' % (len(msg), msg))
    
def main():
    if len(sys.argv) != 8:
        sys.exit(1)
    proc = OrderbyOp()
    proc.master_name = sys.argv[1]
    proc.master_port = string.atoi(sys.argv[2])
    proc.cqid = sys.argv[3]
    proc.opid = sys.argv[4]
    proc.my_port = string.atoi(sys.argv[5])
    proc.log_dir = sys.argv[6]
    proc.procid = sys.argv[7]
    if not os.path.exists(proc.log_dir): os.makedirs(proc.log_dir)
    cur_time = time.strftime('%Y-%m-%d-%H-%M-%S',time.localtime(time.time()))
    ParaLiteLog.init(
        "%s/distinct-%s-%s-%s.log" % (
            proc.log_dir, gethostname(), proc.cqid, proc.procid),
        logging.DEBUG)
    ParaLiteLog.info(
        "--distinct op %s process %s on node %s is started" % (
            proc.opid, proc.procid, gethostname()))
    proc.start()
    
    
if __name__=="__main__":
#    test()
    main()
