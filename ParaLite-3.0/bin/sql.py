import sqlite3
import sys
import string
import time
import cPickle
import base64
import os
import random
import cStringIO
import traceback
import threading
import multiprocessing
import logging
from socket import *
import subprocess
import shlex

# set the home direcotry of paraLite into system's path
sys.path.append(os.path.abspath(os.path.join(sys.path[0], os.path.pardir)))

import dload_client
import conf
import dpp
from cqueue import cqueue
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
    
class mul:
    """A User-Defined Aggregation
    """
    def __init__(self):
        self.product = 1

    def step(self, value):
        try:
            newvalue = value
            if isinstance(value, unicode):
                newvalue = value.encode("ascii")
            if isinstance(newvalue, str):
                newvalue = string.atoi(newvalue)
            self.product *= newvalue
        except:
            ParaLiteLog.info(traceback.format_exc())
            raise(Exception(traceback.format_exc()))
        
    def finalize(self):
        return self.product

class SqlOp:
    
    SOCKET_OUT = "socket_out"
    MULTI_FILE = 0
    SINGLE_BUFFER = 1
    MULTI_BUFFER = 2
    MAX_SIZE = get_max_size()
    
    def __init__(self):
        self.expression = None
        self.dest = None
        self.attrs = {}
        self.output = []
        self.split_key = None
        self.name = None
        self.opid = None
        self.cqid = None
        self.status = None
        self.cost_time = 0
        self.client = (None, 0)
        self.p_node = {}
        self.my_port = 0
        self.hostname = None
        self.database = None
        self.master_name = None
        self.master_port = 0
        self.output_dir = None
        self.cache_size = None
        self.temp_store = None
        self.client_sock = None
        self.dest_table = None        
        self.dest_db = None
        self.db_col_sep = None
        self.db_row_sep = None
        self.limit = -1
        self.distinct = False
        self.fashion = None
        self.hash_key = None
        self.hash_key_pos = []
        self.partition_num = 0
        self.log_dir = None
        self.temp_dir = None
        self.node = None
        self.is_checkpoint = None
        
        self.iom = ioman.mk_ioman()
        self.is_running = True
        self.no_error = True
        self.job_data = {}  # {jobid : data_size}
        self.result = {} # {part_id:result} result = [csio] or [filename]
        self.data_status = {}  # {part_id:[(pos_in_result, status)]}
        self.reader = None 
        self.result_type = None
        self.replica_result = {} # the result data for a failed node
        self.failed_node = []
        self.total_size = 0
        self.total_time = 0
        self.job_list = [] # store jobs in order

        ############
        # for experiments
        self.ex_w_time = 0
        self.ex_s_time = 0
        self.ex_f = None
        self.notifier = None
        self.process_queue = multiprocessing.Queue()
        self.processes = []
        self.threads = []
        
    def parse_args(self, msg):
        b1 = base64.decodestring(msg)
        dic = cPickle.loads(b1)  
        for key in dic.keys():
            value = dic[key]
            if hasattr(self, key):
                setattr(self, key, value)
        if not os.path.exists(self.temp_dir): os.makedirs(self.temp_dir)

    def report_error(self, err):
        sock = socket(AF_INET, SOCK_STREAM)
        sock.connect((self.master_name, self.master_port))
        sock.send("%10s%s" % (len(err), err))
        sock.close()

    def collect_info(self):
        i = 1
        interval = 3
        time.sleep(interval)
        re_time = 0
        while self.is_running:
            cur_time = time.time()
            base_load = re_time
            if self.ex_s_time == 0:
                re_time = self.ex_w_time 
            else:
                re_time = self.ex_w_time + cur_time - self.ex_s_time
            self.ex_f.write("%s\t%s\n" % (i * interval, re_time - base_load))
            self.ex_f.flush()
            time.sleep(interval)
            i += 1
            
        self.ex_f.close()
        
    def sql_proc(self):
        try:
            ParaLiteLog.debug("sql proc : START")
            # start local socket server to listen all connections
            ch = self.iom.create_server_socket(AF_INET,
                                               SOCK_STREAM, 100, ("", self.my_port)) 
            n, self.my_port = ch.ss.getsockname()
            ParaLiteLog.debug("listen on port : %s ..." % str(self.my_port))
            
            # register local port to the master
            self.register_to_master(self.cqid, self.opid, gethostname(), self.my_port)
            ParaLiteLog.debug("reg to master: FINISH")
            
            while self.is_running:
                s_time = time.time()
                ev = self.next_event(None)
                if isinstance(ev, ioman_base.event_accept):
                    self.handle_accept(ev)
                if isinstance(ev, ioman_base.event_read):
                    if ev.data != "":
                        e_time = time.time()
                        self.handle_read(ev)

            for thd in self.threads:
                thd.join()
            for proc in self.processes:
                proc.join()
            ParaLiteLog.info("--sql node %s on %s is finished--" % (self.opid,
                                                                    gethostname()))
            #self.notifier.join()
        except KeyboardInterrupt, e:
            self.report_error("ParaLite receives a interrupt signal and then will close the process\n")
            ParaLiteLog.info("--sql node %s on %s is finished--" % (self.opid,
                                                                    gethostname()))
            sys.exit(1)
        except Exception, e1:
            ParaLiteLog.debug(traceback.format_exc())
            self.report_error(traceback.format_exc())
            sys.exit(1)

    def handle_accept(self, event):
        event.new_ch.flag = SqlOp.SOCKET_OUT
        event.new_ch.buf = cStringIO.StringIO()

    def process_job(self, jobid, exp, target_db, process_queue):
        s_time = time.time()
        rs_type, rs, t_size = self.proc_select(jobid, exp, target_db)
        e_time = time.time()
        process_queue.put((jobid, rs_type, rs, t_size, e_time-s_time))
        ParaLiteLog.debug("Job %s cost %s " % (jobid, e_time-s_time))

    def scan_process_queue(self, process_queue):
        while self.is_running:
            data = process_queue.get()
            jobid, rs_type, old_rs, t_size, costtime = data
            ################################
            rs = []
            for d in old_rs:
                csio = cStringIO.StringIO()
                csio.write(d)
                rs.append(csio)
            ################################
            self.job_data[jobid] = t_size
            self.job_list.append(jobid)
            self.result_type = rs_type
            self.total_size += t_size

            if len(rs) == 1:
                # use jobid as the key
                self.result[jobid] = rs
            else:
                # use partid as the key
                for i in range(len(rs)):
                    if i not in self.result:
                        self.result[i] = [rs[i]]
                    else:
                        self.result[i].append(rs[i])
            if self.total_time == 0:
                self.total_time = costtime
            self.send_status_to_master(jobid, conf.PENDING)


    def handle_read(self, event):
        message = event.data[10:]

        sep = conf.SEP_IN_MSG
        m = message.split(sep)
        try:
            if m[0] == "LOAD_RECORD":
                #################################################
                # start a thread to notify the computation time
                self.ex_f = open("/home/ting/hpdc/%s-load.dat" % gethostname(), "wb")
                self.notifier = threading.Thread(target = self.collect_info)
                self.notifier.setDaemon(True)
                self.notifier.start()
                #################################################

            if m[0] == conf.DATA_END:
                ParaLiteLog.debug("MESSAGE: %s" % message)
                # all data is dipatched to the parent nodes
                self.send_status_to_master(" ".join(self.job_data), conf.ACK)
                ParaLiteLog.debug("notify ACK to master")                    
                self.is_running = False
                
            elif message == conf.END_TAG:
                ParaLiteLog.debug("MESSAGE: %s" % message)
                self.send_status_to_master(" ".join(self.job_data), conf.ACK)
                self.is_running = False

            elif message == conf.DLOAD_END_TAG:
                ParaLiteLog.debug("---------import finish---------")
                self.send_status_to_master(" ".join(self.job_data), conf.ACK)
                self.is_running = False

            elif message == conf.EXIT:
                ParaLiteLog.debug("MESSAGE: %s" % message)
                self.is_running = False
            
            elif m[0] == conf.JOB_ARGUMENT:
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
                    # delete all temporary files for this operator
                    os.system("rm -f %s/%s_%s" % (self.temp_dir, "sql", self.opid))

                ###############################
                # scanner = threading.Thread(target=self.scan_process_queue, args=(self.process_queue, ))
                # scanner.setDaemon(True)
                # scanner.start()
                # self.threads.append(scanner)
                ##########################
            elif m[0] == conf.JOB:
                self.ex_s_time = time.time()
                ParaLiteLog.debug("MESSAGE: %s" % message)
                s_time = time.time()
                jobid = m[1]
                target_db = m[2].split()
                exp = self.expression
                ParaLiteLog.debug("*****JOB %s******:start" % jobid)
                
                # FAULT TOLERANCE:
                if jobid in self.job_data:
                    # this is a failed job, we should first delete the old result value
                    if self.dest == conf.DATA_TO_ANO_OP and self.partition_num > 1:
                        for partid in self.result:
                            pos = self.job_list.index(jobid)
                            self.result[partid][pos] = ""
                    else:
                        self.result[jobid] = ""
                
                if exp.lower().startswith("select"):
                    """
                    selection task: (1), execute sql (2), notify the result to
                    the master (3), wait for the DATA_PERSIST message from the
                    master (4), persist data if so (5), notify ACK to the master
                    """
                    ParaLiteLog.info("proc_select: START")
                    st_time = time.time()
                    
                    rs_type, rs, t_size = self.proc_select(jobid, exp, target_db)

                    et_time = time.time()
                    ParaLiteLog.debug("Job %s cost time %s second" % (jobid, (et_time - st_time)))
                    # FAULT TOLERANCE:
                    if jobid in self.job_data:
                        # this is a failed job
                        if self.dest == conf.DATA_TO_ANO_OP and self.partition_num > 1:
                            for partid in self.result:
                                pos = self.job_list.index(jobid)
                                self.result[partid][pos] = rs[partid]
                        else:
                            self.result[jobid] = rs
                        self.send_status_to_master(jobid, conf.PENDING)
                        return
                        
                    self.job_data[jobid] = t_size
                    self.job_list.append(jobid)
                    self.total_size += t_size
                    
                    # store the result of one job to the final result
                    if len(rs) == 1:
                        if self.dest == conf.DATA_TO_ANO_OP:
                            # dest is AGGR op or ORDER op, use 0 as the key
                             if 0 not in self.result:
                                 self.result[0] = rs
                             else:
                                 self.result[0].append(rs[0])

                             if self.is_checkpoint == 1:
                                 self.write_data_to_disk(0, rs[0].getvalue())
                        else:
                            # dest is UDX op, use jobid as the key
                            self.result[string.atoi(jobid)] = rs
                            if self.is_checkpoint == 1:
                                self.write_data_to_disk(0, rs[0].getvalue())
                        
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
                    if rs_type != self.MULTI_FILE:
                        if self.is_checkpoint is not None and self.is_checkpoint == conf.CHECKPOINT or self.total_size > self.MAX_SIZE:
                            for dataid in self.result:
                                data = ""
                                for d in self.result[dataid]:
                                    data += d.getvalue()
                                self.write_data_to_disk(dataid, data)
                            self.result_type = self.MULTI_FILE
                            
                    e_time = time.time()
                    if self.total_time == 0:
                        self.total_time = (e_time - s_time)
                    self.send_status_to_master(jobid, conf.PENDING)

                elif exp.lower().startswith("create"):
                    ParaLiteLog.info("proc_create: START")
                    ParaLiteLog.info("SQL: %s" % exp)                    
                    self.proc_create(exp, target_db)
                    ParaLiteLog.info("proc_create: START")
                    self.send_status_to_master(jobid, conf.ACK)
                    self.is_running = False
                elif exp.lower().startswith("drop"):
                    ParaLiteLog.info("proc_drop: START")            
                    self.proc_drop(exp, target_db)
                    self.send_status_to_master(jobid, conf.ACK)
                    self.is_running = False
                ParaLiteLog.debug("*****JOB %s******:finish" % jobid)
                self.ex_w_time += (time.time() - self.ex_s_time)
                self.ex_s_time = 0

            elif m[0] == conf.JOB_END:
                ParaLiteLog.debug("MESSAGE: %s" % message)
                # all jobs are finished
                # create a dictionary to store the status of each part of data
                data_status = {}  # {data_id:[(pos_in_result, status)]}
                for dataid in self.result:
                    if dataid not in data_status:
                        data_status[dataid] = []
                    for i in range(len(self.result[dataid])):
                        data_status[dataid].append((i, 1))
                self.data_status = data_status
                self.reader = self.get_next_reader()
                    
                self.send_rs_info_to_master(self.total_size, self.total_time)

                # distribute data
                if self.dest == conf.DATA_TO_ONE_CLIENT:
                    self.distribute_data()
                    self.send_status_to_master(" ".join(self.job_data), conf.ACK)
                    self.is_running = False
                elif self.dest == conf.DATA_TO_DB:
                    self.distribute_data()
 
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

            elif m[0] == conf.DATA_PERSIST:
                ParaLiteLog.debug("MESSAGE: %s" % message)
                # if the data is requried to be persisted or not
                self.process_ck_info(m)
                
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
                ParaLiteLog.debug("send data susscufully   %s %s --> %s" % (self.opid, gethostname(), destnode))

            elif m[0] == conf.DATA_DISTRIBUTE_UDX:
                ParaLiteLog.debug("MESSAGE: %s" % message)
                # send data to udx client
                # m[1:] = worker.id:jobid:(node:port | addr):size
                
                if len(m) == 6:
                    w_id, jobid = m[1:3]
                    addr = (m[3], string.atoi(m[4]))
                    t = AF_INET
                    bk = string.atoi(m[5])
                elif len(m) == 5:
                    w_id, jobid = m[1:3]
                    addr = m[3]
                    t = AF_UNIX
                    bk = string.atoi(m[4])
                data = self.get_data_by_blocksize(jobid, bk)
                if not data:
                    # if we don't send something here, udx will not send KAL
                    # again, and then they will not receive data again, the whole
                    # process will be blocked for ever
                    msg = sep.join([conf.DATA, "EMPTY"])
                else:
                    msg = sep.join([conf.DATA, data])
                self.send_data_to_node(msg, t, addr)
                
            elif m[0] == conf.DATA_REPLICA:
                ParaLiteLog.debug("MESSAGE: %s" % message)
                # message --> DATA_REPLICA:DATANODE:DATAID:DATA
                datanode, dataid = m[1:3]
                f_name = self.get_file_name_by_data_id(gethostname(), dataid)
                fr = open(f_name, "wa")
                fr.write(m[4])
                fr.close()

            elif m[0] == conf.NODE_FAIL:
                ParaLiteLog.debug("MESSAGE: %s" % message)
                # message --> NODE_FAIL:FAILED_NODE:REPLICA_NODE
                failed_node, replica_node = m[1:3]
                self.failed_node.append(failed_node)
                if replica_node == gethostname():
                    # load replica data for the failed node
                    self.recovery_data(self.replica_result, replica_node)
        except Exception, e:
            es("in sql_proc : %s" % traceback.format_exc())
            ParaLiteLog.info(traceback.format_exc())
            self.is_running = False
            self.no_error = False

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
                    ParaLiteLog.debug("CDM: %s" % cmd)
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

    def recovery_data(self, result, node):
        if self.dest == conf.DATA_TO_ANO_OP and self.partition_num > 1:
            for partid in result:
                f_name = self.get_file_name_by_data_id(node, partid)
                result[partid] = [f_name]
        else:
            for jobid in self.job_data:
                f_name = self.get_file_name_by_data_id(node, jobid)
                result[jobid] = [f_name]

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
            self.temp_dir, "sql", self.opid, node, data_id)
    
    def send_data_to_node(self, msg, tp, addr):
        # if send data fails, try 5 times 
        i = 0
        while i < 10:
            try:
                sock = socket(tp, SOCK_STREAM)
                sock.connect(addr)
                sock.send("%10s%s" % (len(msg),msg))
                sock.close()
                break
            except Exception, e:
                if i == 10:
                    raise(Exception(e))
                i += 1
                time.sleep(1)

    def get_data_by_blocksize(self, jobid, bksize):
        if self.reader is None:
            return None
        data = self.reader.read(bksize)
        if not data:
            read_size = 0
        else:
            read_size = len(data)

        if read_size < bksize or read_size == bksize and read_size == self.job_data[jobid]:
            # while True:
            #     if self.reader is not None:
            #         self.reader.close()
            #     self.reader = None
            #     self.reader = self.get_next_reader()
            #     if self.reader is None:
            #         break
            #     new_data = self.reader.read(bksize - read_size)
            #     data += new_data
            #     read_size = len(data)
            #     if read_size >= bksize:
            #         break
            if self.reader is not None:
                self.reader.close()
            self.reader = None
            self.reader = self.get_next_reader()
            
            return data
            
        if self.db_row_sep == "\n":
            if not data.endswith("\n"):
                extra_data = self.reader.readline()
                if extra_data:
                    data += extra_data
            return data
        else:
            if data:
                pos = data.rfind(self.db_row_sep)
                ParaLiteLog.info(pos)
                send_ds =  self.left_ds + data[0:pos]
                self.left_ds = data[pos+len(self.db_row_sep):]
                return send_ds
            else:
                return None

    def get_next_reader(self):
        reader = None
        for dataid in self.data_status:
            for i in range(len(self.data_status[dataid])):
                sta = self.data_status[dataid][i]
                if sta[1] == 1:
                    data = self.result[dataid][i]
                    if isinstance(data, str):
                        # this is a file
                        reader = open(data, "rb")
                    else:
                        data.seek(0)
                        reader = data
                    sta = (i, 0)
                    self.data_status[dataid][i] = sta
                    return reader
        return None
        
    def get_data_by_part_id(self, result, part_id):
        rs = ""
        if part_id not in result:
            return rs
        
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

    def register_to_master(self, cqid, opid, node, port):
        sep = conf.SEP_IN_MSG
        msg = '%s%s%s%s%s%s%s%s%s%s%s%s%s' % (conf.REG, sep, conf.DATA_NODE,
                                          sep, cqid, sep, opid, sep, gethostname(),
                                          sep, self.my_port, sep, "")
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

    def proc_drop(self, exp, target_db):
        try:
            for db in target_db:
                conn = sqlite3.connect(db)
                c = conn.cursor()
                c.execute(exp)
                conn.commit()
                conn.close()
        except sqlite3.OperationalError, e:
            es("%s: %s" % (gethostname(), " ".join(e.args)))
            ParaLiteLog.info(traceback.format_exc())
        except Exception, e:
            es(traceback.format_exc())
            ParaLiteLog.info(traceback.format_exc())

    def proc_create(self, exp, target_db):
        try:
            # first of all, check if the directory holds database exists or not
            for db in target_db:
                parent = db[0:db.rfind(os.sep)]
                if not os.path.exists(parent):
                    os.makedirs(parent)
                conn = sqlite3.connect(db)
                c = conn.cursor()
                c.execute(exp)
                conn.commit()
                conn.close()
        except sqlite3.OperationalError, e:
            ParaLiteLog.info(traceback.format_exc())
            raise(Exception("ERROR: in proc_create: %s: %s" % (gethostname(),
                                                               " ".join(e.args))))
    def proc_select(self, jobid, exp, target_db):
        assert len(target_db) == 1
        cur_db = target_db[0]
        try:
            conn = sqlite3.connect(cur_db)
            conn.text_factory = str
            
            # register the user-defined aggregate
            conn.create_aggregate("mul", 1, mul)

            c = conn.cursor()
            """
            if self.temp_store != 0:
                c.execute('pragma temp_store=%s' % (self.temp_store))
            if self.cache_size != -1:
                c.execute('pragma cache_size=%s' % (self.cache_size))
            """

            # for test
            c.execute('pragma temp_store=memory')
            c.execute('pragma cache_size=2073741824')

            ParaLiteLog.info("start to execute sql: %s" % exp)
            
            col_sep = self.db_col_sep
            row_sep = self.db_row_sep
            num_of_dest = self.partition_num

            if self.dest == conf.DATA_TO_ANO_OP and num_of_dest > 1:
                columns = self.output
                split_key = self.split_key
                assert split_key is not None
                
                # partition data in hash fashion
                pos = []
                for key in split_key:
                    pos.append(columns.index(key))
                data_part_list = []
                for i in range(self.partition_num):
                    data_part_list.append(cStringIO.StringIO())
                size = 0
                t_size = 0
                for row in c.execute(exp):
                    part_id = abs(hash(self.db_col_sep.join(str(row[p]) for p in pos))) % num_of_dest
                    #part_id = abs(hash(row[pos[0]])) % num_of_dest
                    data = col_sep.join(str(s) for s in row)
                    """
                    size += len(data)
                    if size > self.MAX_SIZE:
                        for partid in data_part_list:
                            fs = self.write_data_to_disk(
                                partid, data_part_list[partid])
                            # delete all data in csio
                            data_part_list[partid].truncate(0)
                        t_size += size
                        size = 0
                        self.result_type = self.MULTI_FILE
                    """
                    data_part_list[part_id].write(data)
                    data_part_list[part_id].write(row_sep)

                for i in range(len(data_part_list)):
                    t_size += len(data_part_list[i].getvalue())
                    
                ParaLiteLog.debug("finish to retrieve the result: %s" % t_size)
                
                if self.result_type == self.MULTI_FILE:
                    for partid in data_part_list:
                        self.write_data_to_disk(
                            partid, data_part_list[partid].getvalue())
                        del data_part_list
                    return self.MULTI_FILE, None, t_size
                else:
                    ########################
                    # new_list = []
                    # for d in data_part_list:
                    #     new_list.append(d.getvalue())
                    # return self.MULTI_BUFFER, new_list, t_size
                    ###################
                    return self.MULTI_BUFFER, data_part_list, t_size
                
            else:
                csio = cStringIO.StringIO()
                t_size = 0
                size = 0 # record the size of current data
                data_pos = [] # the file name of data if persisted
                for row in c.execute(exp):
                    # NOTE:  For aggregation SQL, e.g. "select max(col) from T ..."
                    # if there is no record in T, (None,) will be returned
                    if row[0] is None:
                        continue
                    data = col_sep.join(str(s) for s in row)
                    size += len(data)
                    if size >= self.MAX_SIZE:
                        result_type = self.MULTI_FILE
                        self.write_data_to_disk(jobid, csio.getvalue())
                        # delete all data in csio
                        csio.truncate(0)
                        t_size += size
                        size = 0
                    csio.write(data)
                    csio.write(row_sep)

                t_size += len(csio.getvalue())
                ParaLiteLog.debug("finish to retrieve the result: %s" % t_size)

                if self.result_type == conf.MULTI_FILE:
                    self.write_data_to_disk(jobid, csio.getvalue())
                    del csio
                    return conf.MULTI_FILE, None, t_size
                else:
                    return self.SINGLE_BUFFER, [csio], t_size

        except sqlite3.OperationalError, e:
            ParaLiteLog.info(traceback.format_exc())
            raise(Exception("%s: QueryExecutionError: %s" % (gethostname(),
                                                             traceback.format_exc())))
        except Exception, e:
            ParaLiteLog.info(traceback.format_exc())
            raise(Exception("ERROR: %s: ReplyToClientError: %s" % (gethostname(),
                                                                   traceback.format_exc())))

    def distribute_data(self):
        # handle the limit condition: get the first N records
        # E.g. select ... limit 10, the master firstly decides the limit
        # number for each process and set the limit value for each process
        # to be the post-limit

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
        
        if self.dest == conf.DATA_TO_DB:
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

            # dload_client.dload_client().load_internal_buffer(
            #     master, self.cqid, gethostname(), self.my_port, self.dest_db,
            #     self.dest_table, data, conf.LOAD_FROM_API, self.fashion, 
            #     self.hash_key, self.hash_key_pos, self.db_col_sep, row_sep,
            #     col_sep, False, "0", self.log_dir)

        elif self.dest == conf.DATA_TO_ONE_CLIENT:
            random_num = random.randint(0, len(self.client_sock) - 1)
            addr = self.client_sock[random_num]
            sock = socket(AF_INET, SOCK_STREAM)
            sock.connect(addr)

            data_s = data.getvalue()
            ParaLiteLog.info("DATA SIZE = %s" % len(data_s))
            sock.send("%10s%s" % (len(data_s), data_s))
            re = sock.recv(10)
            assert re == "OK"
            sock.close()

    def send_rs_info_to_master(self, total_size, total_time):
        # RS:DATANODE:cqid:opid:rs_type:partition_num:total_size:total_time
        addr = (self.master_name, self.master_port)
        sock = socket(AF_INET, SOCK_STREAM)
        sock.connect(addr)
        sep = conf.SEP_IN_MSG
        job_size = base64.encodestring(cPickle.dumps(self.job_data))
        m = [conf.RS, conf.DATA_NODE, self.cqid, self.opid, gethostname(),
             str(self.my_port), str(self.result_type), 
             str(self.partition_num), str(total_size), str(total_time), job_size]
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


def test():
    proc = SqlOp()
    proc.database = '/data/local/chenting/test.db'
    proc.expression = 'select * from test2'
    proc.dest = 'sql_0.dat'
    proc.sql_proc()

def main():
    if len(sys.argv) != 7:
        sys.exit(1)
    proc = SqlOp()
    proc.master_name = sys.argv[1]
    proc.master_port = string.atoi(sys.argv[2])
    proc.cqid = sys.argv[3]
    proc.opid = sys.argv[4]
    proc.my_port = string.atoi(sys.argv[5])
    proc.log_dir = sys.argv[6]
    
    if not os.path.exists(proc.log_dir): os.makedirs(proc.log_dir)
    ParaLiteLog.init("%s/sql-%s-%s-%s.log" % (
        proc.log_dir, gethostname(), proc.cqid, proc.opid),
                     logging.DEBUG)
    ParaLiteLog.info("--sql node %s on %s is started" % (proc.opid, gethostname()))
    proc.sql_proc()
    
if __name__=='__main__':
#    test()
    main()

