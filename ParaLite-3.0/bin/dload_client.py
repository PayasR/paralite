import os
import traceback
import time
import string
import threading
import random
import pwd
import cStringIO
import logging
import sys
import cPickle
from socket import *

# set the home direcotry of paraLite into system's path
sys.path.append(os.path.abspath(os.path.join(sys.path[0], os.path.pardir)))

import conf
from lib.logger import ParaLiteLog

DATA_MAX_SIZE = 4*1024*1024*1024            # 1GB

class dload_client:
    def __init__(self):
        self.db_col_sep = None
        self.file_reader = None
        self.files = {}
        self.flag = 0

    def get_next_file(self):
        for f in self.files:
            if self.files[f] == 1:
                self.files[f] = 0
                return f
        return None

    def replicate_data(self, table, f_list, total_size, nodes):
        size = DATA_SIZE_BUFFERED
        read_size = 0
        while read_size < total_size:
            data = self.get_data_as_bk(size)
            for n in nodes:
                name = n.split(":")[0]
                port = string.atoi(n.split(":")[1])
                self.send_to_node(table, data, name, port)
            read_size += len(data)

    """
    def hash_data_buffer(self, buf, key_pos, nodes, node_buf, row_sep, col_sep):
        if col_sep is None:
            SEP = self.db_col_sep
        else:
            SEP = col_sep
        buf.seek(0)
        if row_sep is None or row_sep == "\n":
            while True:
                line = buf.readline().strip()
                if not line:
                    break
                key = line.split(SEP)[key_pos]
                pnum = abs(hash(key)) % len(nodes)
                sep = conf.SEP_IN_MSG
                node_buf[nodes[pnum].split(sep)[0]].write("%s\n" % line)
        else:
            lines = buf.read().split(row_sep)
            for line in lines:
                if line == "":
                    continue
                key = string.strip(line).split(SEP)[key_pos]
                pnum = abs(hash(key)) % len(nodes)
                sep = conf.SEP_IN_MSG
                node_buf[nodes[pnum].split(sep)[0]].write(line)
                node_buf[nodes[pnum].split(sep)[0]].write(row_sep)
    """
    def hash_data_buffer(self, buf, key_pos, nodes, row_sep, col_sep, chunk_num, sub_dbs):
        db_buf = {}  # {db_name : data}
        for db in sub_dbs:
            db_buf[db] = cStringIO.StringIO()
        if col_sep is None:
            SEP = self.db_col_sep
        else:
            SEP = col_sep
        buf.seek(0)
        if row_sep is None or row_sep == "\n":
            while True:
                line = buf.readline().strip()
                if not line:
                    break
                key = " ".join(line.split(SEP)[kp] for kp in key_pos)
                pnum = abs(hash(key)) % (len(nodes) * chunk_num)
                sep = conf.SEP_IN_MSG
                db_name = sub_dbs[pnum]
                db_buf[db_name].write("%s\n" % line)
        else:
            lines = buf.read().split(row_sep)
            for line in lines:
                if line == "":
                    continue
                key = string.strip(line).split(SEP)[key_pos]
                pnum = abs(hash(key)) % (len(nodes)*chunk_num)
                sep = conf.SEP_IN_MSG
                db_name = sub_dbs[pnum]
                db_buf[db_name].write(line)
                db_buf[db_name].write(row_sep)                
        return db_buf
        
    def hash_data_file(self, data, key_pos, nodes, row_sep, col_sep, chunk_num, sub_dbs):
        sep = conf.SEP_IN_MSG
        db_buf = {}
        for db in sub_dbs:
            db_buf[db] = cStringIO.StringIO()
        if col_sep is None: SEP = self.db_col_sep
        else: SEP = col_sep
        records = data.split(row_sep)
        count = 1
        for line in records:
            if line == "":
                continue
            key = " ".join(line.strip().split(SEP)[kp] for kp in key_pos)
            pnum = abs(hash(key)) % (len(nodes)*chunk_num)
            if pnum == 0:
                count += 1
            db_name = sub_dbs[pnum]
            db_buf[db_name].write("%s%s" % (line.strip(), row_sep))
            
        ParaLiteLog.debug("count %s" % count)
        
        for db in db_buf:
            ParaLiteLog.debug("%s -- > %s" % (db, len(db_buf[db].getvalue())))
            break

        return db_buf
            
    def get_data_as_bk(self, size):
        if self.file_reader is None:
            return None
        data = self.file_reader.read(size)
        if not data:
            read_size = 0
        else:
            read_size = len(data)
        if read_size < size:
            while True:
                self.file_reader.close()
                self.file_reader = None
                next_file = self.get_next_file()
                if next_file is None:
                    return data
                self.file_reader = open(next_file, "rb")
                new_data = self.file_reader.read(size - read_size)
                data += new_data
                read_size = len(data)
                if read_size >= size:
                    break
        if not data.endswith("\n"):
            extra_data = self.file_reader.readline()
            if extra_data:
                data += extra_data
        if data:
            return data
        else:
            return None

    def really_send(self, addr, data):
        if isinstance(addr, str):
            tag = AF_UNIX
        else:
            tag = AF_INET
        sock = socket(tag, SOCK_STREAM)
        sock.connect(addr)
        sock.send(data)
        sock.close()
        
    def send_to_node(self, db, table, data, addr, row_sep, col_sep, is_replace):
        sep = conf.SEP_IN_MSG
        req_info = "%s%s%s%s%s%s%s%s%s%s%s%s%s" % (conf.INFO, sep, db, sep, table, sep, self.db_col_sep, sep, col_sep, sep, row_sep, sep,is_replace)
        ParaLiteLog.info("sending %s  --> %s" % (req_info, addr[0]))
        self.really_send(addr, req_info)
        # use the first 10 charactors to indicate the database 
        self.really_send(addr, "%10s%s%s" % (len(db), db, data))
        ParaLiteLog.info("sending data : %s --> %s" % (len(data), repr(addr)))
        
    def recv_bytes(self, so, n):
        A = []
        while n > 0:
            x = so.recv(n)
            if x == "": break
            A.append(x)
            ParaLiteLog.debug(len(x))
            n = n - len(x)
        return string.join(A, "")

    def range_data(self):
        ParaLiteLog.info("Now RANGE FASHION is not supported...")
        
    def load_internal(self, master, cqid, node, database, table, files, tag,
                      fashion, key, key_pos, db_col_sep, row_sep, col_sep,
                      is_replace, client_id, LOG_DIR):
        cur_time = time.strftime('%Y-%m-%d-%H-%M-%S',time.localtime(time.time()))
        if tag == 0 or (tag == 1 and isinstance(files, list)):
            self.load_internal_file(master, cqid, node, database, table, files,
                                    tag, fashion, key, key_pos, db_col_sep, row_sep,
                                    col_sep, is_replace, client_id, LOG_DIR)
        else:
            self.load_internal_buffer(master, cqid, node, database, table, files,
                                      tag, fashion, key, key_pos, db_col_sep, row_sep,
                                      col_sep, is_replace, client_id, LOG_DIR)

    def load_internal_buffer(self, master, cqid, node, database, table, buf, tag,
                             fashion, key, key_pos, db_col_sep, row_sep, col_sep,
                             is_replace, client_id, LOG_DIR):
        ParaLiteLog.info("load_internal: START")
        ParaLiteLog.info("row separator = %s col separator = %s" % (row_sep, col_sep) )
        self.db_col_sep = db_col_sep
        total_size = len(buf.getvalue())
        send_size = 0
        try:

            so_master = socket(AF_INET, SOCK_STREAM)
            so_master.connect(master)
            #REQ:cqid:NODE:DATABASE:TABLE:DATA_SIZE:TAG:FASHION:row_sep
            temp_sep = row_sep
            if row_sep is None or row_sep == "\n":
                temp_sep = "NULL"
            sep = conf.SEP_IN_MSG
            msg = sep.join(str(s) for s in [conf.REQ, cqid, node, database, table,
                                            total_size, tag, fashion, temp_sep, client_id])
            so_master.send("%10s%s" % (len(msg), msg))
            ParaLiteLog.info("send a request to the master")
            length = self.recv_bytes(so_master, 10)
            """
            received message = nodes # sub_dbs # chunk_num # replica_info # client_number
            
            nodes should be:
            n1:p1:l1 , n2:p2:l2 , ...               IF fashion = HASH_FASHION 
            n1:p1:l1:s1:num , n2:p2:l2:s2:num , ... IF fashion = ROUND_ROBIN
            TBD                                     IF fashion = RANGE_FASHION

            node_db_info: db_1_1 , db_1_2 , db_2_1, ...
            replica_info: db_1_1 db_1_1_r_1 node1 , db_1_2 db_1_2_r_1 node2 , ...
            """
            mm = self.recv_bytes(so_master, string.atoi(length)).split("#")
            if total_size == 0:
                sep = conf.SEP_IN_MSG
                msg = "%s%s%s%s%s%s%s" % (conf.REQ, sep, conf.END_TAG, sep,
                                          gethostname(), sep, client_id)
                so_master.send("%10s%s" % (len(msg), msg))
                ParaLiteLog.info("sending to master: %s" % (conf.END_TAG))
                l = string.atoi(self.recv_bytes(so_master, 10))
                reply = self.recv_bytes(so_master, l)
                ParaLiteLog.info("receive from master: %s" % (reply))
                assert reply == conf.END_TAG
                so_master.close()
                

            ParaLiteLog.info("receive the information from the master %s" % mm)
            nodes = mm[0].split(",")
            sub_dbs = mm[1].split(",")
            chunk_num = string.atoi(mm[2])
            replica = mm[3]
            client_id = mm[4]
            
            replica_info = {} # {db_name : {replica_db_name:node}}
            if replica != "":
                for ll in replica.split(","):
                    lll = ll.split(" ")
                    if lll[0] not in replica_info:
                        replica_info[lll[0]] = {}
                    replica_info[lll[0]][lll[1]] = lll[2]
            ParaLiteLog.info(nodes)
            node_addr = {} # {node:addr}
            for node in nodes:
                m = node.split(conf.SEP_IN_MSG)
                if m[0] == gethostname(): addr = m[2]
                else: addr = (m[0], string.atoi(m[1]))
                node_addr[m[0]] = addr

            ss1 = time.time()
            if nodes == []:
                ParaLiteLog.info("there is no data to load")
            elif fashion == conf.HASH_FASHION:
                ParaLiteLog.info(fashion)
                # get the data for each sub db
                # db_buf = {db_name, buffer_of_data}
                db_buf = self.hash_data_buffer(buf, key_pos, nodes, row_sep, col_sep, chunk_num, sub_dbs)
                for db in db_buf:
                    data = db_buf[db].getvalue()
                    node = db.split("_")[-3]
                    self.send_to_node(db, table, data, node_addr[node], row_sep, col_sep, is_replace)
                    if db in replica_info:
                        for rdb in replica_info[db]:
                            node = replica_info[db][rdb]                            
                            self.send_to_node(rdb, table, data, node_addr[node], row_sep, col_sep, is_replace)
                """
                buf_scanner = threading.Thread(target=self.scan_buf,
                args=(table, node_buf, node_addr, row_sep, col_sep, is_replace))
                buf_scanner.setDaemon(True)
                buf_scanner.start()
                buf_scanner.join()
                """
            elif fashion == conf.REPLICATE_FASHION:
                self.replicate_data(table, files, total_size, nodes)
            elif fashion == conf.RANGE_FASHION:
                self.range_data()
            else:
                thds = []
                num_of_db = len(nodes) * chunk_num
                if row_sep is not None and row_sep != "\n":
                    whole_data = buf.getvalue()
                    lines = whole_data.split(row_sep)
                    if lines[len(lines)-1] == "":
                        lines.pop(len(lines)-1)
                    l = len(lines)
                    if l % num_of_db == 0:
                        num_each = l / num_of_db
                    else:
                        num_each = l / num_of_db + 1
                    i = 0
                    while i < num_of_db:
                        db = sub_dbs[i]
                        node = db.split("_")[-3]
                        cur_num = i*num_each + num_each
                        if cur_num > l:
                            cur_num = l
                        ds = row_sep.join(lines[i*num_each:cur_num])
                        thd = threading.Thread(target=self.send_to_node,
                                               args=(db, table, ds, node_addr[node], row_sep,
                                                     col_sep, is_replace))
                        thd.setDaemon(True)
                        thd.start()
                        thds.append(thd)
                        if db in replica_info:
                            for rdb in replica_info[db]:
                                node = replica_info[db][rdb]
                                thd = threading.Thread(target=self.send_to_node,
                                                       args=(rdb, table, ds, node_addr[node],
                                                             row_sep, col_sep, is_replace))
                                thd.setDaemon(True)
                                thd.start()
                                thds.append(thd)
                        i += 1
                else:
                    buf.seek(0)
                    i = 0
                    while i < num_of_db:
                        db = sub_dbs[i]
                        node = db.split("_")[-3]
                        node_id = i / chunk_num
                        size = string.atoi(nodes[node_id].split(conf.SEP_IN_MSG)[3]) / chunk_num
                        ParaLiteLog.info("start to get data as bk: %s" % (size))
                        ds = buf.read(size)
                        if ds is None:
                            ParaLiteLog.info("really get data as bk: 0")
                            continue
                        if not ds.endswith("\n"):
                            ds += buf.readline()

                        ParaLiteLog.info("really get data as bk: %s" % (len(ds)))
                        thd = threading.Thread(target=self.send_to_node,
                                               args=(db, table, ds, node_addr[node],
                                                     row_sep, col_sep, is_replace))
                        
                        thd.setDaemon(True)
                        thd.start()
                        thds.append(thd)
                        if db in replica_info:
                            for rdb in replica_info[db]:
                                node = replica_info[db][rdb]
                                thd = threading.Thread(target=self.send_to_node,
                                                       args=(rdb, table, ds, node_addr[node],
                                                             row_sep, col_sep, is_replace))
                                thd.setDaemon(True)
                                thd.start()
                                thds.append(thd)
                        i += 1
                for thd in thds:
                    thd.join()
            ss2 = time.time()
            sep = conf.SEP_IN_MSG
            msg = "%s%s%s%s%s%s%s" % (conf.REQ, sep, conf.END_TAG, sep,
                                      gethostname(), sep, client_id)
            so_master.send("%10s%s" % (len(msg), msg))
            ParaLiteLog.info("sending to master: %s" % (conf.END_TAG))
            l = string.atoi(self.recv_bytes(so_master, 10))
            reply = self.recv_bytes(so_master, l)
            ParaLiteLog.info("receive from master: %s" % (reply))
            assert reply == conf.END_TAG
            so_master.close()
            ss3 = time.time()
        except:
            ParaLiteLog.info(traceback.format_exc())

        
    def load_internal_file(self, master, cqid, node, database, table, files,
                           tag, fashion, key, key_pos, db_col_sep, row_sep,
                           col_sep, is_replace, client_id, LOG_DIR):
        global log
        ParaLiteLog.info("load_internal: START")
        ParaLiteLog.info("row separator = %s col separator = %s" % (row_sep, col_sep) )
        self.db_col_sep = db_col_sep        
        total_size = 0
        for f in files:
            self.files[f] = 1
            total_size += os.path.getsize(f)
        self.file_reader = open(self.get_next_file(), "rb")
        send_size = 0
        try:
            so_master = socket(AF_INET, SOCK_STREAM)
            so_master.connect(master)
            #REQ:cqid:NODE:DATABASE:TABLE:DATA_SIZE:TAG:FASHION:row_sep
            temp_sep = row_sep
            if row_sep is None or row_sep == "\n":
                temp_sep = "NULL"
            sep = conf.SEP_IN_MSG
            msg = sep.join([conf.REQ, cqid, node, database, table, str(total_size),
                            str(tag), fashion, temp_sep, client_id])

            so_master.send("%10s%s" % (len(msg), msg))
            ParaLiteLog.info("send a request to the master")
            length = self.recv_bytes(so_master, 10)
            """
            received message = nodes # sub_dbs # chunk_num # replica_info # client_number
            
            nodes should be (| is SEP_IN_MSG):
            n1 : p1|l1 , n2 : p2|l2 , ...               IF fashion = HASH_FASHION 
            n1 : p1|l1|s1|num , n2 : p2|l2|s2|num , ... IF fashion = ROUND_ROBIN
            TBD                                     IF fashion = RANGE_FASHION

            node_db_info: node1:[db_1_1] , node2:[db_1_2] , node3:[db_2_1], ...
            replica_info: db_1_1 db_1_1_r_1 node1 , db_1_2 db_1_2_r_1 node2 , ...
            """
            mm = self.recv_bytes(so_master, string.atoi(length)).split("#")
            ParaLiteLog.info("receive the information from the master")
            nodes = mm[0].split(",")
            sub_dbs = mm[1].split(",")
            chunk_num = string.atoi(mm[2])
            replica = mm[3]
            client_id = mm[4]
            
            replica_info = {} # {db_name : {replica_db_name:node}}
            if replica != "":
                for whole_re in replica.split(","):
                    lll = whole_re.split(" ")
                    if lll[0] not in replica_info:
                        replica_info[lll[0]] = {}
                    replica_info[lll[0]][lll[1]] = lll[2]

            node_addr = {} # {node:addr}
            for node in nodes:
                m = node.split(conf.SEP_IN_MSG)
                if m[0] == gethostname(): addr = m[2]
                else: addr = (m[0], string.atoi(m[1]))
                node_addr[m[0]] = addr
            
            thds = []
            if nodes == []:
                ParaLiteLog.info("there is no data to load")
            elif fashion == conf.HASH_FASHION:
                ParaLiteLog.info(fashion)
                if row_sep is not None and row_sep != "\n":
                    while True:
                        dst = self.get_data_as_bk(DATA_MAX_SIZE)
                        if dst is None:
                            ParaLiteLog.info("really get data as bk: 0")
                            break
                        ParaLiteLog.info("really get data as bk: %s" % (len(dst)))
                        pos = dst.rfind(row_sep)
                        ParaLiteLog.info(pos)
                        ds =  left_ds + dst[0:pos]
                        left_ds = dst[pos+len(row_sep):]
                        del dst
                        db_buf = self.hash_data_file(ds, key_pos, nodes,
                                                       row_sep, col_sep,
                                                       chunk_num, sub_dbs)
                        ParaLiteLog.debug("hash data finish %s" % len(ds))
                        del ds                        
                        for db in db_buf:
                            data = db_buf[db].getvalue()
                            node = db.split("_")[-3]
                            thd = threading.Thread(target=self.send_to_node,
                                                   args=(db, table, data, node_addr[node],
                                                         row_sep,col_sep,
                                                         is_replace))
                            thd.setDaemon(True)
                            thd.start()
                            thds.append(thd)
                            if db in replica_info:
                                for rdb in replica_info[db]:
                                    node = replica_info[db][rdb]
                                    self.send_to_node(rdb, table, data,
                                                      node_addr[node],
                                                      row_sep, col_sep, is_replace)

                else:
                    while True:
                        ds = self.get_data_as_bk(DATA_MAX_SIZE)
                        if ds is None:
                            ParaLiteLog.info("really get data as bk: 0")
                            break
                        ParaLiteLog.info("really get data as bk: %s" % (len(ds)))
                        db_buf = self.hash_data_file(ds, key_pos, nodes,
                                                       "\n", col_sep, chunk_num, sub_dbs)
                        for db in db_buf:
                            ParaLiteLog.debug(
                                "%s -- > %s" % (db, len(db_buf[db].getvalue())))
                            break
                        for db in db_buf:
                            data = db_buf[db].getvalue()
                            node = db.split("_")[-3]

                            thd = threading.Thread(target=self.send_to_node,
                                                   args=(db, table, data,
                                                         node_addr[node],
                                                         row_sep,col_sep,
                                                         is_replace))
                            thd.setDaemon(True)
                            thd.start()
                            thds.append(thd)
                            if db in replica_info:
                                for rdb in replica_info[db]:
                                    node = replica_info[db][rdb]
                                    self.send_to_node(rdb, table, data,
                                                      node_addr[node],
                                                      row_sep, col_sep, is_replace)
                        del db_buf        
                        del ds
                        
            elif fashion == conf.REPLICATE_FASHION:
                self.replicate_data(table, files, total_size, nodes)
            elif fashion == conf.RANGE_FASHION:
                self.range_data()
            else:
                num_of_db = len(nodes) * chunk_num                
                if row_sep is not None and row_sep != "\n":
                    i = 0
                    left_ds = ""
                    while True:
                        db = sub_dbs[i % num_of_db]
                        #m = nodes[(i % num_of_db) / chunk_num].split(conf.SEP_IN_MSG) 
                        #node = m[0]
                        node = db.split("_")[-3]

                        size = string.atoi(m[3]) / chunk_num + 1
                        if size > DATA_MAX_SIZE:
                            ParaLiteLog.info("start to get data as bk: %s" % (DATA_MAX_SIZE))  
                            ds = self.get_data_as_bk(DATA_MAX_SIZE)
                        else:
                            ParaLiteLog.info("start to get data as bk: %s" % (size))
                            ds = self.get_data_as_bk(size)
                        if ds is None:
                            ParaLiteLog.info("really get data as bk: 0")
                            break
                        ParaLiteLog.info("really get data as bk: %s" % (len(ds)))
                        pos = ds.rfind(row_sep)
                        ParaLiteLog.info(pos)
                        send_ds =  left_ds + ds[0:pos]
                        left_ds = ds[pos+len(row_sep):]
                        thd = threading.Thread(target=self.send_to_node,
                                               args=(db, table, send_ds, node_addr[node],
                                                     row_sep, col_sep, is_replace))
                        thd.setDaemon(True)
                        thd.start()
                        thds.append(thd)
                        if db in replica_info:
                            for rdb in replica_info[db]:
                                node = replica_info[db][rdb]
                                thd = threading.Thread(target=self.send_to_node,
                                                       args=(rdb, table, ds, node_addr[node],
                                                             row_sep, col_sep, is_replace))
                                thd.setDaemon(True)
                                thd.start()
                                thds.append(thd)
                        i += 1
                else:
                    i = 0
                    while True:
                        db = sub_dbs[i % num_of_db]
                        #m = nodes[(i % num_of_db)/chunk_num].split(conf.SEP_IN_MSG) 
                        #node = m[0]
                        node = db.split("_")[-3]
                        size = string.atoi(m[3]) / chunk_num + 1
                        if size > DATA_MAX_SIZE:
                            ParaLiteLog.info("start to get data as bk: %s" % (DATA_MAX_SIZE))  
                            ds = self.get_data_as_bk(DATA_MAX_SIZE)
                        else:
                            ParaLiteLog.info("start to get data as bk: %s" % (size))
                            ds = self.get_data_as_bk(size)
                        if ds is None:
                            ParaLiteLog.info("really get data as bk: 0")
                            break
                        ParaLiteLog.info("really get data as bk: %s" % (len(ds)))
                        thd = threading.Thread(target=self.send_to_node,
                                               args=(db, table, ds, node_addr[node],
                                                     row_sep,col_sep,
                                                     is_replace))
                        thd.setDaemon(True)
                        thd.start()
                        thds.append(thd)
                        if db in replica_info:
                            for rdb in replica_info[db]:
                                ParaLiteLog.info(rdb)
                                node = replica_info[db][rdb]
                                thd = threading.Thread(target=self.send_to_node,
                                                       args=(rdb, table, ds, node_addr[node],
                                                             row_sep, col_sep, is_replace))
                                thd.setDaemon(True)
                                thd.start()
                                thds.append(thd)
                        i += 1
                        del ds
            for thd in thds:
                thd.join()
            ss2 = time.time()
            """
            sss = socket(AF_INET, SOCK_STREAM)
            sss.connect(master)
            msg = "%s%s%s" % (conf.REQ, conf.SEP_IN_MSG, conf.END_TAG)
            sss.send("%10s%s" % (len(msg), msg))
            """
            sep = conf.SEP_IN_MSG
            msg = "%s%s%s%s%s%s%s" % (conf.REQ, sep, conf.END_TAG, sep,
                                      gethostname(), sep, client_id)
            so_master.send("%10s%s" % (len(msg), msg))
            ParaLiteLog.info("sending to master: %s" % (conf.END_TAG))
            l = string.atoi(self.recv_bytes(so_master, 10))
            reply = self.recv_bytes(so_master, l)
            ParaLiteLog.info("receive from master: %s" % (reply))
            assert reply == conf.END_TAG
            so_master.close()
            ss3 = time.time()
        except:
            ParaLiteLog.info(traceback.format_exc())

