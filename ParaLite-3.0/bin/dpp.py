import string
import sys
import traceback
import time
import re
import os
import Queue
import cStringIO
import random
import pwd
from socket import *

import conf

def es(s):
    sys.stderr.write(s)

def ws(s):
    sys.stdout.write(s)

class dpp:

    DATA_NODE = 'datanode'
    REG = 'REG'
    
    def __init__(self, master_name, master_port, csio, size, cqid, port, db_row_sep):
        self.id = random.randint(1, 99999)     # we use the random number as id
        self.cqid = cqid     # collective query id
        self.node_name = None
        self.data = csio
        self.socks_to_workers = {}
        self.t_size = size
        self.left_size = 0
        self.send_size = 0
        self.my_port = port
        self.master_name = master_name
        self.master_port = master_port
        self.db_row_sep = db_row_sep

    def reg_to_master(self):
        self.node_name = gethostname()
        addr = (self.master_name, self.master_port)
        sock = socket(AF_INET, SOCK_STREAM)
        sock.settimeout(300)
        sock.connect(addr)
        sock.settimeout(None)
        sep = conf.SEP_IN_MSG
        msg = '%s%s%s%s%s%s%s%s%s%s%s%s%s' % (self.REG, sep, self.DATA_NODE, sep, self.cqid, sep, self.id, sep, self.node_name, sep, self.my_port, sep, self.t_size)
        sock.send('%10s%s' % (len(msg), msg))
        sock.close()
        
    def read_as_bk(self, bksize):
        data = self.data.read(bksize)
        if not data:
            return None
        while True:
            if self.db_row_sep != "\n":
                if data.endswith(str(self.db_row_sep + "\n")):
                    break
            else:
                if data.endswith(str(self.db_row_sep)):
                    break
            extra_data = self.data.readline()
            if not extra_data:
                break
            data += extra_data 
        return data

    def recv_bytes(self, so, n):
        A = []
        while n > 0:
            x = so.recv(n)
            if x == "": break
            A.append(x)
            n = n - len(x)
        return string.join(A, "")

    def send_data(self, node, data):
        so = self.socks_to_workers[node]
        try:
            s = time.time()*1000
            so.send("%10s%s" % (len(data), data))
            e = time.time()*1000
            #print '%s --> %s data=%d time=%d ms' % (gethostname(), node, len(data), (e-s))
        except:
            traceback.print_exc()    

    def close_socks_to_worker(self):
        for key in self.socks_to_workers.keys():
            so = self.socks_to_workers[key]
            if so:
                so.close()
    
    # waiting for cmd from master. 
    # cmd:    worker_id:worker_name:worker_port:block_size
    def wait_for_cmd(self, listener):
        self.data.seek(0)
        while True:
            sock, addr = listener.accept()
            try:
                length = self.recv_bytes(sock, 10)
                if length == "":
                    continue
                cmd = self.recv_bytes(sock, string.atoi(length))
                if cmd == conf.END_TAG:
                    break
                # m = worker.id:(node:port | addr):size
                m = cmd.split(conf.SEP_IN_MSG)
                if len(m) == 4:
                    w_id = m[0]
                    addr = (m[1], string.atoi(m[2]))
                    t = AF_INET
                    bk = string.atoi(m[3])
                elif len(m) == 3:
                    w_id = m[0]
                    addr = m[1]
                    t = AF_UNIX
                    bk = string.atoi(m[2])
                if self.socks_to_workers.has_key(w_id) != 1:
                    so = socket(t, SOCK_STREAM)
                    so.connect(addr)
                    self.socks_to_workers[w_id] = so
                    so.send('%10sDATA' % (4))
                else:
                    so = self.socks_to_workers[w_id]
                data = self.read_as_bk(bk)
                if not data:
                    # if we don't send something here, udx will not send KAL again, and then
                    # they will not receive data again, the whole process will be blocked for ever
                    so.send('%10sEMPTY' % (5))
                    continue
                s = time.time()*1000
                l = len(data)
                so.send('%10s%s' % (l, data))
                reply = self.recv_bytes(so, string.atoi(self.recv_bytes(so, 10)))
                assert reply == conf.END_TAG
                e = time.time()*1000
            except Exception, e:
                Es("wait_for_cmd in dpp.py raised Exception: %s\n" % (traceback.format_exc()))
            finally:
                if sock:
                    sock.close()    
        listener.close()

    def start(self):
        try:
            listener = socket(AF_INET, SOCK_STREAM)
            addr = ("", self.my_port)
            listener.bind(addr)
            listener.listen(200)
            n, self.my_port = listener.getsockname()
            self.reg_to_master()
            self.wait_for_cmd(listener)
            self.close_socks_to_worker()
        except KeyboardInterrupt, e:
            sock = socket(AF_INET, SOCK_STREAM)
            sock.connect(("", self.my_port))
            msg = conf.END_TAG
            sock.send("%10s%s" % (len(msg), msg))
            sock.close()
        except Exception, e:
            raise(Exception(traceback.format_exc()))

if __name__ == "__main__":
    dpp = dpp()
    dpp.start()

