import Queue
import threading
import time
import string
import sys
import traceback
from socket import *

class data_unit:
    def __init__(self):
        self.id = None
        self.data = None

class cqueue:
    def __init__(self, host, port):
        self.host = host
        if isinstance(port, basestring): self.port = string.atoi(port)
        else: self.port = port
        self.so = None
        self.in_queue = []
        self.out_queue = []
        self.workerqueue = []
        self.s_time = 0
        self.e_time = 0
        
    def listen(self):
        _so = socket(AF_INET, SOCK_STREAM)
        addr = (self.host, self.port)
        try:
            _so.bind(addr)
            _so.listen(5)
            self.so = _so
        except error, msg:
            if not _so:
                _so.close()

    def get(self, num_of_conn):
        try:
            _so = self.so
            count = 0
            mutex = threading.Lock()
            while True:
                """
                bug: if some nodes fail, this loop will not be terminated for ever.
                """
                client, conn = _so.accept()
                if self.s_time == 0:
                    self.s_time = time.time()*1000
                worker = threading.Thread(target=self.put_data_to_queue, args=(client,))
                worker.start()
                self.workerqueue.append(worker)
                mutex.acquire()
                count += 1
                mutex.release()
                if count == num_of_conn:
                    break
            for thread in self.workerqueue:
                thread.join()
            self.e_time = time.time()*1000
            return self.in_queue
        except Exception, e:
            traceback.print_exc()
                
    def put_data_to_queue(self, client):
        """
        the first line of message is identification 
        """
        data = data_unit()
        str_of_len = self.recv_bytes(client, 10)
        if str_of_len == "":
            return
        data.id = self.recv_bytes(client,string.atoi(str_of_len))
        str_of_len = self.recv_bytes(client, 10)
        if str_of_len == "":
            return
        data.data = self.recv_bytes(client,string.atoi(str_of_len))
        self.in_queue.append(data)
        
    def recv_bytes(self, so, n):
        A = []
        while n > 0:
            x = so.recv(n)
            if x == "": break
            A.append(x)
            n = n - len(x)
        return string.join(A, "")

    def connect(self):
        addr = (self.host, self.port)
        _so = socket(AF_INET, SOCK_STREAM)
        try:
            _so.connect(addr)
            self.so = _so
        except error, msg:
            if self.so:
                self.so.close()
            raise Exception("connection refused")
        
    def put(self, data):
        try:
            msg = '%10s%s' % (str(len(data)), data)
            self.so.send(msg)
        except error, msg:
            print self.host
            print self.port
            
    def close(self):
        if not self.so:
            print "close... %s %s" % (self.host, self.port)
            self.so.close()
    
def Ws(s):
    sys.stdout.write(s)

def Es(s):
    sys.stderr.write(s)
    
                        
            
