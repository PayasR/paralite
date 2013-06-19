import os
import sys
import cPickle
import base64
import string
import random
import traceback
import cStringIO
import sqlite3
import threading
import time
import Queue
import pwd
import logging
from socket import *

sys.path.append(os.path.abspath(os.path.join(sys.path[0], os.path.pardir)))

from lib import ioman, ioman_base
from lib.logger import ParaLiteLog
import conf

DB_SIZE_LIMITATION = 20*1024*1024*1024 # 20GB

def es(s):
    sys.stderr.write("%s%s\n" % (conf.CHILD_ERROR, s))

def ws(s):
    sys.stdout.write("%s%s\n" % (conf.CHILD_OUTPUT, s))

"""
I tested three methods for data loading: 1g
(1) executemany : 77s/116s
(2) insert      : 56s/379s
(3) .import     : 94s
so paraLite always import data into database.
"""
def unicode_or_buffer(s):
    try:
        return unicode(s)
    except UnicodeDecodeError,e: 
        return buffer(s)

class cdb:
    def __init__(self):
        self.name = None          # The name of db is complex, but the id is definitely in the last
                                  # so, the real db name = db_name + "_" + db.id
        self.size = 0
        self.table_added_record = 0
        self.table_added_size = 0
        self.status = 1

    def copy(self):
        new_db = cdb()
        new_db.name = self.name
        new_db.size = self.size
        new_db.table_added_record = self.table_added_record
        new_db.table_added_size = self.table_added_size
        new_db.status = self.status

class dload:
    def __init__(self):
        self.iom = ioman.mk_ioman()
        self.is_running = True
        self.table = None
        self.database = None
        self.db_size = 0
        self.cqid = None
        self.master_name = None
        self.master_port = 0
        self.output_dir = None
        self.queue = Queue.Queue()
        self.random_id = random.randint(1,10000)
        self.cur_db = None
        self.modified_db = {}   # [db_name : db_ins]
        self.port = 0
        self.file_reader = None
        self.files = {}
        self.flag = 0
        self.db_col_sep = None
        self.cmd_col_sep = None
        self.cmd_row_sep = None
        self.is_replace = None
        self.log_dir = None
        self.temp_dir = None
        
        self.ch_dic = {}  # {ch.id:tag}
        self.ch_list = [] # {ch}
        self.ss1 = 0
        self.threads = []

    def parse(self, argument):
        s = string.replace(argument, '*', '\n')
        b1 = base64.decodestring(s)
        dic = cPickle.loads(b1)
        for key in dic.keys():
            value = dic[key]
            if hasattr(self, key):
                setattr(self, key, value)
        if not os.path.exists(self.temp_dir): os.makedirs(self.temp_dir)

        db_ins = cdb()
        m = self.database.split("_")
        db_ins.name = self.database[0:self.database.rfind("_")]
        db_ins.id = string.atoi(m[len(m) - 1])
        db_ins.size = self.db_size
        self.cur_db = db_ins

    def register_to_master(self):
        #REG:DLOADER_SERVER:cqid:node:port:local_socket
        addr = (self.master_name, self.master_port)
        sock = socket(AF_INET, SOCK_STREAM)
        sock.connect(addr)
        sep = conf.SEP_IN_MSG
        msg = "%s%s%s%s%s%s%s%s%s%s%s" % (conf.REG, sep, conf.DLOADER_SERVER, sep, self.cqid, sep, gethostname(), sep, self.port, sep, self.local_socket)
        sock.send("%10s%s" % (len(msg), msg))
        sock.close()
        
    def notify_change_to_master(self):
        addr = (self.master_name, self.master_port)
        sock = socket(AF_INET, SOCK_STREAM)
        sock.connect(addr)
        dbs = []
        for key in self.modified_db:
            db = self.modified_db[key]
            dic = {}
            dic[conf.DATABASE] = db.name
            dic[conf.TABLE] = self.table
            dic[conf.NODE] = gethostname()
            dic[conf.ADDED_RECORD] = db.table_added_record
            dic[conf.ADDED_SIZE] = db.table_added_size
            dic[conf.STATUS] = db.status
            dic[conf.DB_SIZE] = db.size
            dbs.append(dic)
        s = cPickle.dumps(dbs)
        b = base64.encodestring(s)
        sep = conf.SEP_IN_MSG
        msg = '%s%s%s%s%s' % (conf.DBM, sep, gethostname(), sep, b)
        sock.send('%10s%s' % (len(msg), msg))
        sock.close()
        
    def next_event(self, t):
        buf = cStringIO.StringIO()
        """
        If multiple clients send data to the same address, we cannot assure the order
        of data. So we have to distinct each client here. ch_list is used to keep the
        order of channel. Here a event is returned only when ev.eof == 1 (which means
        a clint has to be closed after it issues data) and this channel is the first one.
        """
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
                return ioman_base.event_read(ch, ch.buf.getvalue(), 1, ev.err)  

    def handle_accept(self, ev):
        ch = ev.new_ch
        ch.buf = cStringIO.StringIO()
        
    def handle_read(self, ev):
        data = ev.data
        if data == conf.END_TAG:
            ParaLiteLog.info("receive: END_TAG")
            self.is_running = False
            self.queue.put(conf.END_TAG)
        elif data.startswith(conf.INFO):
            m = data.split(conf.SEP_IN_MSG)
            assert len(m) == 7
            if self.table == None: self.table = m[2]
            if self.db_col_sep == None: self.db_col_sep = m[3]
            if self.cmd_col_sep == None: self.cmd_col_sep = m[4]
            if self.cmd_row_sep == None: self.cmd_row_sep = m[5]
            if self.is_replace == None:
                self.is_replace = m[6]
                ParaLiteLog.info("DB_COL_SEP = %s CMD_COL_SEP = %s  CMD_ROW_SEP = %s is_replace = %s" % (self.db_col_sep, self.cmd_col_sep, self.cmd_row_sep, self.is_replace))
        else:
            """
            TODO: we can control the buffer size here.
            """
            self.queue.put(data)
            """
            We cannot write data to database here, because it costs time and will block many read
            event. In this case, many data comes from mulitple clients including END_TAG from master
            will be out of order.
            """
            #self.write_to_db(data, len(data))

    def scan_data_queue(self):
        while True:
            data = self.queue.get()
            if data == conf.END_TAG:
                ParaLiteLog.info("SCAN DATA QUEUE : END")
                break
            try:
                pos = 10+string.atoi(data[0:10].strip())
                target_db = data[10:pos]
                data = data[pos:]
                """
                thd = threading.Thread(target=self.write_to_db, args=(data, len(data)))
                thd.setDaemon(True)
                thd.start()
                self.threads.append(thd)
                """
                self.write_to_db(target_db, data, len(data))
                del(data)
            except Exception, e:
                ParaLiteLog.info(traceback.format_exc())
                es("in write_to_db: %s" % (traceback.format_exc()))
                sys.exit(1)
                
    def write_to_db(self, db, data, size):
        ss = time.time()
        ParaLiteLog.info("%s: START: size = %s" % (self.write_to_db.__name__, size))
        record_num = 0
        if self.is_replace == "True":
            ParaLiteLog.info("LOAD: when is_replace = True")
            con = sqlite3.connect(db)
            con.text_factory = str
            cr = con.cursor()
            if self.cmd_row_sep == "None" or self.cmd_row_sep is None:
                lines = data.split("\n")
            else: lines = data.split(self.cmd_row_sep)
            ParaLiteLog.info(len(lines))
            template = None
            for line in lines:
                if line == "": continue
                #x = tuple([ unicode_or_buffer(s.replace("\\n", "\n").replace("\\t", "\t")) for s in line.split("\t")])
                x = tuple([ s.replace("\\n", "\n").replace("\\t", "\t") for s in line.split("\t")])
                if template is None: 
                    questions = ",".join([ "?" ] * len(x))
                    template = "insert into %s values(%s);" % (self.table, questions)
                try:
                    cr.execute(template, x)
                    record_num += 1
                except sqlite3.OperationalError,e:
                    es("sqlite3.OperationalError: %s" % traceback.format_exc())
                    ParaLiteLog.info(traceback.format_exc())
                    sys.exit(1)
            ParaLiteLog.info("record_num is %s" % (record_num))
            con.commit()
            cr.close()
            con.close()
            ParaLiteLog.info("%s: FINISH" % (self.write_to_db.__name__))
            self.cur_db.table_added_record += record_num
            self.cur_db.table_added_size += size
            self.cur_db.size += size
            return
        if self.cmd_row_sep is not None and self.cmd_row_sep != "None" and self.cmd_row_sep != conf.NEW_LINE:
            ParaLiteLog.info("cmd_row_sep %s" % (self.cmd_row_sep))
            ParaLiteLog.info("db = %s" % (db))
            try:
                ParaLiteLog.info("LOAD: insert one by one")
                con = sqlite3.connect(db)
                con.text_factory = str                
                cr = con.cursor()
                lines = data.split(self.cmd_row_sep)
                template = None
                for line in lines:
                    if line == "": continue
                    x = tuple([ s for s in string.strip(line).split(self.cmd_col_sep)])
                    if template is None: 
                        questions = ",".join([ "?" ] * len(x))
                        template = "insert into %s values(%s);" % (self.table, questions)
                    cr.execute(template, x)
                    record_num += 1
            except Exception, e:
                es("in write_to_db: %s" % (traceback.format_exc()))
                ParaLiteLog.info(traceback.format_exc())
                sys.exit(1) 
            ParaLiteLog.info("record_num is %s" % (record_num))
            con.commit()
            cr.close()
            con.close()
        else:
            data = string.strip(data)
            record_num = len(data.split("\n"))
            ParaLiteLog.info("record number is : %s" % (record_num))
            temp_file = "%s%s%s_%s.dat" % (self.temp_dir, os.sep, random.randint(1, 1000), self.port)
            f = open(temp_file, "wb")
            f.write(data)
            f.close()
            ParaLiteLog.debug("DB_COL_SEP: %s" % self.db_col_sep)
            if self.db_col_sep != "|":
                ParaLiteLog.info("LOAD: execute .separator and .import")
                if self.cmd_col_sep is None or self.cmd_col_sep == "None":
                    sep_temp = self.db_col_sep
                else:
                    sep_temp = self.cmd_col_sep
                sqlf = "%s%s%s-import" % (self.log_dir, os.sep, random.randint(1,1000))
                sqlff = open(sqlf, "wb")
                sqlff.write(".separator %s\n" % (sep_temp))
                sqlff.write(".import %s %s" % (temp_file, self.table))
                sqlff.close()
                cmd = "sqlite3 %s < %s" % (db, sqlf)
                os.system(cmd)
                os.system("rm -f %s" % (sqlf))
            else:
                if self.cmd_col_sep is None or self.cmd_col_sep == "None" or self.cmd_col_sep == self.db_col_sep:
                    ParaLiteLog.info("LOAD: directly import")
                    cmd = "sqlite3 %s \".import %s %s\"" % (db, temp_file, self.table)
                    ParaLiteLog.info("CMD: %s" % (cmd))
                    os.system(cmd)
                else:
                    ParaLiteLog.info("LOAD: execute .separator and .import")
                    sqlf = "%s%s%s-import" % (self.log_dir, os.sep, random.randint(1,1000))
                    sqlff = open(sqlf, "wb")
                    sqlff.write(".separator %s\n" % (self.cmd_col_sep))
                    sqlff.write(".import %s %s" % (temp_file, self.table))
                    sqlff.close()
                    cmd = "sqlite3 %s < %s" % (db, sqlf)
                    os.system(cmd)
                    os.system("rm -f %s" % (sqlf))
            os.system("rm -f %s" % (temp_file))
        ParaLiteLog.info("%s: FINISH" % (self.write_to_db.__name__))
        # check if this is a replica of database
        if db.split("_")[-2] == "r":
            return
        if db in self.modified_db:
            dbins = self.modified_db[db]
            dbins.table_added_record += record_num
            dbins.table_added_size += size
            dbins.size += size
        else:
            dbins = cdb()
            dbins.name = db
            dbins.table_added_record = record_num
            dbins.table_added_size = size
            dbins.size = size
            self.modified_db[db] = dbins
        ee = time.time()
        
    def start(self, argument):
        self.parse(argument)
        
        cur_time = time.strftime('%Y-%m-%d-%H-%M-%S',time.localtime(time.time()))
        ParaLiteLog.init("%s/dload-server-%s-%s.log" % (self.log_dir,
                                                        gethostname(), cur_time),
                         logging.DEBUG)

        ParaLiteLog.info("START")
        ParaLiteLog.info("parse the argumens sucessfully")
        ss = time.time()
        scan_thd = threading.Thread(target=self.scan_data_queue)
        scan_thd.setDaemon(True)
        scan_thd.start()
        
        t = time.strftime('%Y-%m-%d-%H-%M-%S',time.localtime(time.time()))
        self.local_socket = "%s%s%s-%s-%s" % (self.log_dir, os.sep, gethostname(), t, "UNIX.d")
        self.iom.create_server_socket(AF_UNIX, SOCK_STREAM, 5, self.local_socket)
        ch = self.iom.create_server_socket(AF_INET, SOCK_STREAM, 5, ("", self.port))
        n, self.port = ch.ss.getsockname()
        ParaLiteLog.info("global socket addr = %s" % (repr(ch.ss.getsockname())))
        self.register_to_master()
        
        try:
            while self.is_running:
                ev = self.next_event(None)
                if isinstance(ev, ioman_base.event_accept):
                    self.handle_accept(ev)
                elif isinstance(ev, ioman_base.event_read):
                    if ev.data != "":
                        self.handle_read(ev)
                        
        except Exception, e:
            es("in dload_server.py : %s" % traceback.format_exc())
            ParaLiteLog.info(traceback.format_exc())
            sys.exit(1)
        for thd in self.threads:
            thd.join()
        if os.path.exists(self.local_socket):
            os.remove(self.local_socket)
        scan_thd.join()
        ss1 = time.time()
        ParaLiteLog.info("notify_change_to_master: START")
        #self.modified_db.append(self.cur_db)
        self.notify_change_to_master()
        ss2 = time.time()
        ParaLiteLog.info("notify_change_to_master: END")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        argument = sys.argv[1]
    dload().start(argument)

    

