"""ParaLite client which is responsible for receiving user's query
and starting the master.
@version: 2.0
@author: Ting Chen
"""
# import modules from python standard library
import time
import random
import os
import sqlite3
import sys
import shlex
import cPickle
import base64
import string
import traceback
import cStringIO
import Queue
from socket import *
import pwd
import logging

# set the home direcotry of paraLite into system's path
sys.path.append(os.path.abspath(os.path.join(sys.path[0], os.path.pardir)))

# import modules from ParaLite
from lib import ioman, ioman_base
from lib.logger import ParaLiteLog
import dload_client
import conf


class ImportCmd:
    """description of the data loading .import command 
    """
    def __init__(self):
        self.table = None
        self.file_path = None
        self.fashion = None
        self.key = None
        self.col_sep = None
        self.row_sep = None
        self.files = []
        self.key_pos = []
        self.record_tag = None
        self.replace = False

class XmlToTxt:
    """conversion of xml file to text data
    """
    def __init__(self):
        self.record_tag = None
        self.columns = []
        self.columns_path = {}

    def get_columns_value(self, t, columns_path, columns_value):
        for tag in columns_path:
            columns_value[columns_path[tag]] = []
        self.get_tag_value(t, columns_path, columns_value, ())
                                    
    def get_tag_value(self, t, columns_path, columns_value, ancestor_tags):
        if hasattr(t, "tagName"):
            all_tag = ancestor_tags + (t.tagName, )
            if all_tag in columns_path:
                columns_value[columns_path[all_tag]].append(t.childNodes[0].data)
            for c in t.childNodes:
                self.get_tag_value(c, columns_path, columns_value, all_tag)

    def find_columns_path(self, t, column):
        column_dic = {}
        for col in column:
            l = col.split("_")
            tag = l[len(l) - 1]
            if tag not in column_dic:
                column_dic[tag] = []
            column_dic[tag].append(tuple(l))
        self.get_tag_path(t, self.columns_path, column_dic, ())
                
    def get_tag_path(self, t, columns_path, columns_dic, ancestor_tags):
        if hasattr(t, "tagName"):
            if len(columns_path) == len(self.columns):
                return True
            all_tag = ancestor_tags + (t.tagName, )
            if t.tagName in columns_dic:
                for tup in columns_dic[t.tagName]:
                    l = len(tup)
                    if all_tag[-l:] == tup:
                        columns_path[all_tag] = "_".join(tup)
            for c in t.childNodes:
                if self.get_tag_path(c, columns_path, columns_dic, all_tag):
                    return True
        return False

    def parse_sub_file(self, file_str, new_file):
        xmlDoc = minidom.parseString(file_str)
        xmlRoot = xmlDoc.documentElement
        if len(self.columns_path) != len(self.columns):
             un_found = []
             for col in self.columns:
                 if col not in self.columns_path:
                     un_found.append(col)
             self.find_columns_path(xmlRoot, un_found)
        columns_value = {}
        self.get_columns_value(xmlRoot, self.columns_path, columns_value)
        max_num = 1
        for col in columns_value:
            l =len(columns_value[col])
            if l > max_num:
                max_num = l
        i = 0
        while i < max_num:
            record = ""
            for j in range(len(self.columns)):
                col = self.columns[j]
                if col not in columns_value:
                    v = ""
                else:
                    l = len(columns_value[col]) 
                    if l > i:
                        v = columns_value[col][i]
                    else:
                        if l == 0:
                            v = ""
                        else:
                            v = columns_value[col][0]
                if j == len(self.columns) - 1:
                    record += v
                else:
                    record += v + self.sep
            new_file.write(record.encode("utf-8") + "\n")
            i += 1
            
    def read_source(self, sfile, nfile):
        f = open(sfile, "rb")
        flag = 0
        buf = cStringIO.StringIO()
        while True:
            line = string.strip(f.readline())
            if not line:
                break
            if line.startswith("<%s " % (self.record_tag)):
                flag = 1
            if string.strip(line) == "</%s>" % (self.record_tag):
                flag = 0
                buf.write(line + "\n")
                buf.seek(0)
                record = self.parse_sub_file(buf.read(), nfile)
                buf.close()
                buf = cStringIO.StringIO()
            if flag == 1:
                buf.write(line + "\n")

    def parse(self, f_name, columns, record_tag, sep, new_file_name):
        new_file1 = open(new_file_name, "wb")
        new_file = cStringIO.StringIO()
        self.columns = columns
        self.record_tag = record_tag
        self.sep = sep
        self.read_source(f_name, new_file)
        new_file1.write(new_file.getvalue())
        new_file.close()
        new_file1.close()

class ParaLite:
    def __init__(self):
        self.args = []  # [query_type, database, query, configinfo]
        self.hub = (0, None)  # (0--> db, None --> db://path/to/db)
        self.cqid = None
        self.block_size = 0
        self.database = None
        self.ctquery = None
        self.my_port = 0  # the local socket port
        self.output = None
        self.buf = cStringIO.StringIO()
        self.err = None
        self.exit = 0
        self.node = None
        self.master_server = None
        self.master_port = 0
        self.iom = ioman.mk_ioman()
        self.is_running = True
        self.is_master_running = True
        self.is_local_server_running = True
        self.defaultconf = self.init_default_configs()
        self.db_row_sep = None
        self.db_col_sep = None
        
        #time_str = time.strftime('%Y-%m-%d-%H-%M-%S',time.localtime(time.time()))

        self.PROCESS_OUT = 0
        self.SOCKET_OUT = 1

    def init_default_configs(self):
        dic = {}
        #dic[conf.TEMP_DIR] = "/data/local/.paralite-tmp" 
        dic[conf.TEMP_DIR] = "/tmp"
        dic[conf.LOG_DIR] = "/home/%s/.paralite-log" % (pwd.getpwuid(os.getuid())[0])
        dic[conf.BLOCK_SIZE] = 0
        return dic

    def read_config_file(self, cf):
        f = open(cf, "rb")
        while True:
            line = f.readline().strip()
            if not line:
                break
            m = line.split()
            self.defaultconf[m[0]] = m[1]
            
    
    def get_db_info(self, cmd_type, database, table, node, port):
        try:
            if gethostname() == node:
                # the client is on the same node with the master, then we directly query the metadata db
                con = sqlite3.connect(database)
                cr = con.cursor()
                if cmd_type == "TABLE_COLUMN":
                    rs = cr.execute('select attribute from table_attr_info where name = "%s"' % (table)).fetchall()
                    if rs:
                        re = []
                        for row in rs:
                            re.append(row[0])
                        return re
                elif cmd_type == "SEPARATOR":
                    rs = cr.execute('select row_separator, col_separator from setting_info').fetchone()
                    if rs:
                        return conf.SEP_IN_MSG.join(rs)
                elif cmd_type == "PARTITION_KEY":
                    rs = cr.execute('select partition_key from table_partition_info where name = "%s"' % (table)).fetchone()
                    return rs[0]
                return None
            else:
                # send query to the master
                addr = (node, port)
                sock = socket(AF_INET, SOCK_STREAM)
                sock.connect(addr)
                msg = "%s%s%s%s%s%s%s" % (conf.QUE, conf.SEP_IN_MSG, cmd_type,
                                          conf.SEP_IN_MSG,database, conf.SEP_IN_MSG,
                                          table)
                sock.send("%10s%s" % (len(msg), msg))
                data = recv_bytes(sock, string.atoi(recv_bytes(sock, 10)))
                sock.close()
                return data
        except Exception, e:
             ParaLiteLog.error(traceback.format_exc())

    def parse_import_cmd(self, database, cmd, node, port):
        opt = ImportCmd()
        tag = 0
        table_columns = []
        m = cmd.split()
        opt.file_path = os.path.abspath(m[1])
        opt.table = m[2]
        is_ex = 0
        hash_key = self.get_db_info("PARTITION_KEY", database, opt.table, node, port)
        if hash_key is not None and hash_key != '':
            opt.fashion = conf.HASH_FASHION
            opt.key = hash_key
            if table_columns == []:
                table_columns = self.get_db_info("TABLE_COLUMN", database,
                                                 opt.table, node, port)
            for hk in hash_key.split(","):
                opt.key_pos.append(table_columns.index(hk))

            if len(m) == 4:
                opt.record_tag = m[3]
        else:
            # data is partitioned based on round_robin-fashion
            opt.fashion = conf.ROUND_ROBIN_FASHION
            if len(m) < 3:
                is_ex = 1
            elif len(m) == 4:
                opt.record_tag = m[3]
        if cmd.find("-column_separator") != -1:
            index = m.index("-column_separator")
            if len(m) < index + 2:
                is_ex = 1
            else:
                opt.col_sep = m[index + 1]
        if cmd.find("-row_separator") != -1:
            index = m.index("-row_separator")
            if len(m) < index + 2:
                is_ex = 1
            else:
                opt.row_sep = m[index + 1]
        if cmd.find("-replace") != -1:
            opt.replace = True
        if is_ex:
            usage = """Usage: .import file|file_dir table [record_tag]
                               [-column_separator col_sep][-row_separator row_sep]"""
            Es(usage)
            return
        return opt

    def parse_xml_file(self, f_name, table_columns, record_tag, sep, new_file_name):
        xml2txt().parse(f_name, table_columns, record_tag, sep, new_file_name)
        
    def load(self, master_node, master_port, database, ctquery):
        node = master_node
        port = master_port
        opt = self.parse_import_cmd(database, ctquery, node, port)
        files = []
        sep = self.get_db_info("SEPARATOR", database, None, node, port)
        col_sep = sep.split(conf.SEP_IN_MSG)[1]
        procs = []
        f = opt.file_path
        if os.path.isdir(f):
            if not f.endswith("/"):
                f += "/"
            for fi in os.listdir(f):
                f_name = f+fi
                if f_name.endswith(".xml"):
                    if table_columns == []:
                        table_columns = self.get_db_info("TABLE_COLUMN",
                                                         database, table, node, port)
                    new_file = f_name.replace(".xml", ".dat")
                    proc = Process(target=self.parse_xml_file,
                                   args=(f_name,table_columns, record_tag,
                                         col_sep, new_file))
                    procs.start()
                    files.append(new_file)
                    procs.append(proc)
                else:
                    files.append(f_name)
        else:
            if f.endswith(".xml"):
                table_columns = self.get_db_info("TABLE_COLUMN", database,
                                                 table, node, port)
                new_file = xml2txt().parse(f, table_columns, record_tag, col_sep)
                files.append(new_file)
            else:
                files.append(f)
        opt.files = files
        for proc in procs:
            proc.join()
        my_node = gethostname()
        master = (node, port)
        self.start_load(master, "", my_node, database, 0, col_sep, opt)
        
    def start_load(self, master, cqid, node, database, tag, db_col_sep, opt):
        ParaLiteLog.info("start to load")
        table = opt.table
        files = opt.files
        fashion = opt.fashion
        key = opt.key
        key_pos = opt.key_pos
        row_sep = opt.row_sep
        col_sep = opt.col_sep
        ParaLiteLog.debug("============DATA LOADING CLIENT=============")
        dload_client.dload_client().load_internal(master, cqid, node, database, table,
                                                  files, 0, fashion, key, key_pos,
                                                  db_col_sep, row_sep, col_sep,
                                                  opt.replace, "0",
                                                  self.defaultconf[conf.LOG_DIR])
        ParaLiteLog.debug("============DATA LOADING CLIENT=============")        
        ParaLiteLog.info("%s: FINISH" % (self.start_load.__name__))
        
    def parse(self, argv):
        if len(argv) == 2 and argv[1] == "help":
            self.args = ["help"]
            return
        
        database = os.path.abspath(argv[1])
        self.database = database
        ctquery = argv[2]
        i = 3
        
        # by default, using a db as a hub
        self.hub = (0, database)
        while i < len(argv):
            arg = argv[i]
            i += 1
            value = argv[i]
            if arg == '--hub':
                if value.startswith("db"):
                    self.hub = (0, value.split("://")[1])
                elif value.startswith("file"):
                    self.hub = (1, value.split("://")[1])
                elif value.startswith("host"):
                    self.hub = (2, value.split("://")[1])
                else:
                    self.args = ["help"]
                    return
            elif arg == '-f':
                self.hub = (1, value)
            elif arg == '-p':
                self.hub = (2, value)
            elif arg == '-b' or arg == '-block':
                if value.lower() == "all":
                    self.defaultconf[conf.BLOCK_SIZE] = -1
                else:
                    self.defaultconf[conf.BLOCK_SIZE] = string.atoi(value)
            elif arg == "-configure":
                self.read_config_file(value)
            elif arg == "-log":
                self.defaultconf[conf.LOG_DIR] = value
            elif arg == "-temp":
                self.defaultconf[conf.TEMP_DIR] = value
            elif arg == "-port_range":
                self.defaultconf[conf.PORT_RANGE] = value
            else:
                self.args = ["help"]
                return
            i += 1


        self.ctquery = ctquery
        s = ["select", "create", "insert", "drop", "delete", "update"]
        if ctquery.split()[0].lower() in s:
           self.args = ["sql", database, ctquery]
        elif ctquery.startswith("."):
            self.args = ["special", database, ctquery]
        elif ctquery.lower().startswith("pragma"):
            self.args = ["pragma", database, ctquery]
        else:
            Es("ERROR: the syntax of your query is wrong, please check!\n")

        if ctquery.find("collective by") != -1:
            m = string.strip(ctquery).split()
            cqid = m[len(m) - 1]
        else:
            cqid = str(0-random.randint(1,1000))
        self.cqid = cqid

    def do_special_cmd(self, argv):
        confinfo = argv[len(argv) - 1]
        """
        If the option PORT_RANGE is specified, we have to select a port number
        within the range for the client. To avoid to parse the PORT_RANGE 
        (10000-19999), we choose either the first one or the last one.
        """
        if conf.PORT_RANGE in confinfo:
            m_p = 0
            if self.hub[0] == 2:
                m_p = string.atoi(self.hub[1].split(":")[1])
            s = string.atoi(confinfo[conf.PORT_RANGE].split("-")[0])
            e = string.atoi(confinfo[conf.PORT_RANGE].split("-")[1])
            if s != m_p:
                self.my_port = s
            elif e != m_p:
                self.my_port = e
        s = cPickle.dumps(confinfo)
        b = base64.encodestring(s)
        str_confinfo = string.replace(b, '\n', '*') # delete new line

        # at this point, we only allowed one client to issue the special query
        # start local socket server to listen all connections
        ch = self.iom.create_server_socket(AF_INET, SOCK_STREAM, 5, ("", self.my_port))
        n, self.my_port = ch.ss.getsockname()
        # start the master
        path = sys.path[0]
        if self.hub[0] == 2:
            self.master_node = self.hub[1].split(":")[0]
            self.master_port = string.atoi(self.hub[1].split(":")[1])
            cmd = "python %s/m_paraLite.py %s %s %s %s %s" % (path, gethostname(),
                                                              self.my_port,
                                                              self.database,
                                                              self.master_port,
                                                              str_confinfo)
        else:
            self.master_node = gethostname()
            cmd = "python %s/m_paraLite.py %s %s %s %s %s" % (path, self.master_node,
                                                              self.my_port,
                                                              self.database, 0,
                                                              str_confinfo)
            
        if gethostname() != self.master_node:
            #cmd = 'gxpc e -h %s %s' % (self.master_node, cmd)
            cmd = 'ssh %s %s' % (self.master_node, cmd)
            ParaLiteLog.debug(cmd)
        pipes = [ (None, "w", 0), (None, "r", 1), (None, "r", 2) ]
        pid, r_chans, w_chans, err = self.iom.create_process(cmd.split(), None,
                                                             None, pipes, None)
        if err:
            Es("Create the master process failed: %s\n" % (err))
            sys.exit(1)
        for ch in r_chans:
            ch.flag = self.PROCESS_OUT
            ch.buf = cStringIO.StringIO()
        if self.ctquery.startswith(".import"):
            self.wait("import", 1)
            return
        self.wait("special", 1)
        #self.dump(self.buf.getvalue(), 1)
        
    def do_pragma_cmd(self, argv):
        print 1

    def do_sql_cmd(self, argv):
        ParaLiteLog.debug("start to process the query")
        confinfo = argv[len(argv) - 1]
        """
        If the option PORT_RANGE is specified, we have to select a port number
        within the range for the client. To avoid to parse the PORT_RANGE 
        (10000-19999), we choose either the first one or the last one.
        """
        if conf.PORT_RANGE in confinfo:
            m_p = 0
            if self.hub[0] == 2:
                m_p = string.atoi(self.hub[1].split(":")[1])
            s = string.atoi(confinfo[conf.PORT_RANGE].split("-")[0])
            e = string.atoi(confinfo[conf.PORT_RANGE].split("-")[1])
            if s != m_p:
                self.my_port = s
            elif e != m_p:
                self.my_port = e
        s = cPickle.dumps(confinfo)
        b = base64.encodestring(s)
        str_confinfo = string.replace(b, '\n', '*') # delete new line
        
        # start socket server to listen all connections
        ch = self.iom.create_server_socket(AF_INET, SOCK_STREAM, 100, ("", self.my_port)) 
        n, self.my_port = ch.ss.getsockname()
        ParaLiteLog.debug("client listen to %s ..." % (self.my_port))

        path = sys.path[0]
        # get the token for starting the master
        if self.hub[0] == 2:
            # if the hub info is specified in the cmd line,
            # every client first try to start the master process,
            # but only the one who did not receive 98 error info can succuss
            token = 1
            self.master_node = self.hub[1].split(":")[0]
            self.master_port = string.atoi(self.hub[1].split(":")[1])
            cmd = "python %s/m_paraLite.py %s %s %s %s %s" % (path, gethostname(),
                                                              self.my_port,
                                                              self.database,
                                                              self.master_port,
                                                              str_confinfo)
            
        else:
            token = self.get_token(self.hub)
            if token == -1:
                return
            self.master_node = gethostname()
            cmd = "python %s/m_paraLite.py %s %s %s %s %s" % (path, self.master_node,
                                                              self.my_port,
                                                              self.database, 0,
                                                              str_confinfo)
        # start the master
        if token == 1:

            if gethostname() != self.master_node:
                #cmd = 'gxpc e -h %s %s' % (self.master_node, cmd)
                cmd = 'ssh %s %s' % (self.master_node, cmd)
            ParaLiteLog.debug(cmd)
            pipes = [ (None, "w", 0), (None, "r", 1), (None, "r", 2) ]
            pid, r_chans, w_chans, err = self.iom.create_process(cmd.split(), None,
                                                                 None, pipes, None)
            if err:
                Es("Create the master process failed: %s\n" % (err))
                sys.exit(1)
            for ch in r_chans:
                ch.flag = self.PROCESS_OUT
                ch.buf = cStringIO.StringIO()
        else:
            self.is_master_running = False
            self.master_node, self.master_port = self.get_master_info()
            self.register_to_master(self.master_node, self.master_port,
                                    self.database, self.ctquery)
            ParaLiteLog.info("register_to_master: FINISH")

        # wait the local server and the master to be finished
        self.wait("sql", token)
        ParaLiteLog.info("wait ends")        
        # anyway dump the buffered data
        # self.dump(string.strip(self.buf.getvalue()), 1)

    def dump(self, data, is_end):
        if self.output is not None and len(data) > 0:
            if self.output == "stdout" or self.ctquery.startswith("."):
                sys.stdout.write("%s\n" % data.strip())
            else:
                f = open(self.output, "a")
                f.write("%s\n" % data.strip())
                if is_end == 1:
                    f.write("\n")
                f.close()
        
    def handle_accept(self, event):
        ch = event.new_ch
        ch.flag = self.SOCKET_OUT
        ch.buf = cStringIO.StringIO()
        ch.length = 0

    def handle_death(self, event):
        ParaLiteLog.debug("receive a death event")
        pid = event.pid
        status = event.status
        err = event.err
        self.is_master_running = False
        if not self.is_local_server_running and not self.is_master_running:
            self.is_running = False
        if status == 1:
            sys.stderr.write("The master process is failed on %s: %s\n" % (self.master_node, err))
        ParaLiteLog.debug("END <-- The master process ")
        
    def handle_read(self, event, tag):
        flag = event.ch.flag
        if flag == self.PROCESS_OUT:
            ParaLiteLog.debug("handle_read from master")            
            self.handle_read_from_master(event)
        elif flag == self.SOCKET_OUT:
            ParaLiteLog.debug("handle_read from socket")            
            self.handle_read_from_socket(event, tag)

    def handle_read_from_master(self, event):
        if string.strip(event.data) == "98" or event.data.startswith("ssh_exchange_identification"):
            ParaLiteLog.debug("receive error %s : it should not start the master" % (event.data))
            # If multiple clients start the master at the same,
            # only one will success and others will receive a error "98"
            self.is_master_running = False
            self.register_to_master(self.master_node, self.master_port,
                                    self.database, self.ctquery)
            ParaLiteLog.info("register_to_master %s:%s : FINISH" % (self.master_node,
                                                           self.master_port))
        elif event.data.startswith("ERROR"):
            Es(event.data)
            self.is_running = False
        else:
            #Es(event.data)
            self.is_master_running = False
        
    def handle_read_from_socket(self, event, tag):
        message = event.data[10:]
        if message == "OK":
            self.is_local_server_running = False
            ParaLiteLog.debug("closed the local server")
            if not self.is_local_server_running and not self.is_master_running:
                self.is_running = False
        elif message.startswith(conf.INFO):
            # INFO:port
            m = message.split(conf.SEP_IN_MSG)
            assert len(m) == 2
            if m[1] != conf.MASTER_READY:
                self.master_port = string.atoi(m[1])
            if tag == "sql":
                self.set_master_info(self.master_port)
                ParaLiteLog.debug("set_master_info: FINISH")
                self.register_to_master(self.master_node, self.master_port,
                                        self.database, self.ctquery)
                ParaLiteLog.info("register_to_master %s:%s : FINISH" % (self.master_node,
                                                               self.master_port))
            elif tag == "import":
                self.is_local_server_running = False
                self.load(self.master_node, self.master_port,
                          self.database, self.ctquery)
            elif tag == "special":
                self.register_to_master(self.master_node, self.master_port,
                                        self.database, self.ctquery)
        elif message.startswith(conf.DB_INFO):
            # DB_INFO:output:row_sep:col_sep
            m = message.split(conf.SEP_IN_MSG)
            assert len(m) == 4
            self.output = m[1]
            if self.output != "stdout" and not self.ctquery.startswith("."):
                if os.path.exists(self.output): os.remove(self.output)
            self.db_row_sep = m[2]
            self.db_col_sep = m[3]
        elif message.startswith("ERROR"):
            Es(message + "\n")
            self.is_running = False
        else:
            self.buf.write(message.strip() + self.db_row_sep)
            """
            if len(self.buf.getvalue()) > 1024*1024*1024:
                self.buf.seek(0)
                self.dump(self.buf.read(), 0)
            """
                    
    def safe_kill_master(self, node, port):
        ParaLiteLog.debug("start to kill the master process safely")
        # send a KILL singal to the master and wait it to terminate
        sock = socket(AF_INET, SOCK_STREAM)
        sock.connect((node, port))
        sock.send("%10s%s" % (len(conf.KILL), conf.KILL))
        sock.close()
        
    def set_master_info(self, master_port):
        if self.hub[0] == 0:
            # multiple group of query may share a database
            con = sqlite3.connect(self.hub[1])
            con.execute("update hub_info set port = %s where cqid='%s'" % (master_port, self.cqid))
            con.commit()
            con.close()
        elif self.hub[0] == 1:
            # but each group of query has a separate shared file
            fd = open(self.hub[1], "a")
            fd.write(" %s" % (master_port))
            fd.close()

    def get_master_info(self):
        node = None
        port = 0
        if self.hub[0] == 0:
            try:
                con = sqlite3.connect(self.hub[1])
                while True:
                    sql = "select node, port from hub_info where cqid='%s'" % (self.cqid)
                    rs = con.execute(sql).fetchone()
                    if rs is None:
                        Es("Error: Database %s does not have the information of query %s, you may delete it by mistake, please check it!" % (self.hub[1], self.cqid))
                        break
                    else:
                        port = rs[1]
                        if port != 0:
                            node = rs[0]
                            break
                    if self.exit == 1:
                        break
                    time.sleep(0.1)
                con.close()
            except Exception, e:
                print e.args
        elif self.hub[0] == 1:
            try:
                f = open(self.hub[1], "rb")
                while True:
                    f.seek(0)
                    line = string.strip(f.readline()).split()
                    if len(line) == 3:
                        node = line[1]
                        port = string.atoi(line[2])
                        break
                    if self.exit == 1:
                        break
                    time.sleep(0.1)
                f.close()
            except Exception, e:
                print e.args
        else:
            while True:
                if self.port != 0:
                    break
                if self.exit == 1:
                    break
                time.sleep(0.1)
            node = self.hub[1].split(":")[0]
            port = string.atoi(self.hub[1].split(":")[1])
        return node, port
        
    def wait(self, action, token):
        try:
            while self.is_running:
                ev = self.next_event(None)
                if isinstance(ev, ioman_base.event_accept):
                    self.handle_accept(ev)
                elif isinstance(ev, ioman_base.event_read):
                    if ev.data != "":
                        self.handle_read(ev, action)
                elif isinstance(ev, ioman_base.event_death):
                    self.handle_death(ev)
            
        except KeyboardInterrupt, e:
            Es("ParaLite receives a interrupt signal and then will close the process\n")
            if self.is_master_running:
                self.safe_kill_master(self.master_node, self.master_port)
            #sys.exit(1)
        finally:
            if token == 1:
                if action != "special" and action != "import":
                    self.clear_hub_info(self.cqid)
            
    def clear_hub_info(self, cqid):
        ParaLiteLog.debug("clear hub info")
        if self.hub[0] == 0:
            con = sqlite3.connect(self.hub[1])
            con.execute('delete from hub_info where cqid = "%s"' % (cqid))
            con.commit()
            con.close()
        elif self.hub[0] == 1:
            os.remove(self.hub[1])

    def do_help_cmd(self, args):
        u = """Usage: paralite /path/to/db STATEMENT [--hub]
--hub:
       \tfile://path/to/file
       \tdb://path/to/file 
       \thost://hostname:port
       
Examples:

   paralite /path/to/db \"QUERY\"\tissue a query --hub file:/home/user/hub_info
   paralite /path/to/db < query.sql
   Paralite /path/to/db \"SPECIAL COMMAND\" 
       \t.import FILE/DIR TABLE [record_tag][-hash key][-replicate] [-column_separator col_sep -row_separator row_sep]\tImport data from FILE into TABLE
       \t.output FILE\tSend output to FILENAME
       \t.output stdout\tSend output to the screen
       \t.indices [TABLE]\tShow names of all indices. If TABLE specified, only show indices for tables
       \t.row_separator STRING\tChange separator used by output mode and .import
       \t.col_separator STRING\tChange separator used by output mode and .import
       \t.show\tShow the current values for various settings
       \t.analyze \"QUERY\"
"""
        print u

    def register_to_master(self, node, port, database, ctquery):
        sep = conf.SEP_IN_MSG
        msg = '%s%s%s%s%s%s%s%s%s%s%s' % (conf.REG,sep, conf.CLIENT, sep, database,sep,
                                          ctquery,sep, gethostname(), sep, self.my_port)
        addr = (node, port)
        sock = socket(AF_INET, SOCK_STREAM)
        try:
            sock.connect(addr)
        except Exception, e:
            ParaLiteLog.error("Error in register_to_master: %s" % (" ".join(str(s) for s in e.args)))
            print "Error in register_to_master: %s" % (" ".join(str(s) for s in e.args))
            if e.errno == 4:
                sock.connect(addr)
                
        sock.send('%10s%s' % (len(msg), msg))
        sock.close()
            
    def get_token(self, hub):
        token = 0
        hub_type = hub[0]
        if hub_type == 2:
            # using process as a hub :  --hub host:hostname:port
            node = hub[1].split(":")[0]
            port = string.atoi(hub[1].split(":")[1])
            addr = (node, port)
            detect_so = socket(AF_INET, SOCK_STREAM)
            try:
                detect_so.connect(addr)
                detect_so.send("%10s%s" % (len(conf.DETECT_INFO), conf.DETECT_INFO))
                detect_so.close()
                return 0, None
            except Exception, e:
                if e.args[0] == 111:
                    """
                    If the server is not started, we have to start the master
                    """
                    master_server = self.start_master_server()
                    time.sleep(0.5)  # wait for the result coming back
                    if self.err is None:
                        return 1, master_server
                    elif string.strip(self.err) == "98":
                        master_server.join()
                        return 0, None
                    else:
                        return -1, None
        elif hub_type == 1:
            # using shared file as a hub: --hub file://path/to/file
            f = hub[1]
            if os.path.exists(f):
                # this means that the metadata db is not shared, so this file is used
                # to specify the node who hold the metadata db
                return 0
            else:
                # this means that the metadata db is shared, so we can start the master
                # anywhere and this file is just usded as an alternate of metadata db
                try:                    
                    fd = os.open(f, os.O_WRONLY | os.O_CREAT | os.O_EXCL)
                    node = gethostname()
                    os.write(fd, "%s %s" % (self.cqid, node))
                    os.close(fd)
                    return 1
                except OSError, e:
                    if e.args[0] == 17:
                        return 0
                    else:
                        return -1
        elif hub_type == 0:
            # using metadata db as a hub: --hub db://path/to/db
            try:
                con = sqlite3.connect(hub[1])
                cr = con.cursor()
                rs = cr.execute("select * from sqlite_master where type='table' and name='hub_info'").fetchone()
                if rs is None or len(rs) == 0:
                    cr.execute("create table hub_info(cqid varchar(10) primary key, node varchar(50), port int)")
                node_rs = cr.execute("select node, port from hub_info where cqid ='%s'" % (self.cqid)).fetchone()
                if node_rs is not None and len(node_rs) == 1:
                    node = node_rs[0]
                    port = node_rs[1]
                else:
                    node = gethostname()
                cr.execute('insert into hub_info values ("%s", "%s", 0)' % (self.cqid,
                                                                            node))
                con.commit()
                con.close()
                return 1
            except sqlite3.IntegrityError, ex1:
                return 0
            except sqlite3.OperationalError, ex2:
                return 0
            except Exception, e:
                print e.args
                return -1

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
            if ev.data is not None:
                ch.buf.write(ev.data)
            # an I/O error or EOF. we return anyway
            if ev.eof:
                data_to_return = ch.buf.getvalue()
                return ioman_base.event_read(ch, data_to_return, 1, ev.err)
            elif ev.ch.flag == self.SOCKET_OUT:
                # the first 10 bytes is the length of the mssage
                if ch.length == 0:
                    ch.length = string.atoi(ev.ch.buf.getvalue()[0:10])
                if len(ch.buf.getvalue()) >= ch.length + 10:
                    all_data = ch.buf.getvalue()
                    data_to_return = all_data[0:ch.length+10]
                    ch.buf.truncate(0)
                    ch.buf.write(all_data[ch.length+10:])
                    ch.length = 0
                    ev.ch.so.send("OK")
                    return ioman_base.event_read(ch, data_to_return, 0, ev.err)

    def execute(self, argv):
        c = "do_%s_cmd" % (argv[0])
        if hasattr(self, c):
            method = getattr(self, c)
            method(argv[1:])
            
    def main(self, argv, flag):
        # flag = 1: invoked from program
        # flag = 0: invoked from command line
        if len(argv) == 1:
            u = """Usage: paralite /path/to/db STATEMENT [--hub]
Please enter \"paralite help\" for more information."""
            print u
            return
        self.process_single_query(argv, flag)

    def process_single_query(self, argv, flag):
        """
        @param argv the argument as a query is issued 
        @param flag the way of process spwaned
                    # flag = 1: invoked from program
                    # flag = 0: invoked from command line
        """
        self.parse(argv)
        try:
            log_dir = self.defaultconf[conf.LOG_DIR]
            if not os.path.exists(log_dir):
                os.makedirs(log_dir)
            if not log_dir.endswith(os.sep):
                log_dir = "%s%s" % (log_dir, os.sep)                
            temp_dir = self.defaultconf[conf.LOG_DIR]
            if not os.path.exists(temp_dir):
                os.makedirs(temp_dir)
            if not temp_dir.endswith(os.sep):
                temp_dir = "%s%s" % (temp_dir, os.sep)                                
        except OSError, e:
            if e.errno == 17:
                pass
        except Exception:
            Es("ERROR: cannot create log and temp directory: %s\n" % traceback.format_exc())
            sys.exit(1)
        cur_time = time.strftime('%Y-%m-%d-%H-%M-%S',time.localtime(time.time()))
        ParaLiteLog.init("%s/paralite-%s-%s-%s.log" % (log_dir, gethostname(),
                                                          cur_time,
                                                          random.randint(1,10000)),
                            logging.DEBUG)
        ParaLiteLog.info("---------paraLite is started---------")
        ParaLiteLog.info("QUERY: %s" % self.ctquery)        
        self.args.append(self.defaultconf)
        ParaLiteLog.debug(str(self.args))                
        self.execute(self.args)
        if flag == 0:
            self.dump(self.buf.getvalue(), 1)
        else:
            if len(self.buf.getvalue()) == 0:
                return []
            return self.buf.getvalue().strip().split(self.db_row_sep)
        ParaLiteLog.info("---------paraLite exits sucussfully---------")

def recv_bytes(so, n):
    A = []
    while n > 0:
        x = so.recv(n)
        if x == "": break
        A.append(x)
        n = n - len(x)
    return string.join(A, "")

def Ws(s):
    sys.stdout.write(s)

def Es(s):
    sys.stderr.write(s)
    
if __name__ == "__main__":
    paraLite().main(sys.argv, 0)
