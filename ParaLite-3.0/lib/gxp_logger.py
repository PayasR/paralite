#
# gxp_logger.py
#
# Copyright (c) 2005-2011 by Kenjiro Taura. All rights reserved.
#
# THIS MATERIAL IS PROVIDED AS IS, WITH ABSOLUTELY NO WARRANTY 
# EXPRESSED OR IMPLIED.  ANY USE IS AT YOUR OWN RISK.
# 
# Permission is hereby granted to use or copy this program
# for any purpose,  provided the above notices are retained on all 
# copies. Permission to modify the code and to distribute modified
# code is granted, provided the above notices are retained, and
# a notice that the code was modified is included with the above
# copyright notice.

import errno
import os
import socket
import time
import traceback


import portability

dbg = 0

class gxp_logger_bug(Exception):
    pass

class gxp_logger:
    def __init__(self, filename=None):
        self.fd = None
        if filename is None:
            self.filename = self.default_log_filename()
        else:
            self.filename = filename
        self.set_log_filename(self.filename)
        self.set_log_header(None)
        self.set_log_base_time()
        self.set_show_time()
        self.set_show_caller()

    def default_log_filename(self):
        return "log-%s-%d" % (socket.gethostname(), os.getpid())

    def set_log_filename(self, filename):
        self.requested_file = filename

    def delete_log_file(self):
        if self.filename is not None:
            try:
                os.remove(self.filename)
                self.filename = None
            except EnvironmentError,_:
                pass
        
    def set_log_header(self, header):
        self.header = header

    def set_show_time(self):
        self.show_time = 1

    def set_show_caller(self):
        self.show_caller = 1

    def set_log_base_time(self):
        self.base_time = time.time()

    def get_caller(self):
        # traceback.print_stack()
        # get_caller -> log__ -> log -> LOG -> Es -> caller of LOG 
        stack = traceback.extract_stack()
        LOG_caller = None
        log_caller = None
        Es_caller = None
        for i in range(-1, -len(stack)-1, -1):
            f,line,fun,stmt = stack[i]
            if fun == "log": log_caller = stack[i - 1]
            if fun == "LOG": LOG_caller = stack[i - 1]
            if fun == "Es":  Es_caller = stack[i - 1]
        if Es_caller:
            f,line,fun,stmt = Es_caller
        elif LOG_caller:
            f,line,fun,stmt = LOG_caller
        elif log_caller:
            f,line,fun,stmt = log_caller
        else:
            f,line,fun,stmt = "?","?","?","?"
        pad = " " * max(0, (len(stack) - 8))
        filename = os.path.basename(f)[:10]
        return "%11s: %4s:%s%s: " % (filename,line,pad,fun)

    def log__(self, msg):
        if self.requested_file != self.filename:
            if self.fd is not None:
                new_file,new_fd = portability.safe_rename_open_file(self.filename, self.fd, self.requested_file)
                self.requested_file = new_file
                self.fd = new_fd
            self.filename = self.requested_file
        assert self.filename == self.requested_file, \
               (self.filename, self.requested_file)
        if self.fd is None:
            self.fd = os.open(self.filename,
                              os.O_CREAT|os.O_WRONLY|os.O_TRUNC,
                              0644)
            portability.set_close_on_exec_fd(self.fd, 1)
        assert self.fd is not None
        line = ""
        if self.header is not None:
            line = line + ("%s: " % self.header)
        if self.show_time:
            line = line + ("%.6f: " % (time.time() - self.base_time))
        if self.show_caller:
            line = line + self.get_caller()
        line = line + msg
        os.write(self.fd, line)

    def log(self, x):
        for _ in range(100):
            try:
                return self.log__(x)
            except Exception,e:
                if e.args[0] != errno.EINTR: raise
        raise gxp_logger_bug()

the_logger = gxp_logger()

def LOG(x):
    the_logger.log(x)

def set_log_filename(filename):
    the_logger.set_log_filename(filename)

def set_log_base_time():
    the_logger.set_log_base_time()

