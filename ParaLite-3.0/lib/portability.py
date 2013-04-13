#
# portability.py
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
#

import errno,os,sys
import signal

if sys.platform == "win32":
    fcntl = None
else:
    import fcntl

def Es(s):
    sys.stderr.write(s)

class portability_class:
    """
    1. Some versions of Python define F_SETFL and F_GETFL in FCNTL,
    while others in fcntl.
    """
    def __init__(self):
        self.set_fcntl_constants()

    def set_fcntl_constants(self):
        if fcntl is None: return 0
        self.F_GETFL = fcntl.F_GETFL
        self.F_SETFL = fcntl.F_SETFL
        self.F_GETFD = fcntl.F_GETFD
        self.F_SETFD = fcntl.F_SETFD
        ok = 0
        if fcntl.__dict__.has_key("FD_CLOEXEC"):
            self.FD_CLOEXEC = fcntl.FD_CLOEXEC
            ok = 1
        if ok == 0:
            try:
                FCNTL_ok = 0
                import warnings
                warnings.filterwarnings("ignore", "", DeprecationWarning)
                import FCNTL
                FCNTL_ok = 1
                warnings.resetwarnings()
            except ImportError:
                pass
            if FCNTL_ok and FCNTL.__dict__.has_key("FD_CLOEXEC"):
                self.FD_CLOEXEC = FCNTL.FD_CLOEXEC
                ok = 1
        if ok == 0:
            # assume FD_CLOEXEC = 1. see 
            # http://mail.python.org/pipermail/python-bugs-list/2001-December/009360.html
            self.FD_CLOEXEC = 1
            ok = 1

        if ok == 0:
            Es("This platform provides no ways to set "
               "close-on-exec flag. abort\n")
            os._exit(1)

    def set_blocking_fd(self, fd, blocking):
        """
        make fd non blocking
        """
        flag = fcntl.fcntl(fd, self.F_GETFL)
        if blocking:
            new_flag = flag & ~os.O_NONBLOCK
        else:
            new_flag = flag | os.O_NONBLOCK
        fcntl.fcntl(fd, self.F_SETFL, new_flag)

    def set_close_on_exec_fd(self, fd, close_on_exec):
        """
        make fd non blocking
        """
        if close_on_exec:
            fcntl.fcntl(fd, self.F_SETFD, self.FD_CLOEXEC)
        else:
            fcntl.fcntl(fd, self.F_SETFD, 0)

P = portability_class()

def set_close_on_exec_fd(fd, close_on_exec):
    if sys.platform != "win32":
        P.set_close_on_exec_fd(fd, close_on_exec)

def set_blocking_fd(fd, blocking):
    if sys.platform != "win32":
        P.set_blocking_fd(fd, blocking)

def safe_rename_unix(a, b):
    return os.rename(a, b)

def safe_rename_win(a, b):
    for i in range(100):
        try:
            return os.rename(a, b)
        except WindowsError,e:
            if e.args[0] == winerror.ERROR_ALREADY_EXISTS:
                os.remove(b)
            else:
                raise
    assert 0,("could not move %s to %s for 100 times" % (a, b))

def safe_rename(a, b):
    if sys.platform == "win32":
        return safe_rename_win(a, b)
    else:
        return safe_rename_unix(a, b)

#
#  UGLY
#  win - unix
# 

class proc_man_win:
    def proc_running(self, pid):
        """
        1 if pid is running
        """
        try:
            PROCESS_TERMINATE = 1
            PROCESS_ALL_ACCESS = 2097151
            h = win32api.OpenProcess(PROCESS_ALL_ACCESS, 0, pid)
        except pywintypes.error,e:
            if e.args[0] == winerror.ERROR_INVALID_PARAMETER: return 0
            if e.args[0] == winerror.ERROR_ACCESS_DENIED: return 0
            raise
        win32file.CloseHandle(h)
        return 1

    # signum just for interface compatibility with unix
    def kill(self, pid, signum):
        try:
            PROCESS_TERMINATE = 1
            h = win32api.OpenProcess(PROCESS_TERMINATE, 0, pid)
        except pywintypes.error,e:
            if e.args[0] == winerror.ERROR_INVALID_PARAMETER: return 0
            if e.args[0] == winerror.ERROR_ACCESS_DENIED: return 0
            raise
        try:
            win32process.TerminateProcess(h, 1)
        except pywintypes.error,e:
            # it seems if the process has gone, we get this exception
            Es("got this error but what %s\n" % (e.args,))
            if e.args[0] != winerror.ERROR_ACCESS_DENIED:
                raise
        win32file.CloseHandle(h)
        return 1

    def true_kill(self, pid):
        return self.kill(pid, -1)

    def safe_rename_open_file(self, cur, fd, new):
        """
        rename currently open file (whose descriptor is fd)
        to new. if it fails because new already exists and 
        we are working on Windows, try to remove new.
        if it still fails because another process is opening
        the file on windows, try to find a temporry file name
        that can be erasable. 
        return (the new file name, its file descriptor).
        the file pointer is positioned at the end of the file
        """
        x = new
        n_tries = 10
        os.close(fd)
        for i in range(n_tries):
            try:
                os.rename(cur, x)
                break
            except EnvironmentError,e:
                if e.args[0] != winerror.ERROR_ALREADY_EXISTS:
                    raise
            try:
                os.remove(x)
            except EnvironmentError,e:
                if e.args[0] != winerror.ERROR_SHARING_VIOLATION:
                    raise
            x = "%s.%d" % (new, i)
        if i < n_tries:
            fd = os.open(x, os.O_WRONLY, 0644)
            os.lseek(fd, 0, os.SEEK_END)
            return (x,fd)
        else:
            raise e

    def set_stdios_to_binary(self):
        msvcrt.setmode(0, os.O_BINARY)
        msvcrt.setmode(1, os.O_BINARY)
        msvcrt.setmode(2, os.O_BINARY)

class proc_man_unix:
    def kill(self, pid, x):
        try:
            os.kill(pid, x)
            return 1
        except OSError,e:
            if e.args[0] != errno.ESRCH: raise
            return 0

    def true_kill(self, pid):
        return self.kill(pid, signal.SIGKILL)

    def proc_running(self, pid):
        return self.kill(pid, 0)

    def safe_rename_open_file(self, cur, fd, new):
        os.rename(cur, new)
        return (new,fd)

    def set_stdios_to_binary(self):
        pass

if sys.platform == "win32":
    import win32api,win32file,win32process,pywintypes,winerror,msvcrt
    PM = proc_man_win()
else:
    PM = proc_man_unix()

def proc_running(pid):
    return PM.proc_running(pid)

def kill(pid, x):
    return PM.kill(pid, x)

def true_kill(pid):
    return PM.true_kill(pid)

def safe_rename_open_file(cur, fd, new):
    return PM.safe_rename_open_file(cur, fd, new)

def set_stdios_to_binary():
    return PM.set_stdios_to_binary()
