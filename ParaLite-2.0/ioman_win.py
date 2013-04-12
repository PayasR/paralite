# 
# --------- Windows-only implementation ---------
# 
import random

import errno
import os
import re
import socket
import string
import sys
import time
import types

if sys.platform == "win32":
    import select
    import win32api
    import win32event
    import win32file
    import win32pipe
    import win32process
    import win32security
    import winerror
    import pywintypes

import ioman_base
import pp
from gxp_logger import LOG,dbg

#
# On Windows, event-driven loops are implemented as follows.
#
# (1) For pipes, issue ReadFile WriteFile with an overlapping object.
#     An overlapping object has a pointer to Event object which
#     is signaled when the pipe becomes ready.  Readiness of Event
#     objects can be waited for by WaitForMultipleObject on these 
#     Event objects.
# (2) For sockets, register an Event object on socket with 
#     WSAEventSelect.  Then the registered Event object becomes
#     signaled when the socket is ready for a specified operation
#     (READ, WRITE, CLOSE).
# (3) For a child process, its termination can be detected by
#     WaitForMultipleObjects on the process handle.
# (4) In order to enable interrupt by Ctrl-C (which appears to be 
#     somehow disabled in python win32 extension, we use 
#     SetConsoleCtrlHandler, which calls a specified callback function
#     when Ctrl-C is received.  The callback signals en event
#     object, which causes WaitForMultipleObjects raises KeyboardInterrupt
#     exception (this behavior is incomprehensive).

class ioman_win_bug(Exception):
    pass

class chan_win_ctrlc(ioman_base.chan):
    """
    On Windows, WaitForMultipleObjects return with Ctrl-C 
    in Visual C. But in win32 Python extension, it does not.
    If we call SetConsoleCtrlHandler and setting an event within
    the handler (ctrlc_handler below), WaitForMultipleObjects
    now raise exception.
    """
    def __init__(self, iom):
        ioman_base.chan.__init__(self, iom)
        self.handle = win32event.CreateEvent(None, 1, 0, None)
        win32api.SetConsoleCtrlHandler(self.ctrlc_handler__, 1)
        if dbg>=2:
            LOG("%s CreateEvent(None, 1, 0, None) => %s\n" 
                % (self, self.handle))
            LOG("%s SetConsoleCtrlHandler\n" % self)
        iom.ensure_watched(self)
    def reset(self):
        win32event.ResetEvent(self.handle)
    def ctrlc_handler__(self, x):
        if dbg>=2:
            LOG("%s SetEvent(%s)\n" % (self, self.handle))
        win32event.SetEvent(self.handle)
    def reset(self):
        win32event.ResetEvent(self.handle)

class chan_win_handle_base(ioman_base.chan):
    def __init__(self, iom, pipe_handle):
        ioman_base.chan.__init__(self, iom)
        # the handle to issue the actual (overlap) reads on
        self.pipe_handle = pipe_handle
        # overlap object and the buffer for it
        self.overlap = self.mk_overlap_object__()
        # the handle that are waited for by WaitForMultipleObjects
        self.handle = self.overlap.hEvent

    def mk_overlap_object__(self):
        o = win32file.OVERLAPPED()
        o.hEvent = win32event.CreateEvent(None, 1, 0, None)
        o.Offset = 0
        o.OffsetHigh = 0
        if dbg>=2:
            LOG("%s win32file.OVERLAPPED() => %s\n" % (self, o))
            LOG("%s win32event.CreateEvent => %s\n" % (self, o.hEvent))
        return o

class chan_win_handle_read(chan_win_handle_base):
    """
    Channel to read from a pipe
    """
    def __init__(self, iom, pipe_handle):
        chan_win_handle_base.__init__(self, iom, pipe_handle)
        self.read_sz = 1000
        # set when we got BROKEN_PIPE in issue_overlap_read
        self.pipe_broken = 0
        #
        self.overlap_buf = win32file.AllocateReadBuffer(self.read_sz)
        # 1 if we want to pull data from this pipe
        self.want_read = 0
        # 1 if we have an issued but not completed read
        self.outstanding_read = 0 
        # now we issue an overlaped read
        self.request_read()

    def request_read(self):
        """
        say we want to pull whatever data is and will come in the pipe
        """
        # we enesure "want_read" flag on
        if self.want_read == 0:
            # if the pipe is previously "off", it must have been
            # removed from set of channels to watch, so we add it
            self.want_read = 1
            self.iom.ensure_watched(self)
        # ensure we have an outstanding read operation
        if self.outstanding_read == 0:
            self.outstanding_read = 1
            self.issue_overlap_read__()

    def suspend_read(self):
        """
        say we do not want to pull data from this channel
        (presumably because we have buffered too much data in memory)
        """
        # ensure want_read flag is off
        # note tht we still have an outstanding read
        # after this operation. we leave it alone.
        if self.want_read == 1:
            self.want_read = 0
            self.iom.ensure_not_watched(self)

    def issue_overlap_read__(self):
        """
        issue next overlapping read
        """
        if dbg>=2: 
            LOG("%s ResetEvent(%s)\n" % (self, self.overlap.hEvent))
            LOG("%s ReadFile(%s, .., %s)\n" % (self, self.pipe_handle, self.overlap))
        win32event.ResetEvent(self.overlap.hEvent)
        try:
            hr,x = win32file.ReadFile(self.pipe_handle, self.overlap_buf, self.overlap)
            if dbg>=2: 
                LOG("-> RETURNED (OK)\n")
        except pywintypes.error,e:
            if e.args[0] == winerror.ERROR_IO_PENDING:
                if dbg>=2: 
                    LOG("-> IO_PENDING (OK)\n")
            elif e.args[0] == winerror.ERROR_BROKEN_PIPE:
                # upon EOF, overlapping ReadFile immediately raises
                # BROKEN_FILE exception, without making the associated
                # hEvent object signaled state. This leaves 
                # WaitForMultipleObjects blocking forever.
                if dbg>=2: 
                    LOG("-> BROKEN_PIPE (OK)\n")
                    LOG("%s SetEvent(%s)\n" % (self, self.overlap.hEvent))
                self.pipe_broken = 1
                win32event.SetEvent(self.overlap.hEvent)
            else:
                raise

    def close(self):
        self.suspend_read()
        win32file.CloseHandle(self.pipe_handle)
        win32file.CloseHandle(self.handle)
        self.pipe_handle = None
        self.handle = None

    def do_read(self):            # chan_read_win_handle
        """
        Called when WaitForMultipleObjects detect that the 
        outstanding read operation has completed.
        """
        assert self.outstanding_read
        data = ""
        e = None
        if self.pipe_broken == 0:
            # Pull data from the overlap object
            if dbg>=2: 
                LOG("%s GetOverlappedResult(%s, ...)\n" % (self, self.pipe_handle))
            try:
                sz = win32file.GetOverlappedResult(self.pipe_handle, self.overlap, 0)
                data = self.overlap_buf[:sz]
            except pywintypes.error,e_:
                if e_.args[0] == winerror.ERROR_BROKEN_PIPE: 
                    if dbg>=2: 
                        LOG("-> BROKEN_PIPE (OK)\n")
                else:
                    e = e_
        if dbg>=2: 
            LOG("-> data = [%s], e = %s\n" % (pp.pp(data), pp.pp(e)))
        if data == "":
            # Error or EOF. Either case, we no longer read
            # from this pipe
            if dbg>=2:
                LOG("%s CloseHandle(%s)\n" % (self, self.pipe_handle))
                LOG("%s CloseHandle(%s)\n" % (self, self.handle))
            self.close()
        elif self.want_read:
            # A read completed.  Make sure we keep issuing 
            # an overlapped read as soon as one completes.
            self.issue_overlap_read__()
        else:
            # The client does not want to pull data.
            # Record there is no outstanding read operation
            self.outstanding_read = 0
        return data,e

class chan_win_handle_write(chan_win_handle_base):
    """
    Channel to write to a pipe
    """
    def __init__(self, iom, pipe_handle):
        chan_win_handle_base.__init__(self, iom, pipe_handle)
        # list of not-yet-completed write requests
        self.requests = []
        # number of bytes requested by not acknowledged
        self.pending_writes = 0
        self.pending_close = 0

    def request_write(self, data):
        """
        Accept a request to write data.
        """
        # basically we only put it into the queue
        if self.pipe_handle is None:
            LOG("chan_win_handle_write %s already closed, discard data [%s]\n" 
                % (self, pp.pp(data)))
            return None,None
        assert self.handle is not None
        if dbg>=2:
            LOG("%s handle = %s, data = [%s]\n" % (self, self.pipe_handle, pp.pp(data)))
        n = len(self.requests)
        self.requests.append(data)
        if data is None:
            self.pending_close = 1
        else:
            self.pending_writes = self.pending_writes + len(data)
        if n == 0:
            # We have not been having any data in the
            # queue, which means the channel has not
            # been watched.  Ensure it is now watched.
            self.iom.ensure_watched(self)
            # and issue an overlapping write
            self.issue_overlap_write__()
        return self.pending_writes,self.pending_close

    def request_eof(self):
        """
        Accept a request to write data.
        """
        # basically we only put it into the queue
        if dbg>=2: LOG("%s\n" % self)
        return self.request_write(None)

    def issue_overlap_write__(self):
        if dbg>=2: LOG("%s\n" % self)
        data = self.requests[0]
        e = None
        if data is None:
            assert (self.pending_writes == 0), self.pending_writes
            assert self.pending_close
            self.requests.pop(0)
            if dbg>=2:
                LOG("%s CloseHandle(%s)\n" % (self, self.handle))
                LOG("%s CloseHandle(%s)\n" % (self, self.pipe_handle))
            self.close()
            self.pending_close = 0
        else:
            # make data garbage collectable after passing it
            # to WriteFile
            self.requests[0] = len(data)
            if dbg>=2:
                LOG("%s ResetEvent(%s)\n" % (self, self.overlap.hEvent))
                LOG("%s WriteFile(%s, %s)\n" % (self, self.pipe_handle, pp.pp(data)))
            win32event.ResetEvent(self.overlap.hEvent)
            try:
                win32file.WriteFile(self.pipe_handle, data, self.overlap)
            except pywintypes.error,e:
                if dbg>=2: 
                    LOG("%s failed to write %s\n" % (self, e.args,))
        return e
                

    def close(self):
        self.requests = []  # discard buffered data
        self.iom.ensure_not_watched(self)
        win32file.CloseHandle(self.handle)
        win32file.CloseHandle(self.pipe_handle)
        self.handle = None
        self.pipe_handle = None

    def do_write(self):            # chan_write_win_handle
        """
        Called when a write operation (issued for the head item of the queue)
        has been completed.
        """
        # remove the request from the queue
        sz = self.requests.pop(0)
        assert type(sz) is types.IntType
        e = None
        asz = None
        if dbg>=2:
            LOG("%s GetOverlappedResult(%s, ..)\n" % (self, self.pipe_handle))
        try:
            asz = win32file.GetOverlappedResult(self.pipe_handle, self.overlap, 0)
        except pywintypes.error,e_:
            e = e_
        if e:
            self.close()
        else:
            if dbg>=2: 
                LOG("requested %s bytes, %d bytes actually written\n" 
                    % (sz, asz))
            assert (sz == asz), (sz, asz)
            self.pending_writes = self.pending_writes - sz
            assert (self.pending_writes >= 0), self.pending_writes
            if len(self.requests) > 0:
                # We have more requests waiting. issue one.
                e = self.issue_overlap_write__()
            else:
                # No more data to write. Drop this channel
                # from the watched list
                self.iom.ensure_not_watched(self)
        return self.pending_writes,self.pending_close,e

class chan_win_socket(ioman_base.chan):
    """
    Channel to read from a socket
    """
    def __init__(self, iom, so):
        ioman_base.chan.__init__(self, iom)
        self.read_sz = 1000
        self.so = so
        # the handle waited for by WaitForMultipleObjects
        self.handle = win32event.CreateEvent(None, 1, 0, None)
        if dbg>=2:
            LOG("%s CreateEvent(None,1,0,None) => %s\n" % (self, self.handle))
        # associate the handle with the socket
        self.want_read = 0
        # list of not-yet-completed write requests
        self.requests = []
        self.pending_writes = 0
        self.pending_close = 0
        self.request_read()

    def reset(self):
        """
        ensure that 
        """
        events = 0
        if self.want_read:
            events = events | win32file.FD_READ | win32file.FD_CLOSE
        if self.requests:
            events = events | win32file.FD_WRITE
        if dbg>=2:
            LOG("%s ResetEvent(%s)\n" % (self, self.handle))
            LOG("%s WSAEventSelect(%s, %s, %s)\n" 
                % (self, self.so, self.handle, self.pp_event__(events)))
        win32event.ResetEvent(self.handle)
        win32file.WSAEventSelect(self.so, self.handle, events)
        if events:
            self.iom.ensure_watched(self)
        else:
            self.iom.ensure_not_watched(self)

    def close(self):
        self.want_read = 0
        self.requests = []
        self.reset()
        self.so.close()
        win32file.CloseHandle(self.handle)
        self.so = None
        self.handle = None

    def read_wouldnt_block(self):
        R,_,_ = select.select([ self.so ], [], [], 0.0)
        if dbg>=2: 
            LOG("%s select.select([%s],[],[],0.0) => %s\n" 
                % (self, self.so, R))
        return len(R)

    def write_wouldnt_block(self):
        _,W,_ = select.select([], [ self.so ], [], 0.0)
        if dbg>=2: 
            LOG("%s select.select([],[%s],[],0.0) => %s\n" 
                % (self, self.so, W))
        return len(W)

    def pp_event__(self, event):
        s = []
        if event & win32file.FD_READ:
            s.append("FD_READ")
        if event & win32file.FD_WRITE:
            s.append("FD_WRITE")
        if event & win32file.FD_CLOSE:
            s.append("FD_CLOSE")
        if event & win32file.FD_ACCEPT:
            s.append("FD_ACCEPT")
        return string.join(s, "|")

    def request_read(self):
        """
        say we want to pull whatever data is and will come in the pipe
        """
        # ensure the handle is "Waited For"
        if self.want_read == 0:
            self.want_read = 1
            self.reset()

    def suspend_read(self):
        """
        say we do not want to pull data from this channel
        (presumably because we have buffered too much data in memory)
        """
        # ensure the handle is not "Waited For"
        if self.want_read == 1:
            self.want_read = 0
            if len(self.requests) == 0:
                self.reset()

    def request_write(self, data):
        """
        Accept a request to write data.
        """
        if self.so is None:
            LOG("chan_win_socket %s already closed, discard data [%s]\n" 
                % (self, pp.pp(data)))
            return None,None
        assert self.handle is not None
        if dbg>=2:
            LOG("%s so = %s data = [%s]\n" % (self, self.so, pp.pp(data)))
        n = len(self.requests)
        self.requests.append(data)
        if data is None:
            self.pending_close = 1
        else:
            self.pending_writes = self.pending_writes + len(data)
        if n == 0:
            # The queue has been empty. 
            # Ensure this channel is watched for.
            self.reset()
        return self.pending_writes,self.pending_close

    def request_eof(self):
        if dbg>=2: LOG("%s\n" % self)
        return self.request_write(None)

    def do_read(self):            # chwn_read_win_socket
        """
        Called when WaitForMultipleObjects detected the socket is now
        readable. Pull off data.
        """
        data = ""
        e = None
        if dbg>=2:
            LOG("%s socket.recv(%s, %d)\n" % (self, self.so, self.read_sz))
        try:
            data = self.so.recv(self.read_sz)
        except socket.error,e_:
            if e_.args[0] == errno.EINPROGRESS:
                if dbg>=2:
                    LOG("-> %s\n" % (e_.args,))
            elif hasattr(errno, "WSAECONNRESET") \
                    and e_.args[0] == errno.WSAECONNRESET:
                if dbg>=2:
                    LOG("-> %s\n" % (e_.args,))
            else:
                e = e_
        if dbg>=2: 
            LOG("data = [%s], e = %s\n" % (pp.pp(data), pp.pp(e)))
        if e:
            self.close()
        elif data == "":
            # Error or EOF. Either case, we no longer read
            # from this pipe
            self.suspend_read()
        else:
            self.reset()
        return data,e

    def do_write(self):            # chan_write_win_socket
        """
        Called when this socket becomes ready for write.
        Actually perform send operation.
        """
        data = self.requests.pop(0)
        e = None
        if data is None:
            assert self.pending_close
            assert (self.pending_writes == 0), self.pending_writes
            if dbg>=2:
                LOG("%s socket.shutdown(%s, SHUT_WR)\n" % (self, self.so))
            try:
                self.so.shutdown(socket.SHUT_WR)
                self.pending_close = 0
            except socket.error,e_:
                e = e_
        else:
            if dbg>=2:
                LOG("%s socket.send(%s, %s)\n" % (self, self.so, pp.pp(data)))
            sz = len(data)
            asz = None
            try:
                asz = self.so.send(data)
            except socket.error,e_:
                e = e_
            if e is None:
                if dbg>=2:
                    LOG("%d bytes requested, %d bytes actually written\n" % (sz, asz))
                assert (sz == asz), (sz, asz)
                self.pending_writes = self.pending_writes - sz
                assert (self.pending_writes >= 0), self.pending_writes
        if e:
            self.close()
        else:
            self.reset()
        return self.pending_writes,self.pending_close,e

class chan_win_server_socket(ioman_base.chan):
    """
    Channel to accept a connection
    """
    def __init__(self, iom, ss):
        ioman_base.chan.__init__(self, iom)
        handle = win32event.CreateEvent(None, 1, 0, None)
        if dbg>=2:
            LOG("%s CreateEvent(None,1,0,None) => %s\n" % (self, handle))
        self.ss = ss
        self.handle = handle
        self.reset()

    def reset(self):
        """
        ensure that 
        """
        if dbg>=2:
            LOG("%s ResetEvent(%s)\n" % (self, self.handle))
            LOG("%s WSAEventSelect(%s, %s, FD_ACCEPT)\n" 
                % (self, self.ss, self.handle))
        win32event.ResetEvent(self.handle)
        win32file.WSAEventSelect(self.ss, self.handle, win32file.FD_ACCEPT)
        self.iom.ensure_watched(self)

    def do_io(self):            # chan_accept_win_socket
        # FIXME: handle exception
        new_so,_ = self.ss.accept()
        if dbg>=2: 
            LOG("%s socket.accept(%s) => %s\n" % (self, self.ss, new_so))
        self.reset()
        ch = chan_win_socket(self.iom, new_so)
        return ch,None

class chan_win_process(ioman_base.chan):
    """
    Channel to wait for temination of a child
    """
    def __init__(self, iom, handle, pid):
        ioman_base.chan.__init__(self, iom)
        self.handle = handle
        self.pid = pid
        self.status = None
        iom.ensure_watched(self)

    def do_io(self):
        self.status = win32process.GetExitCodeProcess(self.handle)
        if dbg>=2: 
            LOG("%s GetExitCodeProcess(%s) => %s\n" % (self, self.handle, self.status))
            LOG("%s CloseHandle(%s)\n" % (self, self.handle))
        win32file.CloseHandle(self.handle)
        self.iom.ensure_not_watched(self)
        return self.status,None

class ioman_win(ioman_base.ioman):
    """
    IO manager on Windows
    """
    def __init__(self):
        self.pipe_buf_sz = 1000000
        self.unique_id = os.getpid()
        self.rand = random.Random()
        self.channels = {}      # handle -> channel
        self.handles = []       # list of handles = r_channels.keys() + w_channels.keys()
        self.pending = []       # list of pending events
        self.ctrlc = chan_win_ctrlc(self)

    def ensure_watched(self, ch):
        if dbg>=2: LOG("%s handle = %s\n" % (self, ch.handle))
        if not self.channels.has_key(ch.handle):
            self.channels[ch.handle] = ch
            self.handles.append(ch.handle)

    def ensure_not_watched(self, ch):
        if dbg>=2: LOG("%s handle = %s\n" % (self, ch.handle))
        if self.channels.has_key(ch.handle):
            del self.channels[ch.handle]
            self.handles.remove(ch.handle)

    def create_inheritable_security_attribute__(self):
        sa = win32security.SECURITY_ATTRIBUTES()
        sa.SECURITY_DESCRIPTOR = None
        sa.bInheritHandle = 1
        return sa

    def create_pipe_regular_from_child_to_parent__(self):
        bufsz = self.pipe_buf_sz
        name = (r"\\.\Pipe\%08x.%08x" 
                % (self.unique_id, self.rand.randint(0, 100000000)))
        cnp = win32pipe.CreateNamedPipe
        cf = win32file.CreateFile
        # make the parent side of the pipe, OVERLAPPED and non-inheritable
        r = cnp(name, 
                win32pipe.PIPE_ACCESS_INBOUND | win32file.FILE_FLAG_OVERLAPPED,
                win32pipe.PIPE_TYPE_BYTE | win32pipe.PIPE_WAIT,
                1, bufsz, bufsz, win32event.INFINITE, None)
        if dbg>=2: 
            LOG("%s CreateNamedPipe(%s, PIPE_ACCESS_INBOUND, ...) => %s\n" % (self, name, r))
        sa = self.create_inheritable_security_attribute__()
        # make the client side of the pipe, non-overlapped and inheritable
        w = cf(name,
               win32file.GENERIC_WRITE, 0, sa, win32file.OPEN_EXISTING, 
               win32file.FILE_ATTRIBUTE_NORMAL, None)
        if dbg>=2: LOG("%s CreateFile(%s, GENERIC_WRITE, ...) => %s\n" % (self, name, w))
        ch_r = chan_win_handle_read(self, r)
        if dbg>=2: LOG("ch_r = %s\n" % ch_r)
        return ch_r,w

    def create_pipe_regular_from_parent_to_child__(self):
        bufsz = self.pipe_buf_sz
        name = (r"\\.\Pipe\%08x.%08x" 
                % (self.unique_id, self.rand.randint(0, 100000000)))
        cnp = win32pipe.CreateNamedPipe
        cf = win32file.CreateFile
        # make the parent side of the pipe, OVERLAPPED and non-inheritable
        w = cnp(name, 
                win32pipe.PIPE_ACCESS_OUTBOUND | win32file.FILE_FLAG_OVERLAPPED,
                win32pipe.PIPE_TYPE_BYTE | win32pipe.PIPE_WAIT,
                1, bufsz, bufsz, win32event.INFINITE, None)
        if dbg>=2: 
            LOG("%s CreateNamedPipe(%s, PIPE_ACCESS_OUTBOUND, ...) => %s\n" % (self, name, w))
        sa = self.create_inheritable_security_attribute__()
        # make the client side of the pipe, non-overlapped and inheritable
        r = cf(name,
               win32file.GENERIC_READ, 0, sa, win32file.OPEN_EXISTING, 
               win32file.FILE_ATTRIBUTE_NORMAL | win32file.FILE_FLAG_OVERLAPPED, None)
        if dbg>=2: LOG("%s CreateFile(%s, GENERIC_READ, ...) => %s\n" % (self, name, r))
        ch_w = chan_win_handle_write(self, w)
        return r,ch_w

    def create_server_socket(self, af, sock_type, qlen, addr):
        """
        todo: make ss non-inheritable?
        """
        ss = socket.socket(af, sock_type)
        if dbg>=2: 
            LOG("%s socket.socket(af=%s, sock_type=%s, qlen=%d addr=%s) => %s\n"
                % (self, af, sock_type, qlen, addr, ss))
        ss.bind(addr)
        ss.listen(qlen)
        ch = chan_win_server_socket(self, ss)
        return ch

    def add_socket(self, so):
        """
        somewhat ad-hoc/ugly API to add an existing socket 
        to the watch list (used by gxpc to add connection to gxpd).
        """
        if dbg>=2:
            LOG("%s so = %s\n" % (self, so.fileno()))
        return chan_win_socket(self, so)

    def create_redirect_pipes__(self, pipes):
        """
        pipes : (fd on child or None, "r" or "w", pipe, socket, or None)
        aux method for create_process.

        (None,"w",x)

        you get handle:x
        """
        if dbg>=2: LOG("%s pipes = %s\n" % (self, pipes))
        si = win32process.STARTUPINFO()
        si.dwFlags |= win32process.STARTF_USESTDHANDLES
        r_channels = []
        w_channels = []
        handles_to_close_at_parent = []
        # on windows fd must be 0, 1, 2, 
        for kind,r_or_w,fd in pipes:
            if r_or_w == "r":
                ch_r,w_handle = self.create_pipe_from_child_to_parent__(kind)
                # indicate ch_r is connected to the child's fd
                ch_r.client_fd = fd
                r_channels.append(ch_r)
                # child should write to fd
                handles_to_close_at_parent.append((w_handle, fd, "w"))
            elif r_or_w == "w":
                r_handle,ch_w = self.create_pipe_from_parent_to_child__(kind)
                # indicate ch_r is connected to the child's fd
                ch_w.client_fd = fd
                # child should read from fd
                handles_to_close_at_parent.append((r_handle, fd, "r"))
                w_channels.append(ch_w)
            else:
                raise ioman_win_bug()
            if 0 <= fd < 3:
                si.dwFlags |= win32process.STARTF_USESTDHANDLES
                if fd == 0:
                    assert r_or_w == "w"
                    si.hStdInput = r_handle.handle
                elif fd == 1:
                    assert r_or_w == "r"
                    si.hStdOutput = w_handle.handle
                elif fd == 2:
                    assert r_or_w == "r"
                    si.hStdError = w_handle.handle
        if dbg>=2: LOG("r_channels = %s, w_channels = %s\n" % (r_channels, w_channels))
        return si,r_channels,w_channels,handles_to_close_at_parent

    def get_existing_dir(self, cwds):
        if cwds is None: return None
        for d in cwds:
            if os.path.exists(d):
                return d
        return None

    def cmd_list_to_string(self, L):
        """
        L : list of strings
        return a string concatinating all elements of L,
        except:
        - quote (") is escaped
        - if it contains spaces, it is quoted
        """
        S = []
        p = re.compile(r"""[0-9A-Za-z_:/+@\(\)\[\]\\\-\.\^]+$""")
        for x in L:
            LOG("x=%s\n" % x)
            # escape double quotes
            x = string.replace(x, '"', '\\\"')
            LOG(" -> %s\n" % x)
            # if it does not contain any special character
            # simply put it
            if p.match(x):
                y = x
            else:
                # otherwise, double-quote it
                y = '"%s"' % x
            LOG("  -> %s\n" % y)
            S.append(y)
        return string.join(S, " ")

    def create_process(self, cmd, env, cwds, pipes, rlimit):
        """
        Create a new process
        """
        if dbg>=2: 
            LOG("%s cmd=%s, env=%s, cwds=%s, pipes=%s, rlimit=%s\n" 
                % (self, cmd, env, cwds, pipes, rlimit))
        # cmd_ = string.join(cmd, " ")
        cmd_ = self.cmd_list_to_string(cmd)
        si,r_chans,w_chans,handles_to_close = self.create_redirect_pipes__(pipes)
        inherit = 0
        if len(pipes) > 0: inherit = 1
        cp = win32process.CreateProcess
        e = None
        cwd = self.get_existing_dir(cwds)
        if dbg>=2:
            LOG("%s CreateProcess(.., '%s', .., .., %d, 0, %s, %s, %s)\n" 
                % (self, cmd_, inherit, env, cwds, si))
        try:
            ph,th,pid,tid = cp(None, cmd_, None, None, inherit, 0, env, cwd, si)
        except pywintypes.error,e:
            pass
        for h,_,_ in handles_to_close:
            if dbg>=2:
                LOG("%s CloseHandle(%s)\n" % (self, h))
            win32file.CloseHandle(h)
        if e:
            if dbg>=2: LOG("-> %s\n" % (e.args,))
            # return None,None,None,e
            return None,r_chans,w_chans,e
        else:
            if dbg>=2: LOG("-> pid = %d, r_chans = %s, w_chans = %s\n" 
                           % (pid, r_chans, w_chans))
            proc = chan_win_process(self, ph, pid)
            return pid,r_chans,w_chans,e

    def register_std_chans(self, idxs):
        # idx : 0, 1, or 2
        # this assumes they were created with FLAG_OVERLAPPED
        # otherwise we block on stdin
        if dbg>=2:
            LOG("%s idxs = %s\n" % (self, idxs))
        ch_stdin = ch_stdout = ch_stderr = None
        if 0 in idxs:
            h0 = win32api.GetStdHandle(win32api.STD_INPUT_HANDLE)
            ch_stdin = chan_win_handle_read(self, h0)
            if dbg>=2: LOG("ch_stdin = %s\n" % ch_stdin)
        if 1 in idxs:
            h1 = win32api.GetStdHandle(win32api.STD_OUTPUT_HANDLE)
            ch_stdout = chan_win_handle_write(self, h1)
        if 2 in idxs:
            h2 = win32api.GetStdHandle(win32api.STD_ERROR_HANDLE)
            ch_stderr = chan_win_handle_write(self, h2)
        return ch_stdin,ch_stdout,ch_stderr
            
    def redirect_std(self, stdin_file, stdout_file, stderr_file):
        """
        what is this for?
        """
        cf = win32file.CreateFile
        if stdin_file is not None:
            r0 = cf(stdin_file,
                    win32file.GENERIC_READ, 0, None, win32file.OPEN_EXISTING, 
                    win32file.FILE_ATTRIBUTE_NORMAL, None)
            win32api.SetStdHandle(win32api.STD_INPUT_HANDLE, r0)
        if stdout_file is not None:
            w1 = cf(stdout_file,
                    win32file.GENERIC_WRITE, 0, None, win32file.CREATE_NEW, 
                    win32file.FILE_ATTRIBUTE_NORMAL, None)
            win32api.SetStdHandle(win32api.STD_OUTPUT_HANDLE, w1)
        if stderr_file is not None:
            w2 = cf(stderr_file,
                    win32file.GENERIC_WRITE, 0, None, win32file.CREATE_NEW, 
                    win32file.FILE_ATTRIBUTE_NORMAL, None)
            win32api.SetStdHandle(win32api.STD_ERROR_HANDLE, w2)


    def nointr_wait_for_multiple_object(self, handles, x, timeout_ms):
        while 1:
            try:
                return win32event.WaitForMultipleObjects(handles, x, timeout_ms)
            except KeyboardInterrupt,e:
                # if ctrl-c is hit, we get KeyboardInterrupt
                pass

    def fill_pending__(self, timeout):
        """
        wait for the next event to happen.
        respect timeout specified on each channel
        """
        if dbg>=2: LOG("%s\n" % self)
        # to = 
        if timeout == float("inf"):
            timeout_ms = win32event.INFINITE
        else:
            timeout_ms = int(timeout * 1.0E3)
        if dbg>=2: LOG("%s WaitForMultipleObjects(%s)\n" % (self, self.handles))
        x = self.nointr_wait_for_multiple_object(self.handles, 0, timeout_ms)
        o0 = win32event.WAIT_OBJECT_0
        assert (o0 <= x < o0 + len(self.handles) \
                    or x == win32event.WAIT_TIMEOUT), \
            (o0, x, len(self.handles))
        if x == win32event.WAIT_TIMEOUT: return
        h = self.handles[x - o0]
        ch = self.channels[h]
        if dbg>=2: LOG("-> handle=%s ch=%s\n" % (h, ch))
        if isinstance(ch, chan_win_process):
            status,err = ch.do_io()
            assert (err is None), err
            self.pending.append(ioman_base.event_death(ch.pid, status))
        elif isinstance(ch, chan_win_server_socket):
            new_ch,err = ch.do_io()
            assert (err is None), err
            self.pending.append(ioman_base.event_accept(ch, new_ch, err))
        elif isinstance(ch, chan_win_handle_read):
            data,err = ch.do_read()
            eof = 0
            if data == "" or err:
                eof = 1
            self.pending.append(ioman_base.event_read(ch, data, eof, err))
        elif isinstance(ch, chan_win_handle_write):
            pending_bytes,pending_close,err = ch.do_write()
            self.pending.append(ioman_base.event_write(ch, pending_bytes,
                                                     pending_close, err))
        elif isinstance(ch, chan_win_socket):
            n_events = 0       # make sure we get at least one event
            if ch.read_wouldnt_block():
                data,err = ch.do_read()
                eof = 0
                if data == "" or err:
                    eof = 1
                n_events = n_events + 1
                self.pending.append(ioman_base.event_read(ch, data, eof, err))
            if ch.requests and ch.so and ch.write_wouldnt_block():
                pending_bytes,pending_close,err = ch.do_write()
                n_events = n_events + 1
                self.pending.append(ioman_base.event_write(ch, pending_bytes,
                                                         pending_close, err))
        elif isinstance(ch, chan_win_ctrlc):
            ch.reset()
            self.pending.append(ioman_base.event_win_ctrlc(ch))
        else:
            raise ioman_win_bug()

    def next_event(self, timeout):
        if dbg>=2: LOG("\n")
        if len(self.pending) == 0:
            self.fill_pending__(timeout)
            if len(self.pending) == 0:
                return None
        ev = self.pending.pop(0)
        if dbg>=2:
            LOG("-> %s\n" % ev)
        return ev

