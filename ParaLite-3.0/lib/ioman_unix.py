# 
# --------- Unix-only implementation ---------
# 

import cStringIO,errno,os,select,signal,socket,string,time

import portability
import ioman_base
import pp
import gxp_logger
LOG = gxp_logger.LOG
dbg = gxp_logger.dbg

def no_intr(f, args):
    keyboard = 0
    for i in range(0, 500):
        try:
            return apply(f, args)
        except EnvironmentError,e:
            pass
        except socket.error,e:
            pass
        except select.error,e:
            pass
        #except KeyboardInterrupt,e:
        #    keyboard = 1
        #    pass
        if keyboard == 0 and e.args[0] != errno.EINTR: raise
        if dbg>=3: 
            LOG("interrupted %d times %s, retry\n" % (i + 1, e.args,))
    raise

class chan_unix_fd_read(ioman_base.chan):
    def __init__(self, iom, fd):
        ioman_base.chan.__init__(self, iom)
        self.events = 0
        self.read_sz = 1000
        self.fd = fd
        portability.set_blocking_fd(fd, 0)
        portability.set_close_on_exec_fd(fd, 1)
        self.want_read = 0
        self.request_read()
        if dbg>=3: 
            LOG("%s fd = %d\n" % (self.__class__.__name__, fd))

    def __str__(self):
        return "%s(fd=%s, id=%s)" % (self.__class__.__name__, self.fd, id(self))

    def request_read(self):
        """
        say we want to pull whatever data is and will come in the pipe
        """
        # ensure the handle is "Waited For"
        assert self.fd is not None
        if self.fd is None:
            LOG("chan_unix_fd_read %s already closed. cannot read\n" % self)
            return
        if self.want_read == 0:
            self.want_read = 1
            self.iom.add_channel_to_watch_read__(self)

    def suspend_read(self):
        """
        say we do not want to pull data from this channel
        (presumably because we have buffered too much data in memory)
        """
        # ensure the handle is not "Waited For"
        assert self.fd is not None
        if self.want_read == 1:
            self.want_read = 0
            self.iom.del_channel_to_watch_read__(self)

    def do_read(self):
        """
        pull off data.
        """
        data = None
        e = None
        if dbg>=3: 
            LOG("os.read(%d, %d)\n" % (self.fd, self.read_sz))
        try:
            data = os.read(self.fd, self.read_sz)
        except EnvironmentError,e:
            pass
        if dbg>=3: 
            LOG("data = [%s], e = %s\n" % 
                (pp.pp(data), pp.pp(e)))
        if e or data == "":
            # Error or EOF. Either case, we no longer read
            # from this pipe
            if dbg>=1:
                LOG("os.close(%d)\n" % self.fd)
            try:
                no_intr(os.close, (self.fd,))
            except EnvironmentError,e2:
                if dbg>=1:
                    LOG("could not close %d, ignored %s\n" 
                        % (self.fd, pp.pp(e2)))
            self.suspend_read()
            self.fd = None
        return data,e

class chan_unix_fd_write(ioman_base.chan):
    def __init__(self, iom, fd):
        ioman_base.chan.__init__(self, iom)
        self.events = 0
        self.fd = fd
        portability.set_blocking_fd(fd, 0)
        portability.set_close_on_exec_fd(fd, 1)
        # list of not-yet-completed write requests
        self.requests = []
        self.pending_writes = 0
        self.pending_close = 0
        if dbg>=3: 
            LOG("%s fd = %d\n" % (self.__class__.__name__, fd))

    def __str__(self):
        return "%s(fd=%s, id=%s)" % (self.__class__.__name__, self.fd, id(self))

    def request_write(self, data):
        """
        Accept a request to write data.
        """
        if self.fd is None:
            LOG("chan_unix_fd_write %s already closed, discard data [%s]\n" 
                % (self, pp.pp(data)))
            return None,None
        if dbg>=3:
            LOG("fd = %d, data = [%s]\n" % (self.fd, pp.pp(data)))
        n = len(self.requests)
        self.requests.append(data)
        if data is None:
            self.pending_close = 1
        else:
            self.pending_writes = self.pending_writes + len(data)
        if n == 0:
            self.iom.add_channel_to_watch_write__(self)
        return self.pending_writes,self.pending_close

    def request_eof(self):
        """
        Accept a request to write data.
        """
        if self.fd is None:
            LOG("chan_unix_fd_write %s already closed, discard EOF request\n" % self)
            return None,None
        if dbg>=3:
            LOG("fd = %d\n" % self.fd)
        return self.request_write(None)

    def do_write(self):
        """
        Called when this socket becomes ready for write.
        Actually perform send operation.
        """
        data = self.requests[0] # grab data to write
        e = None                # exception object if we get one
        if data is None:
            assert self.pending_close
            assert (self.pending_writes == 0), self.pending_writes
            if dbg>=1:
                LOG("os.close(%d)\n" % self.fd)
            try:
                no_intr(os.close, (self.fd,))
            except EnvironmentError,e:
                if dbg>=1:
                    LOG("os.close(%d) failed, ignored %s\n" 
                        % (self.fd, pp.pp(e)))
            if e is None:
                self.requests.pop(0)
                self.pending_close = 0
        else:
            if dbg>=3:
                LOG("os.write(%d, [%s])\n" % (self.fd, pp.pp(data)))
            sz = len(data)
            asz = 0
            try:
                asz = no_intr(os.write, (self.fd, data))
                assert (0 < asz <= sz), (asz, sz)
            except EnvironmentError,e:
                pass
            if dbg>=3: 
                LOG("requested %s bytes, %d bytes actually written, err = %s\n" 
                    % (sz, asz, pp.pp(e)))
            self.pending_writes = self.pending_writes - asz
            if e:
                if dbg>=1:
                    LOG("os.close(%d)\n" % self.fd)
                try:
                    no_intr(os.close, (self.fd, ))
                except EnvironmentError,e2:
                    if dbg>=1:
                        LOG("os.close(%d) failed, ignored %s\n" 
                            % (self.fd, pp.pp(e2)))
            elif asz == sz:
                self.requests.pop(0)
            else:
                self.requests[0] = data[asz:]
        if e:
            # an error occurred. discard this file descriptor
            if dbg>=1 and len(self.requests) > 0:
                LOG("write_channel %d closed with buffered data %s\n"
                    % (self.fd, pp.pp(self.requests[0])))
            self.requests = []
            self.iom.del_channel_to_watch_write__(self)
            self.fd = None
        elif len(self.requests) == 0:
            # No more requests. Drop this channel
            # from the watched list
            self.iom.del_channel_to_watch_write__(self)
        return self.pending_writes,self.pending_close,e

class chan_unix_socket(ioman_base.chan):
    def __init__(self, iom, so):
        ioman_base.chan.__init__(self, iom)
        self.events = 0
        self.read_sz = 1073741824 # set 1024MB
        self.so = so
        self.fd = so.fileno()
        so.setblocking(0)
        portability.set_close_on_exec_fd(self.fd, 1)
        self.want_read = 0
        self.requests = []
        self.pending_writes = 0
        self.pending_close = 0
        self.request_read()
        if dbg>=3: 
            LOG("%s fd = %d\n" % (self.__class__.__name__, self.fd))

    def __str__(self):
        return "%s(fd=%s, id=%s)" % (self.__class__.__name__, self.fd, id(self))

    def request_read(self):
        """
        say we want to pull whatever data is and will come in the pipe
        """
        # ensure the handle is "Waited For"
        assert self.fd is not None
        if self.fd is None:
            LOG("chan_unix_socket %s already closed. cannot read\n" % self)
            return
        if self.want_read == 0:
            self.want_read = 1
            self.iom.add_channel_to_watch_read__(self)

    def suspend_read(self):
        """
        say we do not want to pull data from this channel
        (presumably because we have buffered too much data in memory)
        """
        # ensure the handle is not "Waited For"
        if dbg>=3: LOG("\n")
        assert self.fd is not None
        if self.want_read == 1:
            self.want_read = 0
            self.iom.del_channel_to_watch_read__(self)

    def discard_socket(self):
        if dbg>=3: LOG("fd = %s\n" % self.fd)
        if len(self.requests) > 0:
            if dbg>=1:
                LOG("write_channel %d closed with buffered data %s\n"
                    % (self.fd, pp.pp(self.requests[0])))
            self.requests = []
            if self.want_read:
                self.want_read = 0
                self.iom.del_channel_to_watch_read_write__(self)
            else:
                self.iom.del_channel_to_watch_write__(self)
        else:
            if self.want_read:
                self.want_read = 0
                self.iom.del_channel_to_watch_read__(self)
            else:
                bomb()
        assert not self.iom.channels.has_key(self.fd)
        self.so = None
        self.fd = None

    def do_read(self):
        """
        pull off data
        """
        data = None
        e = None
        if dbg>=3:
            LOG("socket.recv(%s, %d)\n" % (self.fd, self.read_sz))
        if self.so is None:
            if dbg>=1: LOG("chan_unix_socket %s already closed\n" % self)
            return "",None
        try:
            data = no_intr(self.so.recv, (self.read_sz,))
        except socket.error,e:
            pass
        if dbg>=3: 
            LOG("data = [%s], e = %s\n" % (pp.pp(data), pp.pp(e)))
        if e or data == "":
            # Error or EOF. Either case, we no longer read
            # from this socket
            if dbg>=1:
                LOG("socket.close(%s) after EOF or error\n" % self.fd)
            try:
                no_intr(self.so.close, ())
            except socket.error,e2:
                if dbg>=1:
                    LOG("could not close %d, ignore %s\n" 
                        % (self.fd, pp.pp(e2)))
            # since we got an error, we stop both
            # reading and writing
            assert self.want_read
            self.discard_socket()
        return data,e

    def request_write(self, data):
        """
        Accept a request to write data.
        """
        if self.fd is None:
            if dbg>=1: 
                LOG("chan_unix_socket %s already closed, discard data [%s]\n" 
                    % (self, pp.pp(data)))
            return None,None
        if dbg>=3:
            LOG("fd = %d, data = [%s]\n" % (self.fd, pp.pp(data)))
        n = len(self.requests)
        self.requests.append(data)
        if data is None:
            self.pending_close = 1
        else:
            self.pending_writes = self.pending_writes + len(data)
        if n == 0:
            self.iom.add_channel_to_watch_write__(self)
        return self.pending_writes,self.pending_close

    def request_eof(self):
        """
        Accept a request to write data.
        """
        if self.fd is None:
            if dbg>=1: 
                LOG("chan_unix_socket %s already closed, discard EOF request\n" % self)
            return None,None
        if dbg>=3: LOG("socket = %d\n" % self.fd)
        return self.request_write(None)

    def do_write(self):
        """
        Called when this socket becomes ready for write.
        Actually perform send operation.
        """
        data = self.requests[0]
        e = None
        if data is None:
            # this is send EOF request. shutdown write channel
            assert self.pending_close
            assert (self.pending_writes == 0), self.pending_writes
            if dbg>=3:
                LOG("socket.shutdown(%d)\n" % self.fd)
            try:
                no_intr(self.so.shutdown, (socket.SHUT_WR,))
            except socket.error,e:
                pass
            if e:
                if dbg>=1:
                    LOG("socket.shutdown(%d) failed %s\n" % (self.fd, pp.pp(e)))
            else:
                self.requests.pop(0)
                self.pending_close = 0
        else:
            if dbg>=3:
                LOG("socket.send(%d, %s)\n" % (self.fd, pp.pp(data)))
            sz = len(data)
            asz = 0
            try:
                asz = no_intr(self.so.send, (data,))
                assert (0 < asz <= sz), (asz, sz)
            except EnvironmentError,e:
                pass
            if dbg>=3: 
                LOG("requested %s bytes, %d bytes actually written, err = %s\n" 
                    % (sz, asz, pp.pp(e)))
            self.pending_writes = self.pending_writes - asz
            if e:
                if dbg>=1:
                    LOG("socket.close(%d) to clean up after a send failure\n" % self.fd)
                try:
                    no_intr(self.so.close, ())
                except socket.error,e2:
                    if dbg>=1: 
                        LOG("socket.close(%d) failed, ignored %s\n" 
                            % (self.fd, pp.pp(e2)))
            elif asz == sz:
                self.requests.pop(0)
            else:
                self.requests[0] = data[asz:]
        if e:
            assert len(self.requests) > 0
            self.discard_socket()
        elif len(self.requests) == 0:
            # No more requests. Drop this channel
            # from the watched list
            self.iom.del_channel_to_watch_write__(self)
        return self.pending_writes,self.pending_close,e

class chan_unix_server_socket(ioman_base.chan):
    def __init__(self, iom, ss):
        ioman_base.chan.__init__(self, iom)
        self.events = 0
        self.ss = ss
        self.fd = ss.fileno()
        portability.set_close_on_exec_fd(self.fd, 1)
        iom.add_channel_to_watch_read__(self)
        if dbg>=3: 
            LOG("%s fd = %d\n" % (self.__class__.__name__, self.fd))

    def __str__(self):
        return "%s(fd=%s, id=%s)" % (self.__class__.__name__, self.fd, id(self))

    def do_read(self):
        if dbg>=3: 
            LOG("socket.accept(%s)\n" % self.ss.fileno())
        # FIXME : handle exception
        new_so,_ = no_intr(self.ss.accept, ())
        # todo: not sure if this guarantees next connect to be
        # handled
        ch = chan_unix_socket(self.iom, new_so)
        return ch,None


class ioman_unix_bug(Exception):
    pass

class ioman_unix(ioman_base.ioman):
    def __init__(self):
        self.unique_id = os.getpid()
        self.channels = {}   # fd -> channel
        self.po = select.poll()
        self.events_in = select.POLLIN|select.POLLPRI # readable
        self.events_out = select.POLLOUT              # writable
        # error or EOF
        self.events_hup = select.POLLHUP
        self.events_error = select.POLLERR | select.POLLNVAL 
        self.procs = {}         # pid -> None
        self.pending = []       # list of (chan,data,err)
        self.setup_child_watch__()

    def register_poll(self, fd, mask):
        if dbg>=3:
            LOG("fd = %d, mask = %s\n" % (fd, self.pp_event__(mask)))
        self.po.register(fd, mask)

    def unregister_poll(self, fd):
        if dbg>=3:
            LOG("fd = %d\n" % fd)
        self.po.unregister(fd)
        
    def sigchld_handler__(self, x, y):
        no_intr(os.write, (self.child_watch_w, "x"))

    def setup_child_watch__(self):
        r,w = os.pipe()
        if dbg>=3:
            LOG("os.pipe() -> r=%d,w=%d\n" % (r, w))
        self.child_watch_w = w
        self.child_watch = chan_unix_fd_read(self, r)
        signal.signal(signal.SIGCHLD, self.sigchld_handler__)

    def reap_children__(self):
        dead = []
        while 1:
            if dbg>=3: LOG("waitpid\n")
            try:
                pid,status = os.waitpid(-1, os.WNOHANG)
                if pid == 0: break
            except OSError,e:
                if e.args[0] == errno.ECHILD: break
                raise
            if dbg>=3: LOG("waitpid returned %d,%d\n" % (pid, status))
            if pid in self.procs:
                del self.procs[pid]
            dead.append((pid, status))
        # assert len(dead) > 0
        return dead

    def add_channel_to_watch__(self, ch, event):
        """
        event : self.events_in or self.events_out
        """
        fd = ch.fd
        if dbg>=3:
            LOG("fd = %s\n" % fd)
        assert ((ch.events & event) == 0), (ch.events, event)
        ch.events = ch.events | event
        if self.channels.has_key(fd):
            self.unregister_poll(fd)
        else:
            self.channels[fd] = ch
        self.register_poll(fd, ch.events)

    def add_channel_to_watch_read__(self, ch):
        self.add_channel_to_watch__(ch, self.events_in)

    def add_channel_to_watch_write__(self, ch):
        self.add_channel_to_watch__(ch, self.events_out)

    def del_channel_to_watch__(self, ch, event):
        """
        event : self.event_in or self.event_out
        """
        fd = ch.fd
        if dbg>=3: LOG("fd = %s\n" % fd)
        assert self.channels.has_key(fd)
        assert (ch.events & event), ch.events
        ch.events = ch.events - event
        self.unregister_poll(fd)
        if ch.events:
            self.register_poll(fd, ch.events)
        else:
            del self.channels[fd]

    def del_channel_to_watch_read__(self, ch):
        """
        say we do not want to read from this channel
        (either temporarily or permanently).
        this must be called only when we have been 
        interested in reading from this channel.
        """
        if dbg>=3: LOG("\n")
        self.del_channel_to_watch__(ch, self.events_in)

    def del_channel_to_watch_write__(self, ch):
        """
        say we do not want to write to this channel
        (either temporarily or permanently).
        this must be called only when we have been 
        interested in writing to this channel.
        """
        if dbg>=3: LOG("\n")
        self.del_channel_to_watch__(ch, self.events_out)

    def del_channel_to_watch_read_write__(self, ch):
        if dbg>=3: LOG("\n")
        self.del_channel_to_watch__(ch, self.events_in|self.events_out)

    def add_proc_to_watch__(self, pid):
        if dbg>=3: LOG("pid = %s\n" % pid)
        self.procs[pid] = None

    def create_pipe_regular_from_child_to_parent__(self):
        r,w = os.pipe()
        if dbg>=3:
            LOG("os.pipe() -> r=%d,w=%d\n" % (r, w))
        ch_r = chan_unix_fd_read(self, r)
        return ch_r,w

    def create_pipe_regular_from_parent_to_child__(self):
        r,w = os.pipe()
        if dbg>=3:
            LOG("os.pipe() -> r=%d,w=%d\n" % (r, w))
        ch_w = chan_unix_fd_write(self, w)
        return r,ch_w

    def create_server_socket(self, af, sock_type, qlen, addr):
        """
        todo: make ss non-inheritable?
        """
        if dbg>=3: 
            LOG("af = %s, sock_type = %s, qlen = %d, addr = %s\n"
                % (af, sock_type, qlen, addr))
        ss = socket.socket(af, sock_type)
        ss.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        ss.bind(addr)
        ss.listen(qlen)
        ch = chan_unix_server_socket(self, ss)
        return ch

    def add_socket(self, so):
        """
        somewhat ad-hoc/ugly API to add an existing socket 
        to the watch list (used by gxpc to add connection to gxpd).
        """
        if dbg>=3:
            LOG("so = %s\n" % so.fileno())
        ch = chan_unix_socket(self, so)
        return ch

    def create_redirect_pipes__(self, pipes):
        """
        pipes : (fd on child or None, "r" or "w", pipe, socket, or None)
        aux method for create_process.
        """
        if dbg>=3: LOG("pipes = %s\n" % pipes)
        r_channels = []
        w_channels = []
        fds_to_dup_by_child = []
        for kind,r_or_w,fd in pipes:
            if r_or_w == "r":
                ch_r,w = self.create_pipe_from_child_to_parent__(kind)
                if fd is None: fd = w
                ch_r.client_fd = fd    # indicate ch_r is connected to the child's fd
                r_channels.append(ch_r)
                fds_to_dup_by_child.append((w, fd, "w")) # child should write to fd
            elif r_or_w == "w":
                r,ch_w = self.create_pipe_from_parent_to_child__(kind)
                if fd is None: fd = r
                ch_w.client_fd = fd    # indicate ch_r is connected to the child's fd
                fds_to_dup_by_child.append((r, fd, "r")) # child should read from fd
                w_channels.append(ch_w)
            else:
                raise ioman_unix_bug()
        return r_channels,w_channels,fds_to_dup_by_child

    def dup_fds(self, fds_to_dup):
        """
        fds_to_dup : new_fd -> fd
        e.g., fds_to_dup[0] = 10 means currently open
        file descriptor should be renamed to 0.
        """
        # make a map old_fd -> new_fd
        M = {}
        read_fds = []
        write_fds = []
        for old_fd,new_fd,r_or_w in fds_to_dup:
            M[old_fd] = (new_fd, r_or_w)
        while len(M) > 0:
            x,(y,r_or_w) = M.popitem()
            if x != y:
                # now we like to remap x -> y,
                # but if y is used, we first need
                # to make its copy
                if M.has_key(y):
                    z = os.dup(y)
                    if dbg>=3:
                        LOG("os.dup(%d) -> %d\n" % (y, z))
                    assert (not M.has_key(z)), (fds_to_dup, M, y, z)
                    M[z] = M[y]
                    del M[y]
                if dbg>=1:
                    LOG("os.dup2(%d,%d); os.close(%d)\n" % (x, y, x))
                os.dup2(x, y)
                os.close(x)
            if r_or_w == "r":
                read_fds.append(y)
            elif r_or_w == "w":
                write_fds.append(y)
            else:
                raise ioman_unix_bug()
        # extend env
        os.environ["GXP_READ_FDS"] = string.join(map(str, read_fds), ",")
        os.environ["GXP_WRITE_FDS"] = string.join(map(str, write_fds), ",")

    def cd_to_existing_dir(self, cwds):
        if cwds is None: return None
        for d in cwds:
            if os.path.exists(d):
                os.chdir(d)
                return d
        return None

    def create_process(self, cmd, env, cwds, pipes, rlimit):
        """
        Create a new process
        """
        if dbg>=3:
            LOG("cmd = %s, env = %s, cwds = %s, pipes = %s, rlimit = %s\n" 
                % (cmd, env, cwds, pipes, rlimit))
        r_chans,w_chans,fds_to_dup = self.create_redirect_pipes__(pipes)
        pid = os.fork()
        if pid == 0:
            # change dir
            # set env
            self.dup_fds(fds_to_dup)
            self.cd_to_existing_dir(cwds)
            if env is not None:
                os.environ.update(env)
            os.execvp(cmd[0], cmd)
        else:
            for old_fd,_,_ in fds_to_dup:
                if dbg>=1:
                    LOG("os.close(%d)\n" % old_fd)
                os.close(old_fd)
            self.add_proc_to_watch__(pid)
        return pid,r_chans,w_chans,None

    def register_std_chans(self, idxs):
        if dbg>=3:
            LOG("idxs = %s\n" % idxs)
        ch_stdin = ch_stdout = ch_stderr = None
        if 0 in idxs:
            ch_stdin = chan_unix_fd_read(self, 0)
        if 1 in idxs:
            ch_stdout = chan_unix_fd_write(self, 1)
        if 2 in idxs:
            ch_stderr = chan_unix_fd_write(self, 2)
        return ch_stdin,ch_stdout,ch_stderr

    def pp_event__(self, event):
        s = []
        if event & select.POLLIN:
            s.append("IN")
        if event & select.POLLPRI:
            s.append("PRI")
        if event & select.POLLOUT:
            s.append("OUT")
        if event & select.POLLHUP:
            s.append("HUP")
        if event & select.POLLERR:
            s.append("ERR")
        if event & select.POLLNVAL:
            s.append("NVAL")
        return string.join(s, "|")

    def fill_pending__(self, to):
        """
        wait for the next event to happen.
        respect timeout specified on each channel
        """
        if dbg>=3: LOG("\n")
        if to is None or to == float("inf"):
            events = no_intr(self.po.poll, ())
        else:
            events = no_intr(self.po.poll, (int(to * 1000.0),))
        for fd,event in events:
            if dbg>=3:
                LOG("fd %d got event %s\n" % (fd, self.pp_event__(event)))
            ch = self.channels[fd]
            if event & self.events_error:
                assert (event & (self.events_out | self.events_in)), (fd, event)
            if event & self.events_out:
                pending_bytes,pending_close,err = ch.do_write()
                self.pending.append(ioman_base.event_write(ch, pending_bytes, pending_close, err))
            if event & (self.events_in | self.events_hup):
                if ch is self.child_watch:
                    assert (event & self.events_hup) == 0
                    assert (event & self.events_error) == 0
                    data,err = ch.do_read()
                    assert len(data) > 0
                    assert err is None
                    for pid,st in self.reap_children__():
                        self.pending.append(ioman_base.event_death(pid, st))
                elif isinstance(ch, chan_unix_server_socket):
                    assert (event & self.events_hup) == 0
                    assert (event & self.events_error) == 0
                    new_ch,err = ch.do_read()
                    self.pending.append(ioman_base.event_accept(ch, new_ch, err))
                elif event & self.events_hup:
                    # read until we get EOF or error
                    c = cStringIO.StringIO()
                    while 1:
                        data,err = ch.do_read()
                        # I thought hup means EOF, but for sockets, I got
                        # hup after connection reset by peers error
                        # assert (err is None), err
                        if err:
                            assert (data is None)
                            break
                        elif data == "": 
                            break
                        c.write(data)
                    data = c.getvalue()
                    eof = 1
                    self.pending.append(ioman_base.event_read(ch, data, eof, err))
                else:
                    data,err = ch.do_read()
                    if data == "" or err:
                        eof = 1
                    else:
                        eof = 0
                    self.pending.append(ioman_base.event_read(ch, data, eof, err))

    def get_my_fds(self):
        pid = os.getpid()
        procfile = "/proc/%d/fd" % pid
        files = []
        try:
            files = os.listdir(procfile)
        except OSError,e:
            if dbg>=1:
                LOG("could not get open file descriptors %s\n" 
                    % (e.args,))
        return files

    def next_event(self, timeout):
        if dbg>=3:
            fds = self.get_my_fds()
            LOG("%d open descriptors = %s\n" % (len(fds), fds))
        if len(self.pending) == 0:
            self.fill_pending__(timeout)
            if len(self.pending) == 0:
                return None
        ev = self.pending.pop(0)
        if dbg>=3:
            LOG("-> %s\n" % ev)
        return ev

