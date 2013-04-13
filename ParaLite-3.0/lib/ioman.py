# 
# ioman.py
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
# Summary:
#
# This file provides an event driven loop involving
# pipes, sockets, and child processes.  The central API is
# mk_ioman() function, which creates and returns an instance 
# of ioman_unix or ioman_win class depending on the platform.
# It provides methods to create a server socket, create a child
# process, a method to wait for a next event. An event
# describes things such as "you got data from this pipe", "this write
# you issued completed", "this child process terminated", etc.
# Each such event is represented by an event object.
# 
# In order to provide portability across unix and windows and to allow
# a uniform handling of various underlying communicaiton medium (unix file 
# descriptors, sockets, windows handles), this file also provides 'channel'
# classes.
#
# A typical usage is:
#
#  import ioman
#  iom = ioman.mk_ioman()
#  io.create_server_socket()
#  io.create_process(["ls"], ...)
#  while 1:
#    ev = io.next_event()
#    somehow process event (ev)
#
# There are four basic event types:
#  event_read
#  event_accept
#  event_write
#  event_death
# They are defined in ioman_base module.
#
# - event_read is generated when you receive data from a channel
#   (a socket, pipe to a child process, etc.)
# - event_accept is generated when you get a connection to
#   a server socket you created by create_server_socket
# - event_write is generated when a write you requested (locally) 
#   completes
# - event_death is generated when a child process you careted by
#   create_process terminates.
#
# See document of ioman_base module for further details.
#

import sys

if sys.platform == "win32":
    import ioman_win
else:
    import ioman_unix

def mk_ioman():
    """
    make an instance of either ioman_win or ioman_unix,
    depending on your platform. either case, the object 
    implements ioman_base.ioman interface.  see documents
    of ioman_base for details.
    """
    if sys.platform == "win32":
        return ioman_win.ioman_win()
    else:
        return ioman_unix.ioman_unix()

#def proc_running(pid):
#    if sys.platform == "win32":
#        return ioman_win.proc_running_win(pid)
#    else:
#        return ioman_unix.proc_running_unix(pid)
