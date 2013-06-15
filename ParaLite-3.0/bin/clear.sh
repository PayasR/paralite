#!/bin/bash

#prompt
PS1='\h:\w`gxpc prompt 2> /dev/null`$ '

function psgrep()
{
    if [ $# -lt 1 ]; then
        echo "usage : psgrep [regexp]..."
        return
    fi
    pat=".*"
    for p in "$@"; do
        pat="(?=.*$p)$pat"
    done
    ps -ef | perl -ne "print if /^$pat\$/" | egrep -v 'perl -ne print if /'
}

function pskill()
{
    if [ $# -lt 2 ]; then
        echo "usage : pskill [-SIGNAL] [regexp]..."
        return
    fi
    minus=`echo $1 | cut -c 1`
    if [ $minus != "-" ]; then
        echo "usage : pskill [-SIGNAL] [regexp]..."
        return
    fi
    sig=$1
    shift
    pat=".*"
    for p in "$@"; do
        pat="(?=.*$p)$pat"
    done
    ps -ef | perl -ne "print if /^$pat\$/" | egrep -v 'perl -ne print if /' | awk '{print $2}' | xargs kill $sig 2> /dev/null
}

function gpsgrep()
{
    if [ $# -lt 1 ]; then
        echo "usage : gpsgrep [regexp]..."
        return
    fi
    pat=".*"
    for p in "$@"; do
        pat="(?=.*$p)$pat"
    done
    gxpc e "ps -ef | perl -ne 'print if /^$pat\$/' | egrep -v 'perl -ne print if /' | egrep -v 'gxpc e' 2> /dev/null"
}

function gpskill()
{
    if [ $# -lt 2 ]; then
        echo "usage : gpskill [-SIGNAL] [regexp]..."
        return
    fi
    minus=`echo $1 | cut -c 1`
    if [ $minus != "-" ]; then
        echo "usage : gpskill [-SIGNAL] [regexp]..."
        return
    fi
    sig=$1
    shift
    pat=".*"
    for p in "$@"; do
        pat="(?=.*$p)$pat"
    done
    gxpc e "ps -ef | perl -ne 'print if /^$pat\$/' | egrep -v 'perl -ne print if /' | egrep -v 'gxpc e' | awk '{print \$2}' | xargs kill $sig 2> /dev/null"
}

pskill -9 python m_paraLite.py
gpskill -9 python udx.py
gpskill -9 python sql.py
gpskill -9 python join.py
gpskill -9 python group_by.py
gpskill -9 python order_by.py
gpskill -9 python aggregate.py
gpskill -9 python dload_server.py
gxpc e "rm -f /tmp/paralite-local-addr-*"
