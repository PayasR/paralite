from subprocess import *
import string, time, shlex, sys

def get_cpu_local(pid):
    try:
        core_num = get_core_num_local()
        cpu_total_time1, proc_total_time1 = get_tuple_local(pid)
        time.sleep(1)
        cpu_total_time2, proc_total_time2 = get_tuple_local(pid)
        cpu_usage = 100*(proc_total_time2 - proc_total_time1) / (cpu_total_time2 - cpu_total_time1) * core_num
        return cpu_usage
    except:
        return None

def get_tuple_local(pid):
    cpu_total_time = 0
    proc_total_time = 0
    proc_time = open('/proc/%s/stat' % (pid), 'rb').readline().split()
    cpu_time = open('/proc/stat', 'rb').readline().split()
    for i in range(1, len(cpu_time)):
        if cpu_time[i] == '':
            continue
        cpu_total_time += string.atoi(cpu_time[i])
    for i in range(13, 17):
        proc_total_time += string.atoi(proc_time[i])
    return cpu_total_time, proc_total_time

def get_core_num_local():
    f = open('/proc/cpuinfo', 'rb')
    num = 0
    while True:
        line = f.readline()
        if not line:
            break
        if line.startswith('processor'):
            num += 1
    return num

def get_cpu_remote(node, pid):
    core_num = get_core_num_remote(node)
    cpu_total_time1, proc_total_time1 = get_tuple_remote(node, pid)
    time.sleep(0.5)
    cpu_total_time2, proc_total_time2 = get_tuple_remote(node, pid)
    cpu_usage = 100*(proc_total_time2 - proc_total_time1) / (cpu_total_time2 - cpu_total_time1) * core_num
    return cpu_usage

def get_tuple_remote(node, pid):
    cpu_total_time = 0
    proc_total_time = 0
    args = shlex.split('ssh %s cat /proc/stat' % (node))
    p = Popen(args, stdout = PIPE)
    cpu_time = p.stdout.readline().split()
    p.stdout.close()
    args = shlex.split('ssh %s cat /proc/%s/stat' % (node, pid))
    p = Popen(args, stdout = PIPE)
    proc_time = p.stdout.readline().split()
    p.stdout.close()
    for i in range(1, len(cpu_time)):
        if cpu_time[i] == '':
            continue
        cpu_total_time += string.atoi(cpu_time[i])
    for i in range(13, 17):
        proc_total_time += string.atoi(proc_time[i])
    return cpu_total_time, proc_total_time

def get_core_num_remote(node):
    args = shlex.split('ssh %s cat /proc/cpuinfo' % (node))
    p = Popen(args, stdout = PIPE)
    num = 0
    for line in p.stdout:
        if line.startswith('processor'):
            num += 1
    p.stdout.close()
    return num

if __name__ == "__main__":
    pid = sys.argv[1]
    s = time.time()*1000
    print get_cpu_remote('huscs000', pid)
    e = time.time()*1000
    print e - s
