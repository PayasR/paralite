import sys, config, string
from socket import *

"""
This class is used to distribute final result to clients,
but at this point, it does nothing.
"""
arguments = sys.argv[1]

class select:
    def __init__(self):
        self.id = None
        self.name = None
        self.source = []
        self.clients = []
        self.status = None
        self.cost_time = 0

    """
    # inform master the job status
    def inform_status(self):
        addr = (self.master_host, self.master_port)
        s = socket(AF_INET, SOCK_STREAM)
        s.connect(addr)
        msg = self.mk_job_status_msg()
        s.send(msg)
        s.close()

        
    def mk_job_status_msg(self):
        conf = config.config()
        dic = {}
        dic[conf.MESSAGE_TYPE] = conf.JOB_STATUS_INFO
        dic[conf.OP_NAME] = self.name
        dic[conf.OP_ID] = self.id
        dic[conf.OP_STATUS] = self.status
        dic[conf.COST_TIME] = self.cost_time
        return str(dic)
    """
    def parse_args(self, msg):
        s = string.replace(msg, '*', '\n')
        b1 = base64.decodestring(s)
        dic = cPickle.loads(b1)
        conf = config.config()
        self.id = dic[conf.ID]
        self.source = dic[conf.SOURCE]
        self.clients = dic[conf.CLINETS]
        self.name = dic[conf.NAME]
        
    def proc(self):
        self.parse_args(arguments)
        start = time.time()*1000
        data_manager = dm.dm()
        data_manager.send(self.source, self.clients)
        self.status = config.config().JOB_FINISH
        self.cost_time = time.time()*1000
        print '-------%s_%s cost time %d ms' % (self.name, self.id, (end-start))
        self.inform_status()

def main():
    proc = select()
    proc.proc()

if __name__=='__main__':
    main()
