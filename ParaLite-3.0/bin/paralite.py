import paraLite

def connect(db):
    conn = connection(db)
    return conn

class connection:
    def __init__(self, db):
        self.db = db
        
    def cursor(self):
        cr = cursor()
        cr.db = self.db
        return cr

class cursor:
    def __init__(self):
        self.rowcount = 0
        self.db = None
        self.result = []
        
    def execute(self, query):
        argv = []
        argv.append("paralite")
        argv.append(self.db)
        argv.append(query)
        self.result = paraLite.paraLite().main(argv, 1)
        
    def fetchall(self):
        return self.result

    def fetchone(self):
        return self.result[0]


if __name__ == "__main__":
    con = connect("/home/ting/tmp/d.db")
    cr = con.cursor()
    cr.execute("drop table if exists x")
    cr.execute("create table if not exists x(a) on cloko[[000-001]]")
    cr.execute(".import /home/ting/tmp/test.dat x")
    cr.execute("select * from x")
    rs = cr.fetchall()
    print rs
