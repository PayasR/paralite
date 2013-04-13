
import cStringIO,string,types

def pp_str(x):
    if type(x) is types.StringType:
        l = x.split("\n")
        if len(l) > 1 and len(l[1]) > 0:
            return "%s.. (%d bytes)" % (l[0][:100], len(x))
        elif len(x) > 20:
            return "%s.. (%d bytes)" % (x[:100], len(x))
        else:
            return string.strip(x)
    else:
        return "%s" % (x,)

def pp_(O, lev):
    if O is None or lev <= 0: return "%r" % O
    t = type(O)
    if t is types.ListType:
        s = cStringIO.StringIO()
        s.write("[")
        for x in O:
            s.write("%s, " % pp_(x, lev-1))
        s.write("]")
        return s.getvalue()
    elif t is types.DictType:
        s = cStringIO.StringIO()
        s.write("{\n")
        for x,y in O.items():
            s.write("%s : %s" % (pp_(x, lev-1), pp_(y, lev-1)))
        s.write("}")
        return s.getvalue()
    elif t is types.StringType:
        return pp_str(O)
    elif isinstance(t, Exception):
        return "%s" % (t.args,)
    elif t is types.InstanceType:
        s = cStringIO.StringIO()
        s.write("%s(" % O.__class__.__name__)
        for x,y in O.__dict__.items():
            s.write("%s = %s,\n" % (pp_(x, lev-1), pp_(y, lev-1)))
        s.write(")")
        return s.getvalue()
    else:
        return "%r" % O
        
def pp(O):
    return pp_(O, 3)
