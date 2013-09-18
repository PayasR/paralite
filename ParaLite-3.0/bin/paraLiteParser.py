import sys
import os
import traceback
import m_paraLite

# set the home direcotry of paraLite into system's path
sys.path.append(os.path.abspath(os.path.join(sys.path[0], os.path.pardir)))

from lib import newparser

def syntax_parser(query):
    try:
        parse_result = newparser.parse(query)
    except Exception, e:
        return "The syntax of the query is wrong, please check it!\n%s" % traceback.format_exc()
    if parse_result is None:
        return "The syntax of the query is wrong, please check it!"  
    return parse_result.dump()


def plan_parser(meta_db, query):
    return m_paraLite.ParaLiteMaster().do_analyze_cmd(meta_db, query)

def main(argv):
    if argv[3] == "--parse-syntax":
        print syntax_parser(argv[2])
    elif argv[3] == "--parse-plan":
        print plan_parser(argv[1], argv[2])
    else:
        print "Please enter the right command"


if __name__ == "__main__":
    if len(sys.argv) != 4:

        print """Usage: paraliteParser.py metadata_db SQL --parse-type
       parse-type: parse-syntax | parse-plan"""
        sys.exit(0)
        
    main(sys.argv)
