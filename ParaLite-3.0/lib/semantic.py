import sys, traceback, re, time, string

"""
pyparsing module supports both python 2 and python 3
"""
_PY3 = sys.version_info[0] > 2
if _PY3:
    from pyparsing_py3 import *
else:
    from pyparsing_py2 import * 

import pyparsing_py2
pyparsing_py2.ParserElement.enablePackrat()
    
def change(tokens):
    return "'%s'" % (tokens[0])

LPAR,RPAR,COMMA,DQUOTE,SQUOTE,PIPE, EQUAL = map(Suppress,"(),\"'|=")

# keywords

(UNION, ALL, AND, OR, INTERSECT, EXCEPT, COLLATE, ASC, DESC, ON, 
USING, NATURAL, INNER, CROSS, LEFT, OUTER, JOIN, AS, INDEXED, NOT, 
SELECT, DISTINCT, FROM, WHERE, GROUP, BY,HAVING, ORDER, BY, LIMIT, 
OFFSET, EXPLAIN) = map(CaselessKeyword, """UNION, ALL, AND, OR, 
INTERSECT, EXCEPT, COLLATE, ASC, DESC, ON, USING, NATURAL, INNER, 
CROSS, LEFT, OUTER, JOIN, AS, INDEXED, NOT, SELECT, DISTINCT, FROM, WHERE, 
GROUP, BY, HAVING, ORDER, BY, LIMIT, OFFSET, EXPLAIN""".replace(",","").split())

(CAST, ISNULL, NOTNULL, NULL, IS, BETWEEN, ELSE, END, CASE, WHEN, THEN, 
EXISTS, COLLATE, IN, LIKE, GLOB, REGEXP, MATCH, ESCAPE, CURRENT_TIME, 
CURRENT_DATE, CURRENT_TIMESTAMP, DROP) = map(CaselessKeyword, """CAST, 
ISNULL, NOTNULL, NULL, IS, BETWEEN, ELSE, END, CASE, WHEN, THEN, 
EXISTS, COLLATE, IN, LIKE, GLOB, REGEXP, MATCH, ESCAPE, CURRENT_TIME, 
CURRENT_DATE, CURRENT_TIMESTAMP, DROP""".replace(",","").split())

(CONFLICT, ROLLBACK, ABORT, FAIL, IGNORE, REPLACE, REFERENCES, DELETE, 
CREATE, UPDATE, SET, DEFAULT, INITIALLY, IMMEDIATE, CASCADE, NO, 
ACTION, RESTRICT, DEFERRABLE, DEFERRED, CONSTRAINT, CHECK, UNIQUE, 
PRIMARY, KEY, AUTOINCREMENT, FOREIGN, TEMP, TEMPORARY, IF, TABLE, 
VIRTUAL, FTS3, FTS4, FTS4AUX, INDEX) = map(CaselessKeyword, 
"""CONFLICT, ROLLBACK, ABORT, FAIL, IGNORE, REPLACE, REFERENCES, 
DELETE, CREATE, UPDATE, SET, DEFAULT, INITIALLY, IMMEDIATE, CASCADE, 
NO, ACTION, RESTRICT, DEFERRABLE, DEFERRED, CONSTRAINT, CHECK, UNIQUE, 
PRIMARY, KEY, AUTOINCREMENT, FOREIGN, TEMP, TEMPORARY, IF, TABLE, 
VIRTUAL, FTS3, FTS4, FTS4AUX, INDEX""".replace(",","").split())

(INSERT, INTO, VALUES) = map(CaselessKeyword, 
"""INSERT, INTO, VALUES""".replace(",","").split())
(QUERY, PLAN, FILE) = map(CaselessKeyword, 
"""QUEYR, PLAN, FILE""".replace(",","").split())

(INPUT, OUTPUT, INPUT_ROW_DELIMITER, OUTPUT_ROW_DELIMITER, 
INPUT_COL_DELIMITER, INPUT_RECORD_DELIMITER, OUTPUT_COL_DELIMITER, 
OUTPUT_RECORD_DELIMITER, OUTPUT_COLUMN, STDIN, STDOUT, NEW_LINE, 
EMPTY_LINE, WITH, SPACE, PARTITION, REPLICA) = map(CaselessKeyword, 
"""INPUT, OUTPUT, INPUT_ROW_DELIMITER, OUTPUT_ROW_DELIMITER, 
INPUT_COL_DELIMITER, INPUT_RECORD_DELIMITER, OUTPUT_COL_DELIMITER, 
OUTPUT_RECORD_DELIMITER, OUTPUT_COLUMN, STDIN, STDOUT, NEW_LINE, 
EMPTY_LINE, WITH, SPACE, PARTITION, REPLICA""".replace(",", "").split())

keyword = Regex("(CREATE|TABLE|UNION|ALL|AND|OR|INTERSECT|EXCEPT|COLLATE|AS|DESC|ON|USING|NATURAL|INNER|CROSS|LEFT|OUTER|JOIN|ASC|INDEXED|NOT|SELECT|DISTINCT|FROM|WHERE|GROUP|BY|HAVING|ORDER|BY|LIMIT|OFFSET|EXPLAIN|CAST|ISNULL|NOTNULL|NULL|IS|BETWEEN|ELSE|END|CASE|WHEN|THEN|EXISTS|COLLATE|IN|LIKE|GLOB|REGEXP|MATCH|ESCAPE|CURRENT_TIME|CURRENT_DATE|CURRENT_TIMESTAMP|CONFLICT|ROLLBACK|ABORT|FAIL|IGNORE|REPLACE|REFERENCES|DELETE|UPDATE|SET|DEFAULT|INITIALLY|IMMEDIATE|CASCADE|NO|ACTION|RESTRICT|DEFERRABLE|DEFERRED|CONSTRAINT|CHECK|UNIQUE|PRIMARY|KEY|AUTOINCREMENT|FOREIGN|TEMP|TEMPORARY|IF|INSERT|INTO|VALUES|QUERY|PLAN|INPUT|OUTPUT|INPUT_ROW_DELIMITER|OUTPUT_ROW_DELIMITER|INPUT_COL_DELIMITER|INPUT_RECORD_DELIMITER|OUTPUT_COL_DELIMITER|OUTPUT_RECORD_DELIMITER|OUTPUT_COLUMN|STDIN|STDOUT|NEW_LINE|EMPTY_LINE|WITH|SPACE|VIRTUAL|FTS3|FTS4|FTS4AUX|INDEX|DROP|PARTITION|REPLICA|FILE)", re.IGNORECASE)

identifier = ~keyword + Word(alphas, alphanums+"_")
collation_name = ~keyword + Word(alphas, alphanums+"_")
collate_name = ~keyword + Word(alphas, alphanums+"_")
column_name = ~keyword + Word(alphas, alphanums+"_")
column_alias = ~keyword + Word(alphas, alphanums+"_")
table_name = ~keyword + Word(alphas, alphanums+"_")
table_alias = ~keyword + Word(alphas, alphanums+"_")
index_name = ~keyword + Word(alphas, alphanums+"_")
function_name = ~keyword + Word(alphas, alphanums+"_")
parameter_name = ~keyword + Word(alphas, alphanums+"_")
database_name = ~keyword + Word(alphas, alphanums+"_")
index_name = ~keyword + Word(alphas, alphanums+"_")
foreign_table = ~keyword + Word(alphas, alphanums+"_")
t_name = ~keyword + Word(alphas, alphanums+"_")
exe_name = ~keyword + Word(alphas, alphanums+"_")

command_line = ~keyword + Word("/" + "(" + alphas, alphanums + "{" + "}" + "+"+"<"+ "_" +"." + " "+ "/" + "-" + ";" + "'" + "(" + ")" + "&" + "=" + "$" + ":" + "|")
delimiter = ~keyword + Word("|" + "=" + ":" + "/" + "#" + "\\" + alphas, alphanums + "_" + ":" + "=" + "#" + "\\")
file_name = ~keyword + Word("/" + alphas, alphanums + "_" +"." + "/" + "-")
hash_key = ~keyword + Word(alphas, alphanums+"_")

# expression
time1 = time.time()
expr = Forward().setName("expression")
select_stmt = Forward().setName("select statement")
function = Forward().setName("function")           

integer = Regex(r"[+-]?\d+")
numeric_literal = Regex(r"\d+(\.\d*)?([eE][+-]?\d+)?")
string_literal = QuotedString("'")
string_literal.setParseAction(change)
blob_literal = Combine(oneOf("x X") + "'" + Word(hexnums) + "'")
blob_literal.setParseAction(change)
literal_value = ( numeric_literal | string_literal | blob_literal |
           NULL | CURRENT_TIME | CURRENT_DATE | CURRENT_TIMESTAMP )

bind_parameter = (
            Word("?",nums) |
            Combine(oneOf(": @ $") + parameter_name)
            )
            
name = oneOf("TEXT REAL INTEGER BLOB NULL DATE INT VARCHAR FLOAT text real integer blob null int varchar date float")

#function << function_name + LPAR + (function | delimitedList(Group(table_name 
#+ "."+column_name)|column_name| "*")("args")) + RPAR

function << function_name + LPAR + ("*" | Optional(DISTINCT) + delimitedList(expr)) + RPAR

expr_term = (Group(function)|
            (Group(database_name + "." + table_name + "." + column_name) |Group( table_name + "." +column_name) | column_name )|
             CAST + LPAR + expr + AS + name + RPAR |
             Optional(Optional(NOT) + EXISTS) + LPAR + select_stmt+ RPAR |
             literal_value |
             bind_parameter |
             identifier | 
             LPAR + expr + RPAR | 
             expr + (COLLATE + collation_name | 
                     Optional(NOT) + (LIKE | GLOB | REGEXP | MATCH ) +expr 
                     + Optional(ESCAPE + expr) |
                     (ISNULL | NOTNULL | NOT + NULL) |
                     IS + NOT + expr |
                     Optional (NOT) + 
                     IN + ((LPAR + Optional (select_stmt | 
                                            Group(delimitedList(expr)))+ RPAR) | 
                           Optional(database_name + "." )+ table_name)) |
             CASE + Optional(expr) + OneOrMore(WHEN + expr + THEN + expr) + Optional(ELSE + expr) + END 
             )
            
UNARY,BINARY,TERNARY=1,2,3
            
expr << operatorPrecedence(expr_term,
                    [
                    ((LPAR,RPAR),TERNARY, opAssoc.LEFT),
                    (oneOf('- + ~') | NOT, UNARY, opAssoc.RIGHT),
                    (oneOf('* / %'), BINARY, opAssoc.LEFT),
                    (oneOf('+ -'), BINARY, opAssoc.LEFT),
                    #(oneOf('<< >> < <= > >= == != <> ='), BINARY, opAssoc.LEFT, CheckJoin),
                    (oneOf('<< >> < <= > >= == != <> ='), BINARY, opAssoc.LEFT),
                    (IS | IN | LIKE | GLOB | MATCH | REGEXP, BINARY, opAssoc.LEFT),            
                    ((BETWEEN,AND) , TERNARY, opAssoc.LEFT),
                    (AND| '&&', BINARY, opAssoc.LEFT),
                    (OR | '||', BINARY, opAssoc.LEFT) 
                    ])
            
# select statement
compound_operator = (UNION + Optional(ALL) | INTERSECT | EXCEPT)("compound_op")
ordering_term = expr + Optional(COLLATE + collation_name) + Optional(ASC | DESC)
        
join_constraint = Group(ON + expr | USING + LPAR + Group(delimitedList(column_name)) + RPAR)
join_op = COMMA | (Optional(NATURAL) + Optional(INNER | CROSS | LEFT + OUTER | LEFT | OUTER) + JOIN)
          
join_source = Forward()
single_source = ((Group(database_name + "." + table_name) | table_name) + Optional(Optional(AS) + table_alias) + Optional(INDEXED + BY + index_name | NOT + INDEXED) | (LPAR + select_stmt + RPAR + Optional(Optional(AS) + table_alias)) | (LPAR + join_source + RPAR))

join_source << single_source + ZeroOrMore(Group(join_op + Group(single_source) + Optional(join_constraint)))
            
result_column = "*" | expr + AS + LPAR + Group(delimitedList(column_alias)) + RPAR | table_name + "." + "*"  | expr + Optional(Optional(AS) + column_alias) 

udx = exe_name("exe_name") + EQUAL + DQUOTE + command_line + DQUOTE + Optional(INPUT + (STDIN | (SQUOTE+ file_name + SQUOTE)))("input") + Optional(INPUT_ROW_DELIMITER + ((SQUOTE + delimiter + SQUOTE) | EMPTY_LINE | NEW_LINE )) + Optional(INPUT_COL_DELIMITER + (NULL | SPACE| (SQUOTE + delimiter + SQUOTE))) + Optional(INPUT_RECORD_DELIMITER + (NEW_LINE|EMPTY_LINE | (SQUOTE + delimiter + SQUOTE)))+ Optional(OUTPUT + (STDOUT | (SQUOTE+file_name + SQUOTE))) + Optional(OUTPUT_ROW_DELIMITER + (NEW_LINE | EMPTY_LINE | (SQUOTE +delimiter+ SQUOTE))) + Optional(OUTPUT_COL_DELIMITER + (NULL | SPACE | (SQUOTE + delimiter + SQUOTE))) + Optional(OUTPUT_RECORD_DELIMITER + (EMPTY_LINE | (SQUOTE + delimiter + SQUOTE))) 
#+ Optional(AS + LPAR + Group(delimitedList(Group(column_name + Optional(t_name)))) + RPAR)

select_core = (SELECT + Group(Optional(DISTINCT | ALL) + delimitedList(result_column))("select") + Optional(FROM + Group(join_source)("fromList")) + Optional(WHERE + expr("where")) + Optional(GROUP + BY + delimitedList(ordering_term)("group_by") + Optional(HAVING + expr("having"))) + Optional(WITH + ZeroOrMore(Group(udx))("udx")))

select_stmt << (select_core + ZeroOrMore(compound_operator + select_core) + Optional(ORDER + BY + delimitedList(ordering_term)("order_by")) + Optional(LIMIT + expr("limit") + Optional((OFFSET|COMMA) + expr ("offset"))))

#create statement

signed_number = Optional(oneOf('+ -')) + numeric_literal
            
type_name = OneOrMore(name) + Optional((LPAR+signed_number+RPAR)|(LPAR+signed_number+COMMA+signed_number))
            
conflict_clause = Optional(ON + CONFLICT + (ROLLBACK | ABORT | FAIL | IGNORE | REPLACE))
            
foreign_key_clause = (REFERENCES + foreign_table + Optional(LPAR + Group(delimitedList(column_name)) + RPAR) +  Optional(OneOrMore((ON+ (DELETE | UPDATE) + (SET + NULL | SET + DEFAULT | CASCADE | RESTRICT | NO + ACTION)) | (MATCH + name))) + Optional (Optional(NOT) + DEFERRABLE + Optional(INITIALLY + DEFERRED | INITIALLY + IMMEDIATE)))
            
column_constraint = Group((Optional(CONSTRAINT+name) + (foreign_key_clause | COLLATE + collate_name | (DEFAULT + (signed_number| literal_value |(LPAR + expr + RPAR))) | CHECK + LPAR + expr + RPAR | UNIQUE + conflict_clause | NOT+ NULL + conflict_clause | PRIMARY +KEY + Optional(ASC|DESC) + conflict_clause + Optional(AUTOINCREMENT))))
            
column_def = Group(column_name + Optional(type_name)) + ZeroOrMore(column_constraint)

indexed_column = column_name + Optional(COLLATE + collate_name) + Optional (ASC | DESC)
            
table_constraint = Optional(CONSTRAINT + name) + ((PRIMARY + KEY | UNIQUE) + LPAR + Group(delimitedList ( indexed_column))+ RPAR + conflict_clause | CHECK + LPAR + expr + RPAR | FOREIGN + KEY + LPAR + Group(delimitedList(column_name))+ RPAR + foreign_key_clause)

node = ~keyword + Word(printables)
num = numeric_literal
create_stmt = (CREATE + Optional(TEMP|TEMPORARY|VIRTUAL)("table_type") + TABLE + Optional(IF+NOT+EXISTS)  + Optional(database_name + ".") + table_name("table") + Optional(USING + (FTS3|FTS4|FTS4AUX))("fts") + ((LPAR + Group(delimitedList(column_def))("column") + ZeroOrMore(COMMA+table_constraint)+ RPAR)| (AS+Group(select_stmt)("select"))) + Optional(ON + (Group(OneOrMore(node))("node") | (FILE + node ("file")))) + Optional(PARTITION + BY + hash_key("hash_key"))+ Optional(REPLICA + num("replica_num")))

# create index statement

indexed_column = column_name + Optional(COLLATE + collation_name) + Optional(ASC|DESC)
create_index_stmt = (CREATE + Optional(UNIQUE) + INDEX + Optional(IF + NOT + EXISTS) + Optional(database_name + ".") + index_name("index") + ON + table_name("table") + LPAR + Group(delimitedList(indexed_column))("column") + RPAR)


#insert statement

insert_stmt = ((REPLACE| INSERT + Optional(OR+(ROLLBACK|ABORT|REPLACE|FAIL|IGNORE)))+INTO + Optional(database_name + ".")+ table_name("table")+(DEFAULT + VALUES | (Optional(LPAR + Group(delimitedList(column_name))("column")+ RPAR) + (select_stmt | VALUES + LPAR + Group(delimitedList(expr))("values") + RPAR))))
            
#update statement
            
qualified_table_name = Optional(database_name + ".") + table_name + Optional(INDEXED + BY + index_name | NOT + INDEXED)
update_stmt = (UPDATE + Optional(OR + (ROLLBACK | ABORT | REPLACE | FAIL | IGNORE)) + qualified_table_name("table") + SET + Group(delimitedList(column_name + "=" + expr))("column") + Optional(WHERE + expr) ("where"))

# drop statement

drop_stmt = DROP + TABLE +Optional(IF + EXISTS)+ Optional(database_name + ".") + table_name ("table")
#sql statement

sql_stmt = Optional(EXPLAIN + Optional(QUERY + PLAN)) + (select_stmt | create_stmt | insert_stmt | update_stmt | create_index_stmt | drop_stmt)


def parse(sql):
    try: 
        start = time.time()
        parseResult = (sql_stmt + stringEnd).parseString(sql)
        return parseResult
    except ParseException, pe:
        sys.stderr.write(traceback.format_exc())
        
def aaa(sql):
    print sql
"""    

class BinaryOperation(object):

    def __init__(self, t):
        self.op = t[0][1]
        self.operands = t[0][0::2]

class SearchAnd(BinaryOperation):
    def generateSetExpression(self):
        return "(%s)" % \
            " & ".join(oper.generateSetExpression() for oper in self.operands)
    def __repr__(self):
        return "AND:(%s)" % (",".join(str(oper) for oper in self.operands))

class SearchOr(BinaryOperation):
    def generateSetExpression(self):
        return "(%s)" % \
            " | ".join(oper.generateSetExpression() for oper in self.operands)
    def __repr__(self):
        return "OR:(%s)" % (",".join(str(oper) for oper in self.operands))

class CheckJoin(BinaryOperation):

    def __repr__(self):

        table1 = self.get_table("".join(self.operands[0]))
        table2 = self.get_table("".join(self.operands[1]))
        if table1 != table2 :
            return "JOIN:(%s)" % (self.op+":(%s)" % (",".join(str(oper) for oper in self.operands)))
        else:
            return self.operands[0] +self.op+ self.operands[1]

    def get_table(self,attribute):
        strs = attribute.split('.')
        if len(strs) == 1:
            return 'table'
        else:
            return strs[0]

"""
if __name__=="__main__":

    sql = "select F(a) from x with F=\"grep aaaa\""

    """
    print "Please enter a SQL > "
    sql = sys.stdin.readline()
    """

    parseResult = parse(sql)

    print '--------------------------'
    print parseResult.dump()
    print '--------------------------'

