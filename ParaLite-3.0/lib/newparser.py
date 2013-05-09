""" ParaLite Query Parser with Pyparsing
@version: 3.0
@author: Ting Chen
@see: http://pyparsing.wikispaces.com/
"""

import sys
import traceback
import re
import time
import string

from pyparsing_py2 import *
ParserElement.enablePackrat()

_PY3 = sys.version_info[0] > 2
if _PY3:
    from pyparsing_py3 import * 

# define symbols
LPAR,RPAR,COMMA,DQUOTE,SQUOTE,PIPE,EQUAL = map(Suppress,"(),\"'|=")

# keywords
(UNION, ALL, AND, OR, INTERSECT, EXCEPT, COLLATE, ASC, DESC, ON, 
USING, NATURAL, INNER, CROSS, LEFT, RIGHT, OUTER, JOIN, AS, INDEXED, NOT, 
SELECT, DISTINCT, FROM, WHERE, GROUP, BY,HAVING, ORDER, BY, LIMIT, 
OFFSET, EXPLAIN) = map(CaselessKeyword, """UNION, ALL, AND, OR, 
INTERSECT, EXCEPT, COLLATE, ASC, DESC, ON, USING, NATURAL, INNER, 
CROSS, LEFT, RIGHT, OUTER, JOIN, AS, INDEXED, NOT, SELECT, DISTINCT, FROM, WHERE, 
GROUP, BY, HAVING, ORDER, BY, LIMIT, OFFSET, EXPLAIN""".replace(",","").split())

(CAST, ISNULL, NOTNULL, NULL, IS, BETWEEN, ELSE, END, CASE, WHEN, THEN, 
EXISTS, COLLATE, IN, LIKE, GLOB, REGEXP, MATCH, ESCAPE, CURRENT_TIME, 
CURRENT_DATE, CURRENT_TIMESTAMP, DROP) = map(CaselessKeyword, """CAST, 
ISNULL, NOTNULL, NULL, IS, BETWEEN, ELSE, END, CASE, WHEN, THEN, 
EXISTS, COLLATE, IN, LIKE, GLOB, REGEXP, MATCH, ESCAPE, CURRENT_TIME, 
CURRENT_DATE, CURRENT_TIMESTAMP, DROP""".replace(",","").split())

(SUM, TOTAL, COUNT, MAX, MIN, AVG) = map(CaselessKeyword, """SUM, TOTAL,
COUNT, MAX, MIN, AVG""".replace(",","").split())

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
EMPTY_LINE, WITH, SPACE, PARTITION, REPLICA, CHUNK) = map(CaselessKeyword, 
"""INPUT, OUTPUT, INPUT_ROW_DELIMITER, OUTPUT_ROW_DELIMITER, 
INPUT_COL_DELIMITER, INPUT_RECORD_DELIMITER, OUTPUT_COL_DELIMITER, 
OUTPUT_RECORD_DELIMITER, OUTPUT_COLUMN, STDIN, STDOUT, NEW_LINE, 
EMPTY_LINE, WITH, SPACE, PARTITION, REPLICA, CHUNK""".replace(",", "").split())

keyword = Regex("CREATE[^a-zA-Z0-9_]+|CREATE$|TABLE[^a-zA-Z0-9_]+|TABLE$|UNION[^a-zA-Z0-9_]+|UNION$|ALL[^a-zA-Z0-9_]+|ALL$|AND[^a-zA-Z0-9_]+|AND$|OR[^a-zA-Z0-9_]+|OR$|INTERSECT[^a-zA-Z0-9_]+|INTERSECT$|EXCEPT[^a-zA-Z0-9_]+|EXCEPT$|COLLATE[^a-zA-Z0-9_]+|COLLATE$|AS[^a-zA-Z0-9_]+|AS$|DESC[^a-zA-Z0-9_]+|DESC$|ON[^a-zA-Z0-9_]+|ON$|USING[^a-zA-Z0-9_]+|USING$|NATURAL[^a-zA-Z0-9_]+|NATURAL$|INNER[^a-zA-Z0-9_]+|INNER$|CROSS[^a-zA-Z0-9_]+|CROSS$|LEFT[^a-zA-Z0-9_]+|LEFT$|RIGHT[^a-zA-Z0-9_]+|RIGHT$|OUTER[^a-zA-Z0-9_]+|OUTER$|JOIN[^a-zA-Z0-9_]+|JOIN$|ASC[^a-zA-Z0-9_]+|ASC$|INDEXED[^a-zA-Z0-9_]+|INDEXED$|NOT[^a-zA-Z0-9_]+|NOT$|SELECT[^a-zA-Z0-9_]+|SELECT$|DISTINCT[^a-zA-Z0-9_]+|DISTINCT$|FROM[^a-zA-Z0-9_]+|FROM$|WHERE[^a-zA-Z0-9_]+|WHERE$|GROUP[^a-zA-Z0-9_]+|GROUP$|BY[^a-zA-Z0-9_]+|BY$|HAVING[^a-zA-Z0-9_]+|HAVING$|ORDER[^a-zA-Z0-9_]+|ORDER$|BY[^a-zA-Z0-9_]+|BY$|LIMIT[^a-zA-Z0-9_]+|LIMIT$|OFFSET[^a-zA-Z0-9_]+|OFFSET$|EXPLAIN[^a-zA-Z0-9_]+|EXPLAIN$|CAST[^a-zA-Z0-9_]+|CAST$|ISNULL[^a-zA-Z0-9_]+|ISNULL$|NOTNULL[^a-zA-Z0-9_]+|NOTNULL$|NULL[^a-zA-Z0-9_]+|NULL$|IS[^a-zA-Z0-9_]+|IS$|BETWEEN[^a-zA-Z0-9_]+|BETWEEN$|ELS[^a-zA-Z0-9_]+|ELS$|END[^a-zA-Z0-9_]+|END$|CASE[^a-zA-Z0-9_]+|CASE$|WHEN[^a-zA-Z0-9_]+|WHEN$|THEN[^a-zA-Z0-9_]+|THEN$|EXISTS[^a-zA-Z0-9_]+|EXISTS$|COLLATE[^a-zA-Z0-9_]+|COLLATE$|IN[^a-zA-Z0-9_]+|IN$|LIKE[^a-zA-Z0-9_]+|LIKE$|GLOB[^a-zA-Z0-9_]+|GLOB$|REGEXP[^a-zA-Z0-9_]+|REGEXP$|MATCH[^a-zA-Z0-9_]+|MATCH$|ESCAPE[^a-zA-Z0-9_]+|ESCAPE$|CURRENT_TIME[^a-zA-Z0-9_]+|CURRENT_TIME$|CURRENT_DATE[^a-zA-Z0-9_]+|CURRENT_DATE$|CURRENT_TIMESTAMP[^a-zA-Z0-9_]+|CURRENT_TIMESTAMP$|CONFLICT[^a-zA-Z0-9_]+|CONFLICT$|ROLLBACK[^a-zA-Z0-9_]+|ROLLBACK$|ABORT[^a-zA-Z0-9_]+|ABORT$|FAIL[^a-zA-Z0-9_]+|FAIL$|IGNORE[^a-zA-Z0-9_]+|IGNORE$|REPLACE[^a-zA-Z0-9_]+|REPLACE$|REFERENCES[^a-zA-Z0-9_]+|REFERENCES$|DELETE[^a-zA-Z0-9_]+|DELETE$|UPDATE[^a-zA-Z0-9_]+|UPDATE$|SET[^a-zA-Z0-9_]+|SET$|DEFAULT[^a-zA-Z0-9_]+|DEFAULT$|INITIALLY[^a-zA-Z0-9_]+|INITIALLY$|IMMEDIATE[^a-zA-Z0-9_]+|IMMEDIATE$|CASCADE[^a-zA-Z0-9_]+|CASCADE$|NO[^a-zA-Z0-9_]+|NO$|ACTION[^a-zA-Z0-9_]+|ACTION$|RESTRICT[^a-zA-Z0-9_]+|RESTRICT$|DEFERRABLE[^a-zA-Z0-9_]+|DEFERRABLE$|DEFERRED[^a-zA-Z0-9_]+|DEFERRED$|CONSTRAINT[^a-zA-Z0-9_]+|CONSTRAINT$|CHECK[^a-zA-Z0-9_]+|CHECK$|UNIQUE[^a-zA-Z0-9_]+|UNIQUE$|PRIMARY[^a-zA-Z0-9_]+|PRIMARY$|KEY[^a-zA-Z0-9_]+|KEY$|AUTOINCREMENT[^a-zA-Z0-9_]+|AUTOINCREMENT$|FOREIGN[^a-zA-Z0-9_]+|FOREIGN$|TEMP[^a-zA-Z0-9_]+|TEMP$|TEMPORARY[^a-zA-Z0-9_]+|TEMPORARY$|IF[^a-zA-Z0-9_]+|IF$|INSERT[^a-zA-Z0-9_]+|INSERT$|INTO[^a-zA-Z0-9_]+|INTO$|VALUES[^a-zA-Z0-9_]+|VALUES$|QUERY[^a-zA-Z0-9_]+|QUERY$|PLAN[^a-zA-Z0-9_]+|PLAN$|INPUT[^a-zA-Z0-9_]+|INPUT$|OUTPUT[^a-zA-Z0-9_]+|OUTPUT$|INPUT_ROW_DELIMITER[^a-zA-Z0-9_]+|INPUT_ROW_DELIMITER$|OUTPUT_ROW_DELIMITER[^a-zA-Z0-9_]+|OUTPUT_ROW_DELIMITER$|INPUT_COL_DELIMITER[^a-zA-Z0-9_]+|INPUT_COL_DELIMITER$|INPUT_RECORD_DELIMITER[^a-zA-Z0-9_]+|INPUT_RECORD_DELIMITER$|OUTPUT_COL_DELIMITER[^a-zA-Z0-9_]+|OUTPUT_COL_DELIMITER$|OUTPUT_RECORD_DELIMITER[^a-zA-Z0-9_]+|OUTPUT_RECORD_DELIMITER$|OUTPUT_COLUMN[^a-zA-Z0-9_]+|OUTPUT_COLUMN$|STDIN[^a-zA-Z0-9_]+|STDIN$|STDOUT[^a-zA-Z0-9_]+|STDOUT$|NEW_LINE[^a-zA-Z0-9_]+|NEW_LINE$|EMPTY_LINE[^a-zA-Z0-9_]+|EMPTY_LINE$|WITH[^a-zA-Z0-9_]+|WITH$|SPACE[^a-zA-Z0-9_]+|SPACE$|VIRTUAL[^a-zA-Z0-9_]+|VIRTUAL$|FTS3[^a-zA-Z0-9_]+|FTS3$|FTS4[^a-zA-Z0-9_]+|FTS4$|FTS4AUX[^a-zA-Z0-9_]+|FTS4AUX$|INDEX[^a-zA-Z0-9_]+|INDEX$|DROP[^a-zA-Z0-9_]+|DROP$|PARTITION[^a-zA-Z0-9_]+|PARTITION$|REPLICA[^a-zA-Z0-9_]+|REPLICA$|CHUNK[^a-zA-Z0-9_]+|CHUNK$|FILE[^a-zA-Z0-9_]+|FILE$|SUM[^a-zA-Z0-9_]+|SUM$|TOTAL[^a-zA-Z0-9_]+|TOTAL$|COUNT[^a-zA-Z0-9_]+|COUNT$|MAX[^a-zA-Z0-9_]+|MAX$|MIN[^a-zA-Z0-9_]+|MIN$|AVG[^a-zA-Z0-9_]+|AVG$", re.I)

def change(tokens):
    return "'%s'" % (tokens[0])

def get_string(str, loc, tok):
    return "".join(tok)

def get_string_by_comma(str, loc, tok):
    return ",".join(tok)

def get_string_by_space(str, loc, tok):
    return " ".join(tok)

def lower_string(str, loc, tok):
    return "".join(tok).lower()

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
hash_key = ~keyword + Word(alphas, alphanums+"_")

node = ~keyword + Word("!" + alphas, alphanums + "[" + "]" + "-" + "!" + "+")

delimiter = ~keyword + Word(alphas + "#" + "=" + "@" + "%", alphanums + "#")

file_name = ~keyword + Word("/" + alphas, alphanums + "_" +"." + "/" + "-")
command_line = ~keyword + Word("/" + "(" + alphas, alphanums + "{" + "}" + "+"+"<"+ "_" +"." + " "+ "/" + "-" + ";" + "'" + "(" + ")" + "&" + "=" + "$" + ":" + "|")

integer = Regex(r"[+-]?\d+")
numeric_literal = Regex(r"\d+(\.\d*)?([eE][+-]?\d+)?")
string_literal = QuotedString("'") | QuotedString("\"")
string_literal.setParseAction(change)
blob_literal = Combine(oneOf("x X") + "'" + Word(hexnums) + "'")
blob_literal.setParseAction(change)
literal_value = ( numeric_literal | string_literal | blob_literal |
           NULL | CURRENT_TIME | CURRENT_DATE | CURRENT_TIMESTAMP )


plus  = Literal( "+" )
minus = Literal( "-" )
mult  = Literal( "*" )
div   = Literal( "/" )
lpar  = Literal( "(" )
rpar  = Literal( ")" )
S_COUNT = CaselessLiteral( "count(*)" )

op   = (plus | minus | mult | div)
# UDX
ufunc = ~keyword + Word(alphas, alphanums + '_' + '.')
# general aggregation function
gfunc = SUM | TOTAL | COUNT | AVG | MIN | MAX

e0 = ~keyword + Word(alphas + alphanums, alphanums + '_' + '.' + '\'')
#program_expr = CASE + Optional(e0) + OneOrMore(WHEN + (e0 | e0 + ) + THEN + e0) + Optional(ELSE + expr) + END 

# this is for UDX which may has more than one argument
ue1 = delimitedList(e0).setParseAction(get_string_by_comma)

e1 = e0  
e2 = e1 + ZeroOrMore(op + e1) 
e3 = lpar + e2 + rpar
e4 = ((gfunc.setParseAction(lower_string) + lpar + ZeroOrMore(e1 | e3) +
       ZeroOrMore(op + (e1 | e3)) + rpar).setParseAction(get_string)) | (
    ufunc + lpar + ZeroOrMore(ue1 | e3) +
      ZeroOrMore(op + (ue1 | e3)) + rpar).setParseAction(get_string) | S_COUNT

column_expr = ZeroOrMore(e4.setParseAction(get_string) | e3 | e1 ) + ZeroOrMore(op + ( e4.setParseAction(get_string)| e3 | e1 ))


print (column_expr+StringEnd()).parseString("0.2*avg(ae)") 

notequal     = Literal("!=")
notequal2    = Literal("<>")
larger       = Literal(">")
smaller      = Literal("<")
largerequal  = Literal(">=")
smallerequal = Literal("<=")

prediction_expr = column_expr + (EQUAL | notequal | notequal2 | larger | smaller | largerequal | smallerequal) + column_expr

#print prediction_expr.parseString("C.c_nationkey + 1 = S.s_nationkey ")

# expression for udx
function = Forward()
function << exe_name + LPAR + (function | delimitedList(Group(table_name + "."+column_name|column_name))) + RPAR
udx_expr = function

#print (udx_expr + stringEnd).parseString("F(a, b)")

# expression
expr = Forward().setName("expression")
select_stmt = Forward().setName("select statement")
function = Forward().setName("function")           

bind_parameter = (
            Word("?",nums) |
            Combine(oneOf(": @ $") + parameter_name)
            )
            
name = oneOf("""TEXT REAL INTEGER BLOB NULL DATE INT VARCHAR
FLOAT text real integer blob null int varchar date float""")

function << function_name + LPAR + ("*" | Optional(DISTINCT) + delimitedList(expr)) + RPAR

expr_term = (Group(function).setResultsName("coltype2", listAllMatches=True)|
            (Group(database_name + "." + table_name + "." + column_name) |
             Group( table_name + "." +column_name)| Group(column_name))|
             CAST + LPAR + expr + AS + name + RPAR |
             LPAR + Group(select_stmt) + RPAR |
             Group(Optional(NOT) + EXISTS + LPAR + Group(select_stmt) + RPAR) |
             delimitedList(literal_value) |
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
             CASE + Optional(expr) + OneOrMore(WHEN + expr + THEN + expr) +
             Optional(ELSE + expr) + END 
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
                    (IS | Optional(NOT) + IN | Optional(NOT) + LIKE | GLOB | MATCH | REGEXP, BINARY, opAssoc.LEFT),            
                    ((BETWEEN,AND) , TERNARY, opAssoc.LEFT),
                    (AND| '&&', BINARY, opAssoc.LEFT),
                    (OR | '||', BINARY, opAssoc.LEFT) 
                    ])


# select statement
######################################################################
compound_operator = (UNION + Optional(ALL) | INTERSECT | EXCEPT)("compound_op")

ordering_term = expr + Optional(COLLATE + collation_name) + Optional(ASC | DESC)
        
join_constraint = ON + expr | USING + LPAR + Group(delimitedList(column_name)) + RPAR
join_op = COMMA | (Optional(NATURAL) + (Optional(INNER | CROSS | LEFT + OUTER | LEFT |RIGHT + OUTER | RIGHT) + JOIN).setParseAction(get_string_by_space))
          
join_source = Forward()

single_source = ((Group(database_name + "." + table_name) | table_name) + Optional(Optional(AS) + table_alias) + Optional(INDEXED + BY + index_name | NOT + INDEXED) | (LPAR + Group(select_stmt) + RPAR + Optional(Optional(AS) + table_alias)) | (LPAR + join_source + RPAR))

join_source << Group(single_source) + ZeroOrMore(Group(join_op + Group(single_source) + Optional(join_constraint)))

#print (join_source + stringEnd).parseString("( select * from x)")

#result_column = Group(delimitedList("*" |  Group(column_expr) + Optional(AS + LPAR + Group(delimitedList(column_alias))) + RPAR | table_name + "." + "*"  | Group(S_COUNT | column_expr) + Optional(AS + column_alias)))

result_column = Group(delimitedList("*" | Group(S_COUNT) + Optional(AS + column_alias) | Group(column_expr) + Optional(AS + (LPAR + Group(delimitedList(column_alias)) + RPAR | column_alias)) | table_name + "." + "*"))


#print (result_column + stringEnd).parseString("sum(L.l_extendedprice * (1 - L.l_discount)) as re")

udx = exe_name("exe_name") + EQUAL + DQUOTE + command_line + DQUOTE + Optional(INPUT + (STDIN | (SQUOTE+ file_name + SQUOTE)))("input") + Optional(INPUT_ROW_DELIMITER + ((SQUOTE + delimiter + SQUOTE) | EMPTY_LINE | NEW_LINE )) + Optional(INPUT_COL_DELIMITER + (NULL | SPACE| (SQUOTE + delimiter + SQUOTE))) + Optional(INPUT_RECORD_DELIMITER + (NEW_LINE|EMPTY_LINE | (SQUOTE + delimiter + SQUOTE)))+ Optional(OUTPUT + (STDOUT | (SQUOTE+file_name + SQUOTE))) + Optional(OUTPUT_ROW_DELIMITER + (NEW_LINE | EMPTY_LINE | (SQUOTE +delimiter+ SQUOTE))) + Optional(OUTPUT_COL_DELIMITER + (NULL | SPACE | (SQUOTE + delimiter + SQUOTE))) + Optional(OUTPUT_RECORD_DELIMITER + (EMPTY_LINE | (SQUOTE + delimiter + SQUOTE))) 

select_core = (SELECT + Optional(DISTINCT | ALL)("select_type") + (result_column)("select") + Optional(FROM + Group(join_source)("fromList")) + Optional(WHERE + expr("where")) + Optional(GROUP + BY + Group(delimitedList(ordering_term))("group_by") + Optional(HAVING + expr("having"))) + Optional(WITH + ZeroOrMore(Group(udx))("udx")))

select_stmt << (select_core + ZeroOrMore(compound_operator + select_core) + Optional(ORDER + BY + Group(delimitedList(ordering_term))("order_by")) + Optional(LIMIT + expr("limit") + Optional((OFFSET|COMMA) + expr ("offset"))))


#create statement
signed_number = Optional(oneOf('+ -')) + numeric_literal
            
type_name = OneOrMore(name) + Optional((LPAR+signed_number+RPAR)|(LPAR+signed_number+COMMA+signed_number))
            
conflict_clause = Optional(ON + CONFLICT + (ROLLBACK | ABORT | FAIL | IGNORE | REPLACE))
            
foreign_key_clause = (REFERENCES + foreign_table + Optional(LPAR + Group(delimitedList(column_name)) + RPAR) +  Optional(OneOrMore((ON+ (DELETE | UPDATE) + (SET + NULL | SET + DEFAULT | CASCADE | RESTRICT | NO + ACTION)) | (MATCH + name))) + Optional (Optional(NOT) + DEFERRABLE + Optional(INITIALLY + DEFERRED | INITIALLY + IMMEDIATE)))
            
column_constraint = Group((Optional(CONSTRAINT+name) + (foreign_key_clause | COLLATE + collate_name | (DEFAULT + (signed_number| literal_value |(LPAR + expr + RPAR))) | CHECK + LPAR + expr + RPAR | UNIQUE + conflict_clause | NOT+ NULL + conflict_clause | PRIMARY +KEY + Optional(ASC|DESC) + conflict_clause + Optional(AUTOINCREMENT))))
            
column_def = Group(column_name + Optional(type_name)) + ZeroOrMore(column_constraint)

indexed_column = column_name + Optional(COLLATE + collate_name) + Optional (ASC | DESC)
            
table_constraint = Optional(CONSTRAINT + name) + ((PRIMARY + KEY | UNIQUE) + LPAR + Group(delimitedList ( indexed_column))+ RPAR + conflict_clause | CHECK + LPAR + expr + RPAR | FOREIGN + KEY + LPAR + Group(delimitedList(column_name))+ RPAR + foreign_key_clause)

create_stmt = (CREATE + Optional(TEMP|TEMPORARY|VIRTUAL)("table_type") + TABLE + Optional(IF+NOT+EXISTS)  + Optional(database_name + ".") + table_name("table") + Optional(USING + (FTS3|FTS4|FTS4AUX))("fts") + ((LPAR + Group(delimitedList(column_def))("column") + ZeroOrMore(COMMA+table_constraint)+ RPAR)| (AS+Group(select_stmt)("select"))) + Optional(ON + (Group(OneOrMore(node))("node") | (FILE + node ("file")))) + Optional(PARTITION + BY + delimitedList(hash_key)("hash_key"))+ Optional(CHUNK + numeric_literal("chunk_num")) + Optional(REPLICA + numeric_literal("replica_num")))

# create index statement
######################################################################
indexed_column = column_name + Optional(COLLATE + collation_name) + Optional(ASC|DESC)

create_index_stmt = (CREATE + Optional(UNIQUE) + INDEX + Optional(IF + NOT + EXISTS) + Optional(database_name + ".") + index_name("index") + ON + table_name("table") + LPAR + Group(delimitedList(indexed_column))("column") + RPAR)


#insert statement
######################################################################
insert_stmt = ((REPLACE| INSERT + Optional(OR+(ROLLBACK|ABORT|REPLACE|FAIL|IGNORE)))+INTO + Optional(database_name + ".")+ table_name("table")+(DEFAULT + VALUES | (Optional(LPAR + Group(delimitedList(column_name))("column")+ RPAR) + (select_stmt | VALUES + LPAR + Group(delimitedList(expr))("values") + RPAR))))
            
#update statement
######################################################################
qualified_table_name = Optional(database_name + ".") + table_name + Optional(INDEXED + BY + index_name | NOT + INDEXED)
update_stmt = (UPDATE + Optional(OR + (ROLLBACK | ABORT | REPLACE | FAIL | IGNORE)) + qualified_table_name("table") + SET + Group(delimitedList(column_name + "=" + expr))("column") + Optional(WHERE + expr) ("where"))

# drop statement
#######################################################################
drop_stmt = DROP + TABLE +Optional(IF + EXISTS)+ Optional(database_name + ".") + table_name ("table")

#sql statement
######################################################################
sql_stmt = Optional(EXPLAIN + Optional(QUERY + PLAN)) + (select_stmt | create_stmt | insert_stmt | update_stmt | create_index_stmt | drop_stmt)


def parse(sql):
    try: 
        start = time.time()
        parseResult = (sql_stmt + stringEnd).parseString(sql)
        return parseResult
    except ParseException, pe:
        sys.stderr.write(traceback.format_exc())

def parse_op(expression):
    parseResult = (bnf + stringEnd).parseString(expression)
    return parseResult.asList()

def parse_column_expr(expression):
    parseResult = (column_expr + stringEnd).parseString(expression)
    return parseResult.asList()

def parse_udx_expr(expression):
    parseResult = (udx_expr + stringEnd).parseString(expression)
    return parseResult.asList()

def parse_prediction_expr(expression):
    parseResult = (prediction_expr + stringEnd).parseString(expression)
    return parseResult.asList()

    
if __name__=="__main__":

    sql = "select a, x.a, sum(x.b), F(x.b) from x"
    parseResult = parse(sql)
    if parseResult:
        print '--------------------------'
        print parseResult.dump()
        print '--------------------------'





