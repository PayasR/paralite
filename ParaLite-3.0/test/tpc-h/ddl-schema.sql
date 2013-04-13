create table Part(p_partkey INTEGER PRIMARY KEY, p_name, p_mfgr, p_brand, p_type, p_size INTEGER, p_container, p_retailprice real, p_comment);

create table Supplier(s_suppkey INTEGER PRIMARY KEY, s_name, s_address, s_nationkey INTEGER, s_phone, s_acctbal REAL, s_comment);

create table PartSupp(ps_partkey INTEGER, ps_suppkey INTEGER, ps_availqty  INTEGER, ps_supplycost REAL, ps_comment);

create table Customer(c_custkey INTEGER PRIMARY KEY, c_name, c_address, c_nationkey INTEGER, c_phone, c_acctbal REAL, c_mktsegment, c_comment);

create table Nation(n_nationkey  INTEGER PRIMARY KEY, n_name, n_regionkey INTEGER, n_comment);

create table Region(r_regionkey INTEGER PRIMARY KEY, r_name, r_comment);

create table LineItem(l_orderkey INTEGER, l_partkey INTEGER, l_suppkey INTEGER, l_linenumber INTEGER, l_quantity INTEGER, l_extendedprice REAL, l_discount REAL, l_tax REAL, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment);

create table Orders(o_orderkey INTEGER PRIMARY KEY, o_custkey INTEGER, o_orderstatus, o_totalprice REAL, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment);

