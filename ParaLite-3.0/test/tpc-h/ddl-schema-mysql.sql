create table Part(
	p_partkey INTEGER PRIMARY KEY, p_name varchar(100), p_mfgr varchar(100), p_brand varchar(100), p_type varchar(100), p_size INTEGER, p_container varchar(100), p_retailprice real, p_comment varchar(100));

create table Supplier(
	s_suppkey INTEGER PRIMARY KEY, s_name varchar(100), s_address varchar(100), s_nationkey INTEGER, s_phone varchar(100), s_acctbal REAL, s_comment varchar(100));

create table PartSupp(
	ps_partkey INTEGER, ps_suppkey INTEGER, ps_availqty  INTEGER, ps_supplycost REAL, ps_comment varchar(100));

create table Customer(
       c_custkey INTEGER PRIMARY KEY, c_name varchar(100), c_address varchar(100), c_nationkey INTEGER, c_phone varchar(100), c_acctbal REAL, c_mktsegment varchar(100), c_comment varchar(100));

create table Nation(
	n_nationkey  INTEGER PRIMARY KEY, n_name varchar(100), n_regionkey INTEGER, n_comment varchar(100));

create table Region(
	r_regionkey INTEGER PRIMARY KEY, r_name varchar(100), r_comment varchar(100));

create table LineItem(
	l_orderkey INTEGER, l_partkey INTEGER, l_suppkey INTEGER, l_linenumber INTEGER, l_quantity INTEGER, l_extendedprice REAL, l_discount REAL, l_tax REAL, l_returnflag varchar(100), l_linestatus varchar(100), l_shipdate varchar(100), l_commitdate varchar(100), l_receiptdate varchar(100), l_shipinstruct varchar(100), l_shipmode varchar(100), l_comment varchar(100));

create table O_Orders(
	o_orderkey INTEGER PRIMARY KEY, o_custkey INTEGER, o_orderstatus varchar(100), o_totalprice REAL, o_orderdate varchar(100), o_orderpriority varchar(100), o_clerk varchar(100), o_shippriority varchar(100), o_comment varchar(100));

