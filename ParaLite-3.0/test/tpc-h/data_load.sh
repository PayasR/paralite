datapath=/home/ting/paraLite/trunk/test/tpc-h/data
db=/data/local/chenting/test/tpch.db
sf=0.001

paralite $db ".import $datapath/customer_sf$sf.tbl.sqlite Customer"
echo "Customer: finish"


paralite $db ".import $datapath/lineitem_sf$sf.tbl.sqlite LineItem"
echo "LineItem: finish"

paralite $db ".import $datapath/nation_sf$sf.tbl.sqlite Nation"
echo "Nation: finish"

paralite $db ".import $datapath/orders_sf$sf.tbl.sqlite Orders"
echo "Orders: finish"

paralite $db ".import $datapath/part_sf$sf.tbl.sqlite Part"
echo "Part: finish"

paralite $db ".import $datapath/partsupp_sf$sf.tbl.sqlite PartSupp"
echo "PartSupp: finish"

paralite $db ".import $datapath/region_sf$sf.tbl.sqlite Region"
echo "Region: finish"

paralite $db ".import $datapath/supplier_sf$sf.tbl.sqlite Supplier"
echo "Supplier: finish"