--Unsupported!

select
        L.shipmode,
        sum(case
                when O.orderpriority = '1-URGENT'
                        or O.orderpriority = '2-HIGH'
                        then 1
                else 0
        end) as high_line_count,
        sum(case
                when O.orderpriority <> '1-URGENT'
                        and O.orderpriority <> '2-HIGH'
                        then 1
                else 0
        end) as low_line_count
from
        orders O,
        lineitem L
where
        O.orderkey = L.orderkey
        and L.shipmode in ('MAIL', 'AIR')
        and L.commitdate < L.receiptdate
        and L.shipdate < L.commitdate
        and L.receiptdate >= date('1995-10-11')
        and L.receiptdate < date('1995-10-11', '+1 year')
group by
        L.shipmode
order by
        L.shipmode
limit 100;
