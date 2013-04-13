select * from Rankings

select pageRank from Rankings where pageRank > 1

select count(*) from Rankings

select pageRank, count(pageRank) from Rankings where pageRank = 1 group by pageRank

select Rankings.pageRank, UserVisits.destURL from Rankings, UserVisits where Rankings.pageRank = UserVisits.destURL and Rankings.pageRank > 1

select Rankings.pageRank, count(Rankings.pageRank) from Rankings, UserVisits where Rankings.pageRank = UserVisits.destURL group by Rankings.pageRank

select pageURL, F(pageRank) from Rankings where pageRank > 1 with F="grep a"

create table foo as select F(pageURL) as ss from Rankings where pageRank>1 with F="grep a" on huscs[[001-002]]

select * from foo order by id

select id, count(*) from foo group by id order by id
