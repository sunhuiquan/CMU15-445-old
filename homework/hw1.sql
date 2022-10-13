q1: select CategoryName from Category order by CategoryName asc;

q2: select distinct ShipName, substr(ShipName,0,instr(ShipName,'-')) from 'Order' where ShipName like '%-%' order by shipName;

q3: select id, ShipCountry, case when ShipCountry = 'USA' in ('USA', 'Mexico','Canada') then 'NorthAmerica' else 'OtherPlace' end 
from 'Order' where id >= 15445 
order by id limit 20;

q4: select CompanyName, round(cast(cnt as double) * 100 / total, 2) as percent
from (select ShipVia, count(*) as cnt from 'Order' where ShippedDate > RequiredDate group by ShipVia) as a
inner join (select ShipVia, count(*) as total from 'Order' group by ShipVia) as b on a.ShipVia = b.ShipVia
inner join Shipper on a.ShipVia = id
order by percent desc;

q5: select CategoryName, count(*) as CategoryCnt, round(avg(UnitPrice),2), min(UnitPrice), max(UnitPrice), sum(UnitsOnOrder) from Product join Category on CategoryId = Category.id group by CategoryId having CategoryCnt > 10;

q6: // 比答案的写的麻烦，因为大部分 sql 语法不支持 select group by 外的非聚集属性，而 sqlite 不光可以而且还会根据聚集函数选择对应的那一个，如果多个对应则选择未定义。
with ta as (
	select Product.id as pid, ProductName, CompanyName, ContactName, OrderDate
	from Product
	inner join OrderDetail on Product.id = OrderDetail.ProductId
	inner join 'Order' on OrderDetail.OrderId = 'Order'.id
	inner join Customer on 'Order'.CustomerId = Customer.id
	where Discontinued == 1
),
tb as (
	select pid, min(OrderDate) as MinOrderTime from ta group by pid
),
tc as (
	select ProductName, CompanyName, ContactName, pid, OrderDate from ta
),
td as (
select * from tb inner join tc on tb.pid = tc.pid and tb.MinOrderTime = tc.OrderDate
)
select ProductName, CompanyName, ContactName from td order by ProductName asc;


q7: select Id, OrderDate, PrevDate, round(julianday(OrderDate) - julianday(PrevDate), 2) from (select Id, OrderDate, LAG(OrderDate, 1, 0) over (order by OrderDate asc) as PrevDate from 'Order' where CustomerId = 'BLONP' order by OrderDate asc limit 10);

q8:
with expenditure as (
	select ifnull(CompanyName, 'MISSING_NAME') as CompanyName, CustomerId, round(sum(Quantity * UnitPrice), 2) as TotalCost
	from 'Order'
	inner join OrderDetail on 'Order'.Id = OrderDetail.OrderId
	left join Customer on 'Order'.CustomerId = Customer.Id
	group by 'Order'.CustomerId
),
quartile AS (
    select *, ntile(4) over (order by TotalCost asc) as quar from expenditure
)
select CompanyName, CustomerId, TotalCost from quartile where quar = 1 order by TotalCost asc;

q9:
with tb as (
	select r.RegionDescription as rd, e.FirstName as fn, e.LastName as ln, e.BirthDate as bd, t.RegionId as trid
	from Employee e
	inner join EmployeeTerritory et on e.id = et.EmployeeId
	inner join Territory t on et.TerritoryId = t.Id
	inner join Region r on t.RegionId = r.Id
)
select distinct rd, fn, ln, mbd
from (select trid, max(bd) as mbd from tb group by tb.trid) as a
inner join (select rd, fn, ln, trid, bd from tb) as b on a.trid = b.trid and a.mbd = b.bd
order by b.trid;

q10:
with t1 as (
select o.id as id from 'Order' as o join Customer as c on o.customerId = c.id where c.CompanyName = 'Queen Cozinha' and date(o.OrderDate) = '2014-12-25'
),
t2 as (
select row_number() over (order by p.id asc) as seqnum, p.ProductName as name from Product as p
join (select ProductId from OrderDetail as od join t1 on od.OrderId = t1.id) as t2 on p.id = t2.ProductId
),
t3 as (
    select seqnum, name as name 
    from t2
    where seqnum = 1
    union all
    select t2.seqnum, t3.name || ', ' || t2.name
    from t2 
	join t3 on t2.seqnum = t3.seqnum + 1
)
select name from t3
order by seqnum desc limit 1;

