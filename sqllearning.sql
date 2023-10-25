-- SUM(value) OVER(PARTITION BY ORDER BY) --
-- what do you want to calculate? what do you want to partition by? is order important? --


select s.salesemployeeid, s.saledate, s.saleamount, 
    sum(saleamount) over(partition by s.salesemployeeid order by saledate) as running_total,
    cast(sum(saleamount) over(partition by s.salesemployeeid order by saledate) as float)/e.quota as percent_quota
from sales s 
join employees e
on s.SALESEMPLOYEEID = e.employeeid
order by s.salesemployeeid

-- AVG(value) OVER(PARTITION BY ORDER BY ROWS (current row/n preceding/n following)) -- 
SELECT o.customerid,o.orderid,sum(d.price) ordertotal,
round(avg(sum(d.price)) over(partition by customerid order by o.orderid rows between 2 preceding and current row),2) Movingavg
from Orders o
join ordersdishes od on o.orderid=od.orderid
join dishes d on od.dishid = d.DISHID
group by o.orderid
order by customerid

-- DENSE_RANK() OVER(PARTITION BY ORDER BY) --
-- RANK() and ROW() are similar --
SELECT firstname, lastname, weeklypay,department ,
dense_rank() over (partition by department order by weeklypay desc) as DeptRank
FROM Employees
order by department, deptrank

-- LEAD(value,row) over() --
select statusmovementid, subscriptionid, statusid, movementdate,
lead(MOVEMENTDATE,1) over(order by movementdate) as nextstatusmovementdate,
(lead(MOVEMENTDATE,1) over(partition by subscriptionid order by movementdate))-movementdate as timeinstatus
from paymentstatuslog 
where subscriptionid = '38844'

-- LAG(value, row) over() --
with cte as(
select o.orderid, sum(d.price) ThisOrderPrice, 
from orders o
join ordersdishes od
on o.orderid = od.ORDERID
join dishes d
on od.dishid = d.dishid
where orderdate >='2022-01-01'
group by o.ORDERID
)
select orderid, thisorderprice,
thisorderprice-lag(thisorderprice,1)over(order by orderid)as difffromprev
from cte