-- Tag orders based on if their ShipCountry is in North America.
-- For our purposes, this is only 'USA', 'Mexico', and 'Canada'\

-- Detail: You should print the Order Id, ShipCountry, and another column
-- that is either 'NorthAmerica' or 'OtherPlace' depending on the Ship Country.
-- Order by the primary key (Id) ascending
-- Return 20 rows starting Order Id 15445
-- Your output should look like:
-- 15445|France|OtherPlace

SELECT Id, ShipCountry, 
       CASE 
              WHEN ShipCountry IN ('USA', 'Mexico','Canada')
              THEN 'NorthAmerica'
              ELSE 'OtherPlace'
       END
FROM 'Order'
WHERE Id >= 15445
ORDER BY Id ASC
LIMIT 20;

-- 15464|Brazil|OtherPlace

sqlite> select id, ShipCountry, case when ShipCountry = 'USA' in ('USA', 'Mexico','Canada') then 'NorthAmerica' else 'OtherPlace' end from 'Order' where id >= 15445 order by id limit 20;

