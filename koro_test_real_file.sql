DROP TABLE if exists orders;
DROP TABLE if exists users;


CREATE TABLE users (
id INTEGER PRIMARY KEY,
firstname TEXT NOT NULL,
lastname TEXT NOT NULL,
email TEXT NOT NULL,
firstlogin DATE DEFAULT CURRENT_DATE);

CREATE TABLE orders (
id INTEGER PRIMARY KEY,
userid INTEGER,
invoiceamount REAL,
ordertime DATE DEFAULT CURRENT_DATE,
orderstatus INTEGER
);


select *
from users;

select *
from orders;

select * from users;

INSERT INTO users (firstname, lastname, email) VALUES('Max', 'Musterman', 'Mustermann@korodrogerie.de');
INSERT INTO users (firstname, lastname, email) VALUES('Max', 'Musterman', 'Mustermann@korodrogerie.de');
INSERT INTO users (firstname, lastname, email) VALUES('Lisa', 'Beispiel', 'Lisa@korodrogerie.de');
INSERT INTO users (firstname, lastname, email) VALUES('Louise', 'X', 'louise@korodrogerie.de');
INSERT INTO users (firstname, lastname, email) VALUES('Giovanni', 'Y', 'giovanni@korodrogerie.de');
INSERT INTO users (firstname, lastname, email) VALUES('angelo', 'Yangelo', 'angelo@korodrogerie.de');

INSERT INTO orders (userid, invoiceamount, orderstatus) VALUES(1, 67.00, 1);
INSERT INTO orders (userid, invoiceamount, orderstatus) VALUES(2, 45.00, 1);
INSERT INTO orders (userid, invoiceamount, orderstatus) VALUES(3, 25.56, 1);
INSERT INTO orders (userid, invoiceamount, orderstatus) VALUES(5, 30.98, 1);
INSERT INTO orders (userid, invoiceamount, orderstatus) VALUES(5, 55.72, 1);
INSERT INTO orders (userid, invoiceamount, orderstatus) VALUES(5, 55.72, 1);
INSERT INTO orders (userid, invoiceamount, orderstatus) VALUES(6, 55.99, 0);


UPDATE orders
SET ordertime = DATE(ordertime,'+1 months')
WHERE id = 6;
UPDATE orders
SET ordertime = DATE(ordertime,'+92 days')
WHERE id = 3;
UPDATE orders
SET ordertime = DATE(ordertime,'+3 months')
WHERE id = 7;
UPDATE users
SET firstlogin = DATE(firstlogin,'+92 days')
WHERE id = 2;
SELECT *
FROM orders;
SELECT *
FROM users;


-- TWO QUERRIES!
-- CONDITIONS:
-- email unique user; only once user made tx count as new user


-- Create Query for total orders per month; groupby month - adjust month on first of month
SELECT strftime('%Y-%m', ordertime) AS month, COUNT(ordertime) AS 'Total orders'
FROM users
JOIN orders
ON orders.userid = users.id
WHERE orderstatus = 1
GROUP BY DATE(ordertime, 'start of month');


-- Create Query for new customers per month; again group by month; EMAIL UNIQUE IDENTIFIER!!!!! USER CAN HAVE MULTIPLE UIDS!!!

-- first get the first order per user
SELECT email, min(orders.ordertime) AS 'firstshoppingdate'
FROM users
JOIN orders
ON orders.userid = users.id
WHERE orderstatus = 1
GROUP BY orders.userid;

--  perform the same grouping as in total orders per month

SELECT strftime('%Y-%m', firstshoppingdate) AS monthfirstshopping, COUNT(DISTINCT Firsttimecustomer.email) AS Newcustomers
FROM (SELECT email, min(orders.ordertime) AS 'firstshoppingdate'
FROM users
JOIN orders
ON orders.userid = users.id
WHERE orderstatus = 1
GROUP BY orders.userid) AS Firsttimecustomer
GROUP BY DATE(Firsttimecustomer.firstshoppingdate, 'start of month');


-- set the two queries together (UNION??)
-- UNION ONLY ADDS FIELDS UP!

-- JOIN THE QUERIES ON MONTH!
SELECT month, Newcustomers AS 'New customers', totalorderscompleted AS 'Total orders'
FROM
( SELECT strftime('%Y-%m-01', ordertime) AS month, COUNT(ordertime) AS totalorderscompleted
FROM users
JOIN orders
ON orders.userid = users.id
WHERE orderstatus = 1
GROUP BY DATE(ordertime, 'start of month')) AS totalorderscompleted

LEFT JOIN

( SELECT strftime('%Y-%m-01', firstshoppingdate) AS monthfirstshopping, COUNT(DISTINCT Firsttimecustomer.email) AS Newcustomers
FROM (SELECT email, min(orders.ordertime) AS 'firstshoppingdate'
FROM users
JOIN orders
ON orders.userid = users.id
WHERE orderstatus = 1
GROUP BY orders.userid) AS Firsttimecustomer
GROUP BY DATE(Firsttimecustomer.firstshoppingdate, 'start of month')) AS firstshop

ON firstshop.monthfirstshopping=totalorderscompleted.month;