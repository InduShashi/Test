--- VIDEO_Q1 ---

/* Problem Statement:
- For pairs of brands in the same year (e.g. apple/samsung/2020 and samsung/apple/2020) 
    - if custom1 = custom3 and custom2 = custom4 : then keep only one pair

- For pairs of brands in the same year 
    - if custom1 != custom3 OR custom2 != custom4 : then keep both pairs

- For brands that do not have pairs in the same year : keep those rows as well
*/
use sakila;
DROP TABLE IF EXISTS brands;
CREATE TABLE brands 
(
    brand1      VARCHAR(20),
    brand2      VARCHAR(20),
    year        INT,
    custom1     INT,
    custom2     INT,
    custom3     INT,
    custom4     INT
);
INSERT INTO brands VALUES ('apple', 'samsung', 2020, 1, 2, 1, 2);
INSERT INTO brands VALUES ('samsung', 'apple', 2020, 1, 2, 1, 2);
INSERT INTO brands VALUES ('apple', 'samsung', 2021, 1, 2, 5, 3);
INSERT INTO brands VALUES ('samsung', 'apple', 2021, 5, 3, 1, 2);
INSERT INTO brands VALUES ('google', NULL, 2020, 5, 9, NULL, NULL);
INSERT INTO brands VALUES ('oneplus', 'nothing', 2020, 5, 9, 6, 3);
SELECT * FROM brands;


use sakila;
create table sales(sale_id int, sale_date date, kitchen varchar(20), brand varchar(20), sales_amount INT);
DESC sales;
insert into sales values(1, '2023-05-01', "KitchenA", "BrandX", 150),
                         (2, '2023-05-01', "KitchenA", "BrandY", 200),
                         (3, '2023-05-02', "KitchenA", "BrandX", 170),
                         (4, '2023-05-02', "KitchenA", "BrandY", 190),
                         (5, '2023-05-01', "KitchenB", "BrandX", 300),
                         (6, '2023-05-02', "KitchenB", "BrandX", 350);
                     SELECT * FROM sales;
              
              
select kitchen, brand,sales_amount,
dense_rank() over(partition by kitchen order by sales_amount) 
from sales
where sale_date = '2023-05-02'
and brand='brandX';


select kitchen, brand,sales_amount,
dense_rank() over(partition by kitchen order by sales_amount) 
from sales
where sale_date = '2023-05-02'
and brand='brandX';


select kitchen, brand,sales_amount,               
	(select  *, 
    row_number() over(partition by kitchen order by sales_amount) as RN FROM sales) x 
    where x.RN<1;

                         
                         
                         
                         
                         
                         
                         
                         
                         
                         
                         
         -- one way ans                 
WITH PreviousDaySales AS (
    SELECT
        sale_date,
        kitchen,
        brand,
        sales_amount AS prev_day_sales
    FROM
        sales
    WHERE
        MONTH(sale_date) = 5  -- Specify the month here
),
CurrentDaySales AS (
    SELECT
        sale_date,
        kitchen,
        brand,
        sales_amount AS curr_day_sales
    FROM
        sales
    WHERE
        MONTH(sale_date) = 5  -- Specify the month here
)
SELECT
    curr.sale_date,
    curr.kitchen,
    curr.brand,
    curr.curr_day_sales,
    prev.prev_day_sales
FROM
    CurrentDaySales curr
JOIN
    PreviousDaySales prev ON curr.kitchen = prev.kitchen
    AND curr.brand = prev.brand
    AND DATE_SUB(curr.sale_date, INTERVAL 1 DAY) = prev.sale_date
WHERE
    curr.curr_day_sales > prev.prev_day_sales;
    
    



                         
create table employees(emp_id int, emp_name varchar(20),dept_id INT, salary varchar(20));
insert into employees values(1, 'John', 101,60000),
                         (2, 'Jane', 101,80000),
                         (3, 'Mary', 102,90000),
                         (4, 'Peter', 103,75000),
                         (5, 'Luke', 102,62000);
                         SELECT * FROM employees;
                         
create table department(dept_id int, dept_name varchar(20));
insert into department values(101, 'HR'),
                             (102, 'Engineering'),
                             (103, 'Marketing');
                         SELECT * FROM department;   
                         
SELECT * FROM employees; 
SELECT * FROM department; 








create table Orders(order_id int, customer_id int, product_id int, order_date date);
insert into Orders values(1, 1,101,'2023-01-01'),
                         (2, 1,101,'2023-01-15'),
                         (3, 1,102,'2023-01-01'),
                         (4, 2,101,'2023-01-01'),
                         (5, 2,102,'2023-01-11'),
                         (6, 2,102,'2023-01-15'),
                         (7, 3,103,'2023-01-01');
                         SELECT * FROM Orders;
                         
            select customer_id, customer_id 
		rank() over(partition by customer_id )
                         



