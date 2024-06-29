use test;
create table sales(sale_id int, sale_date date, kitchen varchar(20), brand varchar(20), sales_amount INT);
DESC sales;
insert into sales values(1, '2023-05-01', "KitchenA", "BrandX", 150),
                         (2, '2023-05-01', "KitchenA", "BrandY", 200),
                         (3, '2023-05-02', "KitchenA", "BrandX", 170),
                         (4, '2023-05-02', "KitchenA", "BrandY", 190),
                         (5, '2023-05-01', "KitchenB", "BrandX", 300),
                         (6, '2023-05-02', "KitchenB", "BrandX", 350);
                         SELECT * FROM sales;
                         
with PreviouseDaySales as (
select sale_date,kitchen,brand,sales_amount as pre_day_sales
from sales 
where month(sale_date) = 5),
CurrentDaySales as (
select sale_date,kitchen,brand,sales_amount as current_day_sales
from sales 
where month(sale_date) = 5)
select curr.sale_date,curr.kitchen,curr.brand,current_day_sales,pre_day_sales 
from CurrentDaySales curr join PreviouseDaySales pre 
on curr.kitchen=pre.kitchen and curr.brand=pre.brand
and date_sub(curr.sale_date, interval 1 day ) = pre.sale_date
where current_day_sales > pre_day_sales ;



WITH PreviousDaySales AS (SELECT sale_date,kitchen,brand,sales_amount AS prev_day_sales
    FROM sales
    WHERE MONTH(sale_date) = 5  -- Specify the month here
),
CurrentDaySales AS (SELECT sale_date,kitchen,brand,sales_amount AS curr_day_sales
    FROM sales
    WHERE MONTH(sale_date) = 5  -- Specify the month here
)
SELECT curr.sale_date,curr.kitchen,curr.brand,curr.curr_day_sales,prev.prev_day_sales
FROM CurrentDaySales curr
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
  
 
  select E.emp_id, E.salary, D.dept_name
  FROM employees E JOIN department D
  ON E.dept_id=D.dept_id where (E.dep_id, E.salary) 
  in (select dept_id, max(salary) from employees group by dept_id);

  
    
with RankedSalaries as(
  select E.emp_id, E.salary, D.dept_name, E.emp_name,
  row_number() over(partition by E.dept_id order by E.salary DESC) AS rn
  from employees e join
  department D on E.dept_id = d.dept_id)
  select dept_name,emp_id,salary 
  from 
  RankedSalaries where rn =1;
  
  
  

     
                         
                         
                         
		