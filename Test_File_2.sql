
SELECT * FROM sakila.customer;
use sakila;
select count(*) from customer;
select count(*) from customer group by store_id;
select store_id, count(*) from customer group by store_id;
desc customer;
select distinct(store_id), count(*)from customer group by store_id;
select concat(first_name ," " , last_name) as full_name from customer; 
ALTER TABLE customer ADD COLUMN Full_name varchar(50);
update customer set Full_name = concat(first_name , last_name);
set sql_safe_updates=1;
set sql_safe_updates=0;
 -- write the only left side after @ data from email column --
delete from customer where address_id = 8;
SELECT COUNT(*) FROM customer;
select length('shashikala');
select right('shashikala', 5);
select left('shashikala', 5);
select reverse('shashikala');
select position('i' in 'shashikala');
select email from customer;
SELECT * FROM sakila.customer;
select substring_index( email , '@' , 1) as newcolumn from customer;
alter table customer add column First_name1 varchar(25);
SELECT * FROM sakila.customer;
ALTER TABLE customer DROP COLUMN First_name2;
SELECT * FROM sakila.customer;
update customer set First_name1 = (substring_index( email , '@' , 1));
select substring_index( email , '@' , -1) as newcolumn from customer;
select substring_index( email , '@' , 1) as newcolumn from customer;
SELECT * from customer order by customer_id;
SELECT MAX(address_id) from customer;
SELECT Full_name, MAX(address_id) 
from customer 
group by Full_name;
SELECT * , MAX(address_id) over()
from customer;
SELECT * , MAX(address_id) over(partition by store_id)
from customer;
SELECT * FROM sakila.customer;
select concat('&', substring(last_name,1),'&') as st1_Index from customer order by customer_id; 
select concat(first_name,'','(', substring(last_name,1,1),')') as st1_Index from customer order by customer_id; 
select concat('(', substring(last_name,1,1),')',first_name) as st1_Index from customer order by customer_id; 

-- How to count the dublicates in coliumn --
select store_id, count(*) as a from customer group by store_id having count(*) >1;
SELECT * FROM sakila.customer;  
select first_name,max(store_id) from customer group by first_name having first_name < (select max(store_id) from customer);
select max(store_id) from customer where store_id < (select max(store_id) from customer);

  --  2nd height salary
  select * from
  (select row_number() over(partition by first_name order by customer_id desc) as rn from customer ) x 
  where x.rn >0 ; 
 
   select * from
  (select row_number() over(partition by first_name order by customer_id desc) as rn from customer ) x 
  where x.rn >1; 
  
  select last_name, email from
  (select row_number() over(partition by first_name order by customer_id desc) as rn from customer ) x
where x.rn >1;
  