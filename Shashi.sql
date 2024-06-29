create database RailwayDetails;
use RailwayDetails;
create table RailwayType(Rail_Name varchar(80), Rail_Number int, Rail_Loctation varchar(80));
insert into RailwayType values("Vande Bharath Exprece", 1234, "Dharwad to Bengaluru");
insert into RailwayType values("Vande Exprece", 1247, "Bengaluru to Mangaluru");
insert into RailwayType values("Chanai Exprece", 12524, "Bengaluru to Chnnai");
insert into RailwayType values("Kolkatta Exprece", 123854, "Bengaluru to Kolkatta");
insert into RailwayType values("Neharu Exprece", 12374, "Bengaluru to Bihar");
insert into RailwayType values("RaniChannamma Exprece", 5634, "Dharwad to Hubli");
Select * from RailwayType;
select substr(Rail_Name,' ',1) as First_String,substr(Rail_Name,' ',-1) as Last_String from RailwayType;

-- Print First Name from the column 
select substr(Rail_Name,1,7) as extract from RailwayType;
select substr(Rail_Name," ", 1) as First_Name from RailwayType;
select substring_index(Rail_Name,' ',1) as First_Name from RailwayType;
select Rail_Name, substring_index(Rail_Name,' ',1) as First_Name, substring_index(Rail_Name,' ',-1) as Last_Name from RailwayType;
select substring_index(substring_index(Rail_Name,' ',2),' ',-1) as Middle_Name from RailwayType;

-- ADD NEW COLUMN 
Select * from RailwayType;
Alter table RailwayType add column Rail_Email varchar(30);
insert into RailwayType values (("Shashi@cognizant.com"),
                                 ("Sudha@cognizant.com"),
                                 ("Shweth@cognizant.com"),
                                 ("Prathiba@cognizant.com"));

Alter table RailwayType add column Rail_Time1 varchar(20);
set sql_safe_updates=1;
set sql_safe_updates=0;
Update RailwayType set Rail_Time1="11.30am" where Rail_Name = "Vande Bharath Exprece";

create table vegg(id int not null unique , name varchar(20));
insert into vegg values(1,"potato");
insert into vegg values(1,"potato");
insert into vegg values(1,"potato");
select * from vegg;
insert into vegg values(2,"chilly");
insert into vegg values(3,"tameto");

create table vegs(id int, name varchar(20));
insert into vegs values(1,"potato");
insert into vegs values(1,"potato");
insert into vegs values(1,"potato");
delete from vegs where id=1;
select * from vegs;
Alter table vegs modify column id int not null unique;
insert into vegs values(1,"potato");
insert into vegs values(1,"potato");
insert into vegs values(1,"potato");
insert into vegs values(2,"chilly");
insert into vegs values(3,"tameto");
insert into vegs values(4,"ladyfinger");

use RailwayDetails;
create table studentInfo (id int auto_increment primary key,s_name varchar(20),s_marks float);
insert into studentInfo(s_name,s_marks) values("santho",85.5);
insert into studentInfo(s_name,s_marks) values("Bharath",95.5);
insert into studentInfo(s_name,s_marks) values("swamy",85.5);
insert into studentInfo(s_name,s_marks) values("chethan",55.5);
insert into studentInfo(s_name,s_marks) values("ramesh",88.5);
insert into studentInfo(s_name,s_marks) values("ramu",75.5);
insert into studentInfo(s_name,s_marks) values("suresh",65.5);
insert into studentInfo(s_name,s_marks) values("ganesh",55.5);
select * from studentinfo;
select max(s_marks) from studentinfo;
select max(s_marks) from studentinfo where s_marks<95;
select * from studentinfo order by s_marks desc limit 1 offset 2;
select max(s_marks) from studentinfo;
SELECT id, s_name, s_marks FROM studentInfo WHERE 
s_marks = ( SELECT DISTINCT s_marks FROM studentInfo ORDER BY s_marks DESC LIMIT 1 OFFSET 3); 
select * from studentinfo order by s_marks desc limit 1 offset 2;

