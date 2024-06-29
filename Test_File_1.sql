CREATE DATABASE College;
use College;
Create table Student1(S_ID INT, Course varchar(20), Email_Id varchar(50));
insert into Student1 values(1, "xyz","XYZ@SDBDGJS.COM");
insert into Student1 values(2, "ABC","ABC@SDBDGJS.COM");
insert into Student1 values(3, "DEF","DEF@SDBDGJS.COM");
insert into Student1 values(4, "ZXY","ZXY@SDBDGJS.COM");

Create table Prifeser1(F_ID INT, Course varchar(20), S_ID INT);
insert into Prifeser1 values(1,"XYZ",1);
insert into Prifeser1 values(2,"ABC",2);
insert into Prifeser1 values(3,"DEF",5);
insert into Prifeser1 values(4,"ZXY",6);


SELECT * FROM Prifeser1;
SELECT * FROM Student1;
-- I WANT all the student details and respected faculties, if thestudent not having the faculty and i should be shoew null?
select F_ID from Prifeser1 GROUP BY F_ID IN (select * From Student1);
SELECT Student1.S_ID,Student1.Course,Student1.Email_Id, Prifeser1.F_ID FROM Student1 inner join Prifeser1 ON Student1_S_ID=Prifeser1_S_ID;
SELECT Student1.S_ID, Student1.Course,Student1.Email_Id, Prifeser1.F_ID FROM Student1 Student1 left join Prifeser1 Prifeser1 ON Student1.S_ID=Prifeser1.S_ID;
SELECT Student1.S_ID FROM Student1 Student1;



-- Suppose you have a table named 'example' with a column 'text_column'
CREATE TABLE example (id INT,text_column VARCHAR(50) );

-- Insert some data into the table
INSERT INTO example (id, text_column) VALUES
(1, 'Hello, World!'),
(2, 'SQL is powerful');

-- Use SUBSTRING to extract a substring
SELECT id, text_column, SUBSTRING(text_column, 1, 5) AS substring_result
FROM example;

select text_column, substring_index(text_column,' ',1) as firstname,substring_index(text_column,' ',-1) as lastname from example;

select substr(text_column,1,5) as extract from example;

