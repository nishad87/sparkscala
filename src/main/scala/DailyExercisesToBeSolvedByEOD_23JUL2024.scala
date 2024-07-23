/*
Table :
  employees
emp_id name dept_id
1 Alice 101
2 Bob 102
3 Charlie NULL
4 David 103
5 Eve 104

departments
dept_id dept_name
  101 HR
  102 Engineering
  103 Marketing
  105 Sales
*/

/*
* CREATE TABLES AND INSERT SCRIPTS
*
* */


CREATE TABLE employees
(
  emp_id NUMBER,
  name VARCHAR2(255),
  dept_id NUMBER
);

CREATE TABLE departments
(
  dept_id NUMBER,
  dept_name VARCHAR2(255)
);


INSERT INTO employees VALUES (1, 'Alice', 101);
INSERT INTO employees VALUES(2, 'Bob', 102);
INSERT INTO employees VALUES(3, 'Charlie', NULL);
INSERT INTO employees VALUES(4, 'David', 103);
INSERT INTO employees VALUES(5,'Eve', 104);


INSERT INTO departments VALUES(101, 'HR');
INSERT INTO departments VALUES(102,'Engineering');
INSERT INTO departments VALUES(103, 'Marketing');
INSERT INTO departments VALUES(105,'Sales');

COMMIT;

/*
Coding Questions
  1. Inner Join Question
  o Write an SQL query to find the names of employees who work in the 'Engineering'
department.
*/


SELECT name
  FROM employees e INNER JOIN departments d
ON e.dept_id = d.dept_id
WHERE d.dept_name = 'Engineering';

Output :
  Name
----
Bob

/*
2. Left Join Question
  o Write an SQL query to list all employees and their respective department names. If an
  employee does not belong to any department, still include their name in the result.
  */

SELECT e.emp_id , e.name , e.dept_id
FROM employees e LEFT OUTER JOIN departments d
  ON e.dept_id = d.dept_id

Output :

EMP_ID  NAME  DEPT_ID
1 Alice 101
2 Bob 102
4 David 103
5 Eve 104
3 Charlie

/*
3. Right Join Question
  o Write an SQL query to find all departments and the names of employees who belong to
them. If a department does not have any employees, still include the department name
in the result.
*/



SELECT d.dept_name , e.name
FROM employees e RIGHT OUTER JOIN departments d
  ON e.dept_id = d.dept_id

Output :
  DEPT_NAME	    NAME
  HR	        Alice
  Engineering	Bob
  Marketing	  David
  Sales


/*
4. Full Join Question
  o Write an SQL query to list all employees and all departments. Include employees who do
  not belong to any department and departments that do not have any employees.

*/

SELECT e.name , d.dept_name
FROM employees e FULL OUTER JOIN departments d
ON e.dept_id = d.dept_id

Output :

NAME	       DEPT_NAME
Charlie
Eve
Bob	         Engineering
Alice	       HR
David	       Marketing
	           Sales

 /*
5. Self Join Question

  Given the employees and departments tables, write a SQL query to find pairs of
  employees who work in the same department.

*/


INSERT INTO employees VALUES (6, 'Nishad', 101);

SELECT e.name , e1.name
FROM employees e JOIN employees e1
  ON e.dept_id = e1.dept_id
AND e.name != e1.name
WHERE e.emp_id < e1.emp_id