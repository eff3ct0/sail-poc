package com.poc.sail.advanced

import com.poc.sail.SailSession

/** E11: Complex SQL - correlated subqueries, lateral, EXISTS, IN, HAVING, QUALIFY */
object E11_ComplexSQL extends App {
  val spark = SailSession()
  import spark.implicits._

  // Setup tables
  Seq(
    (1, "Alice", "Engineering", 95000),
    (2, "Bob", "Marketing", 72000),
    (3, "Charlie", "Engineering", 110000),
    (4, "Diana", "Marketing", 68000),
    (5, "Eve", "Engineering", 105000),
    (6, "Frank", "Sales", 80000),
    (7, "Grace", "Sales", 77000),
    (8, "Hector", "Engineering", 92000),
    (9, "Irene", "HR", 65000),
    (10, "Juan", "HR", 70000)
  ).toDF("id", "name", "dept", "salary").createOrReplaceTempView("emp")

  spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("data/transactions.csv")
    .createOrReplaceTempView("tx")

  spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("data/departments.csv")
    .createOrReplaceTempView("dept")

  println("=== Correlated Subquery: employees earning above dept avg ===")
  spark.sql("""
    SELECT e.name, e.dept, e.salary,
      (SELECT ROUND(AVG(e2.salary), 2) FROM emp e2 WHERE e2.dept = e.dept) as dept_avg
    FROM emp e
    WHERE e.salary > (SELECT AVG(e2.salary) FROM emp e2 WHERE e2.dept = e.dept)
    ORDER BY e.dept, e.salary DESC
  """).show()

  println("=== EXISTS subquery ===")
  spark.sql("""
    SELECT e.name, e.dept
    FROM emp e
    WHERE EXISTS (
      SELECT 1 FROM tx t WHERE t.employee_id = e.id AND t.amount > 1000
    )
  """).show()

  println("=== NOT EXISTS ===")
  spark.sql("""
    SELECT e.name, e.dept
    FROM emp e
    WHERE NOT EXISTS (
      SELECT 1 FROM tx t WHERE t.employee_id = e.id
    )
  """).show()

  println("=== IN subquery ===")
  spark.sql("""
    SELECT name, dept, salary
    FROM emp
    WHERE dept IN (SELECT dept_name FROM dept WHERE budget > 150000)
    ORDER BY salary DESC
  """).show()

  println("=== Scalar subquery in SELECT ===")
  spark.sql("""
    SELECT
      e.name,
      e.salary,
      e.salary - (SELECT AVG(salary) FROM emp) as diff_from_avg,
      ROUND(e.salary * 100.0 / (SELECT SUM(salary) FROM emp), 2) as pct_of_total
    FROM emp e
    ORDER BY e.salary DESC
  """).show()

  println("=== HAVING with complex condition ===")
  spark.sql("""
    SELECT dept, COUNT(*) as cnt, ROUND(AVG(salary), 2) as avg_sal
    FROM emp
    GROUP BY dept
    HAVING COUNT(*) >= 2 AND AVG(salary) > 70000
    ORDER BY avg_sal DESC
  """).show()

  println("=== Nested aggregation: dept with max avg salary ===")
  spark.sql("""
    WITH dept_stats AS (
      SELECT dept, AVG(salary) as avg_salary, COUNT(*) as size
      FROM emp
      GROUP BY dept
    )
    SELECT * FROM dept_stats
    WHERE avg_salary = (SELECT MAX(avg_salary) FROM dept_stats)
  """).show()

  println("=== Multi-level CTE chain ===")
  spark.sql("""
    WITH
    emp_tx AS (
      SELECT e.id, e.name, e.dept, e.salary,
             COALESCE(SUM(t.amount), 0) as total_spent
      FROM emp e
      LEFT JOIN tx t ON e.id = t.employee_id
      GROUP BY e.id, e.name, e.dept, e.salary
    ),
    dept_summary AS (
      SELECT dept,
             SUM(salary) as dept_salary_cost,
             SUM(total_spent) as dept_expenses,
             COUNT(*) as headcount
      FROM emp_tx
      GROUP BY dept
    ),
    dept_efficiency AS (
      SELECT *,
             ROUND(dept_expenses / dept_salary_cost * 100, 2) as expense_ratio,
             ROUND(dept_salary_cost / headcount, 2) as avg_salary
      FROM dept_summary
    )
    SELECT * FROM dept_efficiency
    ORDER BY expense_ratio DESC
  """).show()

  spark.stop()
}
