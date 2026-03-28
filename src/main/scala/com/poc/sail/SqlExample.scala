package com.poc.sail

/** Pure SQL example: register temp views and run SQL queries via Sail/DataFusion. */
object SqlExample extends App {
  val spark = SailSession()
  import spark.implicits._

  Seq(
    (1, "Alice", "Engineering", 95000),
    (2, "Bob", "Marketing", 72000),
    (3, "Charlie", "Engineering", 110000),
    (4, "Diana", "Marketing", 68000),
    (5, "Eve", "Engineering", 105000),
    (6, "Frank", "Sales", 80000),
    (7, "Grace", "Sales", 77000)
  ).toDF("id", "name", "department", "salary")
    .createOrReplaceTempView("employees")

  println("--- Department Statistics ---")
  spark.sql("""
    SELECT department, COUNT(*) as headcount,
      ROUND(AVG(salary), 2) as avg_salary, MIN(salary) as min_salary, MAX(salary) as max_salary
    FROM employees GROUP BY department ORDER BY avg_salary DESC
  """).show()

  println("--- Salary Rank per Department ---")
  spark.sql("""
    SELECT name, department, salary,
      RANK() OVER (PARTITION BY department ORDER BY salary DESC) as dept_rank
    FROM employees ORDER BY department, dept_rank
  """).show()

  println("--- Above Average Earners (CTE) ---")
  spark.sql("""
    WITH avg_sal AS (SELECT AVG(salary) as overall_avg FROM employees)
    SELECT e.name, e.department, e.salary, ROUND(a.overall_avg, 2) as company_avg
    FROM employees e CROSS JOIN avg_sal a
    WHERE e.salary > a.overall_avg ORDER BY e.salary DESC
  """).show()

  spark.stop()
}
