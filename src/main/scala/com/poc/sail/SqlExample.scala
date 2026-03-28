package com.poc.sail

import org.apache.spark.sql.SparkSession

/**
 * Pure SQL example: register temp views and run SQL queries,
 * all executed by DataFusion through Sail.
 */
object SqlExample extends App {

  val sailHost = sys.env.getOrElse("SAIL_HOST", "localhost")
  val sailPort = sys.env.getOrElse("SAIL_PORT", "50051")

  val spark = SparkSession
    .builder()
    .remote(s"sc://$sailHost:$sailPort")
    .build()

  println("=== SQL Queries via Sail/DataFusion ===")

  import spark.implicits._

  // Create temp views
  val employees = Seq(
    (1, "Alice", "Engineering", 95000),
    (2, "Bob", "Marketing", 72000),
    (3, "Charlie", "Engineering", 110000),
    (4, "Diana", "Marketing", 68000),
    (5, "Eve", "Engineering", 105000),
    (6, "Frank", "Sales", 80000),
    (7, "Grace", "Sales", 77000)
  ).toDF("id", "name", "department", "salary")

  employees.createOrReplaceTempView("employees")

  // Query 1: department stats
  println("--- Department Statistics ---")
  spark.sql("""
    SELECT
      department,
      COUNT(*) as headcount,
      ROUND(AVG(salary), 2) as avg_salary,
      MIN(salary) as min_salary,
      MAX(salary) as max_salary
    FROM employees
    GROUP BY department
    ORDER BY avg_salary DESC
  """).show()

  // Query 2: window function - rank within department
  println("--- Salary Rank per Department ---")
  spark.sql("""
    SELECT
      name,
      department,
      salary,
      RANK() OVER (PARTITION BY department ORDER BY salary DESC) as dept_rank
    FROM employees
    ORDER BY department, dept_rank
  """).show()

  // Query 3: CTE
  println("--- Above Average Earners (CTE) ---")
  spark.sql("""
    WITH avg_sal AS (
      SELECT AVG(salary) as overall_avg FROM employees
    )
    SELECT e.name, e.department, e.salary, ROUND(a.overall_avg, 2) as company_avg
    FROM employees e
    CROSS JOIN avg_sal a
    WHERE e.salary > a.overall_avg
    ORDER BY e.salary DESC
  """).show()

  spark.stop()
  println("=== Done ===")
}
