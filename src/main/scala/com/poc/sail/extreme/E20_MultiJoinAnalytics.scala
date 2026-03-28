package com.poc.sail.extreme

import com.poc.sail.SailSession

/**
 * E20: Multi-table join analytics with self-joins, inequality joins,
 * and complex business logic. The most "real-world complex" query.
 */
object E20_MultiJoinAnalytics extends App {
  val spark = SailSession()
  import spark.implicits._

  // Setup all tables
  Seq(
    (1, "Alice", "Engineering", 95000, "2020-03-15"),
    (2, "Bob", "Marketing", 72000, "2019-06-01"),
    (3, "Charlie", "Engineering", 110000, "2018-01-10"),
    (4, "Diana", "Marketing", 68000, "2021-09-01"),
    (5, "Eve", "Engineering", 105000, "2019-11-20"),
    (6, "Frank", "Sales", 80000, "2020-07-01"),
    (7, "Grace", "Sales", 77000, "2022-01-15"),
    (8, "Hector", "Engineering", 92000, "2021-03-01"),
    (9, "Irene", "HR", 65000, "2023-06-01"),
    (10, "Juan", "HR", 70000, "2022-08-15")
  ).toDF("id", "name", "dept", "salary", "hire_date")
    .createOrReplaceTempView("emp")

  spark.read.option("header", "true").option("inferSchema", "true")
    .csv("data/transactions.csv").createOrReplaceTempView("tx")

  spark.read.option("header", "true").option("inferSchema", "true")
    .csv("data/departments.csv").createOrReplaceTempView("dept")

  // Salary history (simulated)
  Seq(
    (1, 85000, "2020-03-15"), (1, 90000, "2021-04-01"), (1, 95000, "2023-01-01"),
    (3, 90000, "2018-01-10"), (3, 100000, "2019-06-01"), (3, 110000, "2021-01-01"),
    (5, 95000, "2019-11-20"), (5, 100000, "2021-01-01"), (5, 105000, "2022-06-01"),
    (6, 70000, "2020-07-01"), (6, 75000, "2021-07-01"), (6, 80000, "2023-01-01")
  ).toDF("emp_id", "salary", "effective_date")
    .createOrReplaceTempView("salary_history")

  println("=== Self-join: find employees earning more than their department manager ===")
  spark.sql("""
    SELECT
      e.name as employee,
      e.dept,
      e.salary as emp_salary,
      mgr.name as manager,
      mgr.salary as mgr_salary,
      e.salary - mgr.salary as salary_diff
    FROM emp e
    JOIN dept d ON e.dept = d.dept_name
    JOIN emp mgr ON d.manager_id = mgr.id
    WHERE e.id != mgr.id AND e.salary > mgr.salary
    ORDER BY salary_diff DESC
  """).show()

  println("=== Inequality join: find all salary increases > 5% ===")
  spark.sql("""
    SELECT
      e.name,
      sh1.salary as old_salary,
      sh2.salary as new_salary,
      sh1.effective_date as from_date,
      sh2.effective_date as to_date,
      ROUND((sh2.salary - sh1.salary) * 100.0 / sh1.salary, 2) as pct_increase
    FROM salary_history sh1
    JOIN salary_history sh2
      ON sh1.emp_id = sh2.emp_id
      AND sh2.effective_date > sh1.effective_date
    JOIN emp e ON e.id = sh1.emp_id
    -- Only consecutive raises (no intermediate date between sh1 and sh2)
    WHERE NOT EXISTS (
      SELECT 1 FROM salary_history sh3
      WHERE sh3.emp_id = sh1.emp_id
        AND sh3.effective_date > sh1.effective_date
        AND sh3.effective_date < sh2.effective_date
    )
    AND (sh2.salary - sh1.salary) * 100.0 / sh1.salary > 5
    ORDER BY pct_increase DESC
  """).show()

  println("=== Peer comparison: each employee vs peers hired in same year ===")
  spark.sql("""
    WITH emp_years AS (
      SELECT *, YEAR(DATE(hire_date)) as hire_year
      FROM emp
    ),
    peer_stats AS (
      SELECT
        hire_year,
        AVG(salary) as peer_avg,
        MIN(salary) as peer_min,
        MAX(salary) as peer_max,
        COUNT(*) as peer_count
      FROM emp_years
      GROUP BY hire_year
    )
    SELECT
      e.name, e.dept, e.salary, e.hire_year,
      p.peer_count,
      ROUND(p.peer_avg, 2) as peer_avg_salary,
      e.salary - CAST(p.peer_avg AS INT) as vs_peer_avg,
      CASE
        WHEN e.salary = p.peer_max AND p.peer_count > 1 THEN 'TOP_OF_COHORT'
        WHEN e.salary = p.peer_min AND p.peer_count > 1 THEN 'BOTTOM_OF_COHORT'
        WHEN e.salary > p.peer_avg THEN 'ABOVE_AVG'
        ELSE 'BELOW_AVG'
      END as cohort_position
    FROM emp_years e
    JOIN peer_stats p ON e.hire_year = p.hire_year
    ORDER BY e.hire_year, e.salary DESC
  """).show()

  println("=== Grand analytics: 6-table join with window + aggregation ===")
  spark.sql("""
    WITH
    emp_full AS (
      SELECT e.*,
        d.budget, d.manager_id,
        DATEDIFF(DATE '2024-06-01', DATE(e.hire_date)) as tenure_days,
        ROUND(DATEDIFF(DATE '2024-06-01', DATE(e.hire_date)) / 365.25, 1) as tenure_years
      FROM emp e
      LEFT JOIN dept d ON e.dept = d.dept_name
    ),
    emp_spend AS (
      SELECT
        t.employee_id,
        COUNT(*) as tx_count,
        ROUND(SUM(t.amount), 2) as total_spent,
        COUNT(DISTINCT t.category) as categories_used,
        MIN(t.tx_date) as first_tx,
        MAX(t.tx_date) as last_tx
      FROM tx t
      GROUP BY t.employee_id
    ),
    salary_growth AS (
      SELECT
        emp_id,
        MIN(salary) as starting_salary,
        MAX(salary) as current_salary,
        COUNT(*) as num_raises,
        ROUND((MAX(salary) - MIN(salary)) * 100.0 / MIN(salary), 2) as total_growth_pct
      FROM salary_history
      GROUP BY emp_id
    )
    SELECT
      ef.name,
      ef.dept,
      ef.salary,
      ef.tenure_years,
      COALESCE(es.tx_count, 0) as transactions,
      COALESCE(es.total_spent, 0) as total_spent,
      COALESCE(sg.num_raises, 0) as raises,
      COALESCE(sg.total_growth_pct, 0) as salary_growth_pct,
      ROUND(ef.salary / NULLIF(ef.tenure_years, 0), 0) as salary_per_year_tenure,
      ROUND(COALESCE(es.total_spent, 0) / NULLIF(ef.tenure_years, 0), 2) as spend_per_year,
      RANK() OVER (ORDER BY ef.salary DESC) as salary_rank,
      RANK() OVER (ORDER BY COALESCE(es.total_spent, 0) DESC) as spend_rank,
      RANK() OVER (ORDER BY ef.tenure_years DESC) as tenure_rank,
      ROUND(
        0.4 * PERCENT_RANK() OVER (ORDER BY ef.salary) +
        0.3 * PERCENT_RANK() OVER (ORDER BY ef.tenure_years) +
        0.3 * PERCENT_RANK() OVER (ORDER BY COALESCE(sg.total_growth_pct, 0)),
        3
      ) as composite_score
    FROM emp_full ef
    LEFT JOIN emp_spend es ON ef.id = es.employee_id
    LEFT JOIN salary_growth sg ON ef.id = sg.emp_id
    ORDER BY composite_score DESC
  """).show(truncate = false)

  spark.stop()
}
