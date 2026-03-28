package com.poc.sail.extreme

import com.poc.sail.SailSession

/**
 * E16: The kitchen sink - a single massive query combining:
 * Multiple CTEs, window functions, correlated subqueries,
 * CASE expressions, date functions, multiple joins, aggregations.
 *
 * Business question: "For each department, show the top spender,
 * their spending trend, how they compare to department average,
 * their rank company-wide, and a spending classification."
 */
object E16_MegaQuery extends App {
  val spark = SailSession()
  import spark.implicits._

  // Setup
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
    .option("header", "true").option("inferSchema", "true")
    .csv("data/transactions.csv")
    .createOrReplaceTempView("tx")

  spark.read
    .option("header", "true").option("inferSchema", "true")
    .csv("data/departments.csv")
    .createOrReplaceTempView("dept")

  println("=== THE MEGA QUERY ===")
  spark.sql("""
    WITH
    -- Step 1: Employee spending with monthly breakdown
    emp_spending AS (
      SELECT
        e.id, e.name, e.dept, e.salary,
        COALESCE(SUM(t.amount), 0) as total_spent,
        COUNT(t.tx_id) as num_transactions,
        MIN(t.tx_date) as first_tx,
        MAX(t.tx_date) as last_tx
      FROM emp e
      LEFT JOIN tx t ON e.id = t.employee_id
      GROUP BY e.id, e.name, e.dept, e.salary
    ),

    -- Step 2: Monthly spending per employee
    monthly_spending AS (
      SELECT
        t.employee_id,
        DATE_FORMAT(DATE(t.tx_date), 'yyyy-MM') as month,
        SUM(t.amount) as monthly_total,
        COUNT(*) as monthly_count
      FROM tx t
      GROUP BY t.employee_id, DATE_FORMAT(DATE(t.tx_date), 'yyyy-MM')
    ),

    -- Step 3: Spending trend (compare last month to first month)
    spending_trend AS (
      SELECT
        employee_id,
        FIRST_VALUE(monthly_total) OVER (PARTITION BY employee_id ORDER BY month) as first_month_spend,
        LAST_VALUE(monthly_total) OVER (
          PARTITION BY employee_id ORDER BY month
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) as last_month_spend,
        COUNT(*) OVER (PARTITION BY employee_id) as active_months
      FROM monthly_spending
    ),

    -- Step 4: Deduplicate trends
    trend_summary AS (
      SELECT DISTINCT
        employee_id,
        first_month_spend,
        last_month_spend,
        active_months,
        CASE
          WHEN last_month_spend > first_month_spend * 1.2 THEN 'INCREASING'
          WHEN last_month_spend < first_month_spend * 0.8 THEN 'DECREASING'
          ELSE 'STABLE'
        END as trend
      FROM spending_trend
    ),

    -- Step 5: Department averages
    dept_avgs AS (
      SELECT dept,
        ROUND(AVG(total_spent), 2) as dept_avg_spent,
        ROUND(AVG(salary), 2) as dept_avg_salary,
        SUM(total_spent) as dept_total_spent
      FROM emp_spending
      GROUP BY dept
    ),

    -- Step 6: Combine everything
    combined AS (
      SELECT
        es.*,
        da.dept_avg_spent,
        da.dept_avg_salary,
        da.dept_total_spent,
        d.budget as dept_budget,
        ts.first_month_spend,
        ts.last_month_spend,
        ts.active_months,
        COALESCE(ts.trend, 'NO_DATA') as spending_trend,

        -- Window functions
        RANK() OVER (ORDER BY es.total_spent DESC) as company_spending_rank,
        RANK() OVER (PARTITION BY es.dept ORDER BY es.total_spent DESC) as dept_spending_rank,
        ROUND(es.total_spent * 100.0 / NULLIF(da.dept_total_spent, 0), 2) as pct_of_dept_spend,
        ROUND(es.salary * 100.0 / da.dept_avg_salary, 2) as salary_vs_dept_avg_pct,

        -- Spending classification
        CASE
          WHEN es.total_spent = 0 THEN 'ZERO_SPEND'
          WHEN es.total_spent < 500 THEN 'LOW'
          WHEN es.total_spent < 2000 THEN 'MODERATE'
          WHEN es.total_spent < 5000 THEN 'HIGH'
          ELSE 'VERY_HIGH'
        END as spend_class,

        -- Budget utilization (individual spend vs dept budget share)
        CASE
          WHEN d.budget IS NOT NULL THEN
            ROUND(es.total_spent * 100.0 / (d.budget / COUNT(*) OVER (PARTITION BY es.dept)), 2)
          ELSE NULL
        END as budget_utilization_pct

      FROM emp_spending es
      JOIN dept_avgs da ON es.dept = da.dept
      LEFT JOIN dept d ON es.dept = d.dept_name
      LEFT JOIN trend_summary ts ON es.id = ts.employee_id
    )

    -- Final output: top spender per department with full context
    SELECT
      name,
      dept,
      salary,
      total_spent,
      num_transactions,
      spend_class,
      spending_trend,
      dept_spending_rank,
      company_spending_rank,
      ROUND(pct_of_dept_spend, 1) as pct_dept,
      ROUND(salary_vs_dept_avg_pct, 1) as sal_vs_avg,
      ROUND(budget_utilization_pct, 1) as budget_util
    FROM combined
    WHERE dept_spending_rank <= 2
    ORDER BY dept, dept_spending_rank
  """).show(truncate = false)

  spark.stop()
}
