package com.poc.sail.extreme

import com.poc.sail.SailSession

/**
 * E13: Recursive CTE - hierarchy traversal, graph-like queries.
 * This is one of the most complex SQL features. DataFusion added
 * recursive CTE support relatively recently.
 */
object E13_RecursiveCTE extends App {
  val spark = SailSession()
  import spark.implicits._

  // Org hierarchy: employee -> manager
  Seq(
    (1, "CEO", null.asInstanceOf[java.lang.Integer]),
    (2, "VP Engineering", 1: java.lang.Integer),
    (3, "VP Sales", 1: java.lang.Integer),
    (4, "Director Backend", 2: java.lang.Integer),
    (5, "Director Frontend", 2: java.lang.Integer),
    (6, "Senior Dev A", 4: java.lang.Integer),
    (7, "Senior Dev B", 4: java.lang.Integer),
    (8, "Junior Dev", 6: java.lang.Integer),
    (9, "Sales Lead", 3: java.lang.Integer),
    (10, "Sales Rep", 9: java.lang.Integer)
  ).toDF("id", "title", "manager_id").createOrReplaceTempView("org")

  println("=== Recursive CTE: Full org tree with depth ===")
  spark.sql("""
    WITH RECURSIVE org_tree AS (
      -- Base case: CEO (no manager)
      SELECT id, title, manager_id, 0 as depth, title as path
      FROM org
      WHERE manager_id IS NULL

      UNION ALL

      -- Recursive case: employees who report to someone in the tree
      SELECT o.id, o.title, o.manager_id,
             t.depth + 1,
             CONCAT(t.path, ' > ', o.title)
      FROM org o
      JOIN org_tree t ON o.manager_id = t.id
    )
    SELECT id, title, depth, path
    FROM org_tree
    ORDER BY path
  """).show(truncate = false)

  println("=== Recursive CTE: Number sequence (Fibonacci) ===")
  spark.sql("""
    WITH RECURSIVE fib AS (
      SELECT 1 as n, CAST(0 AS BIGINT) as a, CAST(1 AS BIGINT) as b
      UNION ALL
      SELECT n + 1, b, a + b FROM fib WHERE n < 20
    )
    SELECT n, a as fibonacci_number FROM fib ORDER BY n
  """).show(25)

  println("=== Recursive CTE: Date series generation ===")
  spark.sql("""
    WITH RECURSIVE dates AS (
      SELECT DATE '2024-01-01' as dt
      UNION ALL
      SELECT DATE_ADD(dt, 1) FROM dates WHERE dt < DATE '2024-01-31'
    )
    SELECT dt, DAYOFWEEK(dt) as dow,
           CASE DAYOFWEEK(dt)
             WHEN 1 THEN 'Sun' WHEN 2 THEN 'Mon' WHEN 3 THEN 'Tue'
             WHEN 4 THEN 'Wed' WHEN 5 THEN 'Thu' WHEN 6 THEN 'Fri'
             WHEN 7 THEN 'Sat'
           END as day_name
    FROM dates
  """).show(35)

  spark.stop()
}
