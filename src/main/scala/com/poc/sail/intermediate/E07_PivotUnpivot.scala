package com.poc.sail.intermediate

import com.poc.sail.SailSession
import org.apache.spark.sql.functions._

/** E07: Pivot and Unpivot (stack) operations */
object E07_PivotUnpivot extends App {
  val spark = SailSession()
  import spark.implicits._

  val tx = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("data/transactions.csv")

  val employees = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("data/sample.csv")
    .withColumn("id", monotonically_increasing_id() + 1)

  val joined = tx.join(employees, tx("employee_id") === employees("id"))
    .select($"name", $"category", $"amount")

  println("=== PIVOT: spending per employee per category ===")
  val pivoted = joined
    .groupBy("name")
    .pivot("category")
    .agg(round(sum("amount"), 2))
    .na.fill(0)
  pivoted.show()

  println("=== UNPIVOT (stack): back to long format ===")
  // Get category columns (all except name)
  val catCols = pivoted.columns.filter(_ != "name")
  val stackExpr = s"stack(${catCols.length}, ${catCols.map(c => s"'$c', `$c`").mkString(", ")}) as (category, total_amount)"
  val unpivoted = pivoted
    .selectExpr("name", stackExpr)
    .filter($"total_amount" > 0)
    .orderBy("name", "category")
  unpivoted.show(truncate = false)

  println("=== PIVOT: monthly spending summary ===")
  tx.withColumn("month", date_format(to_date($"tx_date"), "yyyy-MM"))
    .groupBy("category")
    .pivot("month")
    .agg(round(sum("amount"), 2))
    .na.fill(0)
    .orderBy("category")
    .show()

  spark.stop()
}
