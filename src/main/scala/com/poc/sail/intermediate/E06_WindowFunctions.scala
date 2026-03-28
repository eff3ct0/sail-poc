package com.poc.sail.intermediate

import com.poc.sail.SailSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/** E06: Window functions - rank, dense_rank, row_number, lag, lead, ntile, cumulative */
object E06_WindowFunctions extends App {
  val spark = SailSession()
  import spark.implicits._

  val df = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("data/sample.csv")

  val byDept = Window.partitionBy("department").orderBy($"salary".desc)
  val byDeptRows = Window.partitionBy("department").orderBy($"salary".desc)
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

  println("=== rank / dense_rank / row_number ===")
  df.select(
    $"name", $"department", $"salary",
    rank().over(byDept).as("rank"),
    dense_rank().over(byDept).as("dense_rank"),
    row_number().over(byDept).as("row_num")
  ).show()

  println("=== lag / lead ===")
  df.select(
    $"name", $"department", $"salary",
    lag("salary", 1).over(byDept).as("prev_salary"),
    lead("salary", 1).over(byDept).as("next_salary"),
    ($"salary" - lag("salary", 1).over(byDept)).as("diff_from_prev")
  ).show()

  println("=== ntile (quartiles within department) ===")
  df.select(
    $"name", $"department", $"salary",
    ntile(4).over(byDept).as("quartile")
  ).show()

  println("=== Cumulative sum / avg ===")
  df.select(
    $"name", $"department", $"salary",
    sum("salary").over(byDeptRows).as("cumulative_salary"),
    avg("salary").over(byDeptRows).as("running_avg"),
    count("*").over(byDeptRows).as("running_count")
  ).show()

  println("=== percent_rank / cume_dist ===")
  df.select(
    $"name", $"department", $"salary",
    round(percent_rank().over(byDept), 4).as("pct_rank"),
    round(cume_dist().over(byDept), 4).as("cume_dist")
  ).show()

  println("=== first_value / last_value ===")
  val byDeptFull = Window.partitionBy("department").orderBy($"salary".desc)
    .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
  df.select(
    $"name", $"department", $"salary",
    first("name").over(byDept).as("highest_paid"),
    last("name").over(byDeptFull).as("lowest_paid")
  ).show()

  spark.stop()
}
