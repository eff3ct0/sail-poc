package com.poc.sail.intermediate

import com.poc.sail.SailSession
import org.apache.spark.sql.functions._

/** E05: Every join type - inner, left, right, full, cross, semi, anti */
object E05_AllJoinTypes extends App {
  val spark = SailSession()
  import spark.implicits._

  val employees = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("data/sample.csv")
    .withColumn("id", monotonically_increasing_id() + 1)

  val departments = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("data/departments.csv")

  val transactions = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("data/transactions.csv")

  println("=== INNER JOIN: employees + departments ===")
  employees.join(departments, employees("department") === departments("dept_name"))
    .select("name", "department", "salary", "budget")
    .show()

  println("=== LEFT JOIN: employees + transactions ===")
  employees.join(transactions, employees("id") === transactions("employee_id"), "left")
    .select("name", "tx_id", "amount", "category")
    .show()

  println("=== RIGHT JOIN ===")
  employees.join(transactions, employees("id") === transactions("employee_id"), "right")
    .select("name", "tx_id", "amount")
    .show()

  println("=== FULL OUTER JOIN ===")
  val extraDepts = Seq(("Research", 100000, 99, 6, "2024-01-01"))
    .toDF("dept_name", "budget", "manager_id", "floor", "created_date")
  val allDepts = departments.union(extraDepts)

  employees.join(allDepts, employees("department") === allDepts("dept_name"), "full")
    .select("name", "dept_name", "budget")
    .show()

  println("=== CROSS JOIN ===")
  val colors = Seq("red", "blue", "green").toDF("color")
  val sizes = Seq("S", "M", "L").toDF("size")
  colors.crossJoin(sizes).show()

  println("=== LEFT SEMI JOIN (employees who have transactions) ===")
  employees.join(transactions, employees("id") === transactions("employee_id"), "left_semi")
    .show()

  println("=== LEFT ANTI JOIN (employees with NO transactions) ===")
  employees.join(transactions, employees("id") === transactions("employee_id"), "left_anti")
    .show()

  spark.stop()
}
