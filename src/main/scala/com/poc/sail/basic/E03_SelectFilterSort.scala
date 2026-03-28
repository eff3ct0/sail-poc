package com.poc.sail.basic

import com.poc.sail.SailSession
import org.apache.spark.sql.functions._

/** E03: Select, filter, sort, limit, distinct, drop, rename */
object E03_SelectFilterSort extends App {
  val spark = SailSession()
  import spark.implicits._

  val df = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("data/sample.csv")

  println("=== Select columns ===")
  df.select("name", "salary").show()

  println("=== Select with expressions ===")
  df.select($"name", ($"salary" * 1.1).as("salary_with_raise")).show()

  println("=== Filter ===")
  df.filter($"salary" > 80000).show()
  df.where($"department" === "Engineering" && $"salary" >= 95000).show()

  println("=== Sort ===")
  df.orderBy($"salary".desc).show()
  df.sort($"department".asc, $"salary".desc).show()

  println("=== Limit ===")
  df.orderBy($"salary".desc).limit(3).show()

  println("=== Distinct ===")
  df.select("department").distinct().show()

  println("=== Drop column ===")
  df.drop("city").show()

  println("=== Rename column ===")
  df.withColumnRenamed("name", "employee_name").show()

  println("=== Add/Replace columns ===")
  df.withColumn("tax", $"salary" * 0.21)
    .withColumn("net_salary", $"salary" - $"salary" * 0.21)
    .show()

  spark.stop()
}
