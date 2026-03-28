package com.poc.sail

/**
 * Basic connection test: connect to Sail server via Spark Connect,
 * create a simple DataFrame and show it.
 */
object BasicConnection extends App {

  val spark = SailSession()
  import spark.implicits._

  println("=== Connected to Sail via Spark Connect ===")
  println(s"Spark version reported: ${spark.version}")

  // 1. Simple range DataFrame
  println("\n--- Range DataFrame ---")
  spark.range(10).show()

  // 2. SQL expression
  println("\n--- SQL Expression ---")
  spark.sql("SELECT 1 + 1 AS result").show()

  // 3. Create DataFrame from Seq and do transformations
  println("\n--- DataFrame Transformations ---")
  val data = Seq(
    ("Alice", 34, "Engineering"),
    ("Bob", 28, "Marketing"),
    ("Charlie", 45, "Engineering"),
    ("Diana", 31, "Marketing"),
    ("Eve", 27, "Engineering")
  ).toDF("name", "age", "department")

  data.show()

  println("--- Group by department ---")
  data.groupBy("department")
    .agg(
      org.apache.spark.sql.functions.count("*").as("count"),
      org.apache.spark.sql.functions.avg("age").as("avg_age")
    )
    .show()

  println("--- Filter age > 30 ---")
  data.filter($"age" > 30).show()

  spark.stop()
  println("=== Done ===")
}
