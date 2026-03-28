package com.poc.sail

import org.apache.spark.sql.SparkSession

/**
 * Basic connection test: connect to Sail server via Spark Connect,
 * create a simple DataFrame and show it.
 */
object BasicConnection extends App {

  val sailHost = sys.env.getOrElse("SAIL_HOST", "localhost")
  val sailPort = sys.env.getOrElse("SAIL_PORT", "50051")

  val spark = SparkSession
    .builder()
    .remote(s"sc://$sailHost:$sailPort")
    .build()

  println("=== Connected to Sail via Spark Connect ===")
  println(s"Spark version reported: ${spark.version}")

  // 1. Simple range DataFrame
  println("\n--- Range DataFrame ---")
  val df = spark.range(10)
  df.show()

  // 2. SQL expression
  println("\n--- SQL Expression ---")
  spark.sql("SELECT 1 + 1 AS result").show()

  // 3. Create DataFrame from Seq and do transformations
  println("\n--- DataFrame Transformations ---")
  import spark.implicits._

  val data = Seq(
    ("Alice", 34, "Engineering"),
    ("Bob", 28, "Marketing"),
    ("Charlie", 45, "Engineering"),
    ("Diana", 31, "Marketing"),
    ("Eve", 27, "Engineering")
  ).toDF("name", "age", "department")

  data.show()

  println("--- Group by department ---")
  data
    .groupBy("department")
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
