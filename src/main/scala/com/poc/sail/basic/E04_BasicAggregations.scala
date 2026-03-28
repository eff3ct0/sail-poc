package com.poc.sail.basic

import com.poc.sail.SailSession
import org.apache.spark.sql.functions._

/** E04: GroupBy and basic aggregate functions */
object E04_BasicAggregations extends App {
  val spark = SailSession()
  import spark.implicits._

  val df = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("data/sample.csv")

  println("=== Count, Sum, Avg, Min, Max ===")
  df.agg(
    count("*").as("total"),
    sum("salary").as("total_salary"),
    avg("salary").as("avg_salary"),
    min("salary").as("min_salary"),
    max("salary").as("max_salary")
  ).show()

  println("=== GroupBy department ===")
  df.groupBy("department")
    .agg(
      count("*").as("headcount"),
      round(avg("salary"), 2).as("avg_salary"),
      sum("salary").as("total_salary"),
      stddev("salary").as("salary_stddev")
    )
    .orderBy($"avg_salary".desc)
    .show()

  println("=== GroupBy multiple columns ===")
  df.groupBy("department", "city")
    .agg(count("*").as("count"))
    .orderBy("department", "city")
    .show()

  spark.stop()
}
