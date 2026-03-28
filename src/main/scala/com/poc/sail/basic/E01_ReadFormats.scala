package com.poc.sail.basic

import com.poc.sail.SailSession
import org.apache.spark.sql.SparkSession

/** E01: Read CSV, JSON, and Parquet formats */
object E01_ReadFormats extends App {
  val spark = SailSession()

  println("=== CSV Read ===")
  val csv = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("data/sample.csv")
  csv.show()
  csv.printSchema()

  println("=== JSON Read (nested) ===")
  val json = spark.read.json("data/employees.json")
  json.show(truncate = false)
  json.printSchema()

  println("=== Write Parquet then Read back ===")
  csv.write.mode("overwrite").parquet("output/e01_sample.parquet")
  val parquet = spark.read.parquet("output/e01_sample.parquet")
  parquet.show()

  spark.stop()
}
