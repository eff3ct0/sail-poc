package com.poc.sail

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Read a CSV file through Sail/DataFusion, do some transformations,
 * and write the result as Parquet.
 */
object CsvExample extends App {

  val sailHost = sys.env.getOrElse("SAIL_HOST", "localhost")
  val sailPort = sys.env.getOrElse("SAIL_PORT", "50051")

  val spark = SparkSession
    .builder()
    .remote(s"sc://$sailHost:$sailPort")
    .build()

  println("=== CSV -> Transform -> Parquet via Sail ===")

  // Read CSV
  val df = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("data/sample.csv")

  println("--- Raw data ---")
  df.show()
  df.printSchema()

  // Transformations
  val result = df
    .withColumn("name_upper", upper(col("name")))
    .withColumn("salary_eur", col("salary") * 0.92)
    .filter(col("salary") > 50000)
    .orderBy(col("salary").desc)

  println("--- Transformed data ---")
  result.show()

  // Write as Parquet
  result.write
    .mode("overwrite")
    .parquet("output/filtered_employees.parquet")

  println("=== Parquet written to output/filtered_employees.parquet ===")

  spark.stop()
}
