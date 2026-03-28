package com.poc.sail

import org.apache.spark.sql.functions._

/** Read a CSV file through Sail/DataFusion, transform, write as Parquet. */
object CsvExample extends App {
  val spark = SailSession()

  println("=== CSV -> Transform -> Parquet via Sail ===")

  val df = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("data/sample.csv")

  println("--- Raw data ---")
  df.show()
  df.printSchema()

  val result = df
    .withColumn("name_upper", upper(col("name")))
    .withColumn("salary_eur", col("salary") * 0.92)
    .filter(col("salary") > 50000)
    .orderBy(col("salary").desc)

  println("--- Transformed data ---")
  result.show()

  result.write.mode("overwrite").parquet("output/filtered_employees.parquet")
  println("=== Parquet written to output/filtered_employees.parquet ===")

  spark.stop()
}
