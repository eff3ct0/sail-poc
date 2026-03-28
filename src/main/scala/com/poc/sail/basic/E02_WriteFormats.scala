package com.poc.sail.basic

import com.poc.sail.SailSession

/** E02: Write in multiple formats with different modes and options */
object E02_WriteFormats extends App {
  val spark = SailSession()
  import spark.implicits._

  val data = Seq(
    ("Alice", 34, 95000.0),
    ("Bob", 28, 72000.0),
    ("Charlie", 45, 110000.0)
  ).toDF("name", "age", "salary")

  // CSV with custom delimiter
  println("=== Write CSV (pipe-delimited) ===")
  data.write
    .mode("overwrite")
    .option("header", "true")
    .option("delimiter", "|")
    .csv("output/e02_pipe.csv")

  // JSON
  println("=== Write JSON ===")
  data.write.mode("overwrite").json("output/e02_data.json")

  // Parquet with snappy compression
  println("=== Write Parquet (snappy) ===")
  data.write
    .mode("overwrite")
    .option("compression", "snappy")
    .parquet("output/e02_data.parquet")

  // Partitioned write
  println("=== Partitioned Parquet ===")
  val partData = Seq(
    ("Alice", "Engineering", 95000),
    ("Bob", "Marketing", 72000),
    ("Charlie", "Engineering", 110000),
    ("Diana", "Marketing", 68000)
  ).toDF("name", "dept", "salary")

  partData.write
    .mode("overwrite")
    .partitionBy("dept")
    .parquet("output/e02_partitioned.parquet")

  // Read back partitioned
  val readBack = spark.read.parquet("output/e02_partitioned.parquet")
  readBack.show()
  println(s"Partitions discovered: ${readBack.inputFiles.mkString(", ")}")

  spark.stop()
}
