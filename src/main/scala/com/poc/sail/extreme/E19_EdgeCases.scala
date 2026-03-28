package com.poc.sail.extreme

import com.poc.sail.SailSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * E19: Edge cases and stress tests designed to find Sail's limits.
 * Nulls, empty DataFrames, type coercion, large expressions,
 * deeply nested operations, column name edge cases.
 */
object E19_EdgeCases extends App {
  val spark = SailSession()
  import spark.implicits._

  println("=== Null handling ===")
  val nullDf = Seq(
    (Some(1), Some("a"), Some(10.0)),
    (Some(2), None, Some(20.0)),
    (None, Some("c"), None),
    (None, None, None)
  ).toDF("id", "name", "value")

  nullDf.show()
  nullDf.select(
    $"id",
    coalesce($"name", lit("UNKNOWN")).as("name_or_default"),
    $"value".isNull.as("is_null"),
    $"value".isNotNull.as("is_not_null"),
    nanvl($"value", lit(0.0)).as("nanvl_test"),
    nullif($"id", lit(2)).as("nullif_test"),
    nvl($"name", lit("N/A")).as("nvl_test")
  ).show()

  println("=== Null-safe equality (<=>) ===")
  val df1 = Seq(Some(1), None, Some(3)).toDF("a")
  val df2 = Seq(Some(1), None, Some(4)).toDF("b")
  df1.crossJoin(df2)
    .withColumn("eq", $"a" === $"b")
    .withColumn("null_safe_eq", $"a" <=> $"b")
    .show()

  println("=== Empty DataFrame operations ===")
  val empty = spark.emptyDataFrame
  println(s"Empty columns: ${empty.columns.mkString(", ")}")
  println(s"Empty count: ${empty.count()}")

  val emptyTyped = Seq.empty[(Int, String)].toDF("id", "name")
  emptyTyped.show()
  emptyTyped.groupBy("name").count().show()

  println("=== Type coercion stress test ===")
  spark.sql("""
    SELECT
      1 + 1.5 as int_plus_double,
      CAST('2024-01-15' AS DATE) as str_to_date,
      CAST(12345 AS STRING) as int_to_str,
      CAST('99.99' AS DOUBLE) as str_to_double,
      CAST(NULL AS INTEGER) as null_int,
      TYPEOF(1) as type_of_int,
      TYPEOF(1.0) as type_of_double,
      TYPEOF('hello') as type_of_string,
      TYPEOF(ARRAY(1,2,3)) as type_of_array,
      TYPEOF(MAP('a', 1)) as type_of_map
  """).show(truncate = false)

  println("=== Column names with special characters ===")
  val special = Seq((1, "a")).toDF("my column", "another-col")
  special.select($"`my column`", $"`another-col`").show()
  special.filter($"`my column`" === 1).show()

  println("=== Deeply chained transformations (20+ ops) ===")
  val chain = (1 to 100).map(i => (i, i * 10, i % 7, s"name_$i"))
    .toDF("id", "value", "group", "label")
    .filter($"id" > 5)
    .filter($"id" < 95)
    .withColumn("v2", $"value" * 2)
    .withColumn("v3", $"v2" + $"group")
    .withColumn("v4", when($"v3" > 1000, "high").otherwise("low"))
    .withColumn("v5", concat($"label", lit("_"), $"v4"))
    .withColumn("v6", length($"v5"))
    .withColumn("v7", $"v6" * $"id")
    .groupBy("v4")
    .agg(
      count("*").as("cnt"),
      sum("value").as("total"),
      avg("v3").as("avg_v3"),
      min("v7").as("min_v7"),
      max("v7").as("max_v7"),
      collect_list("group").as("groups")
    )
    .withColumn("groups_distinct", array_distinct($"groups"))
    .withColumn("num_groups", size($"groups_distinct"))
    .drop("groups")
    .orderBy($"cnt".desc)

  chain.show(truncate = false)
  println(s"Plan:\n${chain.queryExecution.toString().take(2000)}")

  println("=== CASE with many branches ===")
  spark.range(20).select(
    $"id",
    when($"id" === 0, "zero")
      .when($"id" === 1, "one")
      .when($"id" === 2, "two")
      .when($"id" === 3, "three")
      .when($"id" === 4, "four")
      .when($"id" === 5, "five")
      .when($"id" < 10, "single_digit")
      .when($"id" < 15, "low_teen")
      .otherwise("high")
      .as("label")
  ).show(20)

  println("=== Large IN list ===")
  val inList = (1 to 50).map(lit).reduce(_ || _)
  // Alternative with SQL
  val inValues = (1 to 50).mkString(", ")
  spark.sql(s"SELECT * FROM range(100) WHERE id IN ($inValues)").show(60)

  println("=== Multiple nested COALESCE and CASE ===")
  nullDf.select(
    coalesce(
      when($"id".isNotNull && $"name".isNotNull, concat($"id".cast("string"), lit(":"), $"name")),
      when($"id".isNotNull, $"id".cast("string")),
      when($"name".isNotNull, $"name"),
      lit("completely_null")
    ).as("best_identifier"),
    coalesce($"value", lit(0.0)).as("safe_value")
  ).show()

  spark.stop()
}
