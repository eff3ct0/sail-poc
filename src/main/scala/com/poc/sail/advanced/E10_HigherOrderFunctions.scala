package com.poc.sail.advanced

import com.poc.sail.SailSession
import org.apache.spark.sql.functions._

/** E10: Higher-order functions on arrays - transform, filter, aggregate, exists, forall */
object E10_HigherOrderFunctions extends App {
  val spark = SailSession()
  import spark.implicits._

  val df = Seq(
    (1, "Alice", Seq(10, 20, 30, 40, 50)),
    (2, "Bob", Seq(5, 15, 25, 35)),
    (3, "Charlie", Seq(100, 200, 300))
  ).toDF("id", "name", "scores")

  println("=== transform: double each score ===")
  df.select($"name", transform($"scores", x => x * 2).as("doubled")).show()

  println("=== filter: keep scores > 20 ===")
  df.select($"name", filter($"scores", x => x > 20).as("high_scores")).show()

  println("=== aggregate: sum of scores ===")
  df.select(
    $"name",
    aggregate($"scores", lit(0), (acc, x) => acc + x).as("total")
  ).show()

  println("=== exists: any score > 100? ===")
  df.select($"name", exists($"scores", x => x > 100).as("has_high")).show()

  println("=== forall: all scores > 10? ===")
  df.select($"name", forall($"scores", x => x > 10).as("all_above_10")).show()

  println("=== Chained: filter then transform ===")
  df.select(
    $"name",
    transform(
      filter($"scores", x => x >= 20),
      x => x * 100 / aggregate($"scores", lit(0), (a: org.apache.spark.sql.Column, b: org.apache.spark.sql.Column) => a + b)
    ).as("pct_of_total")
  ).show(truncate = false)

  spark.stop()
}
