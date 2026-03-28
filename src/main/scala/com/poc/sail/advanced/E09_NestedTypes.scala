package com.poc.sail.advanced

import com.poc.sail.SailSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/** E09: Structs, Arrays, Maps - create, access, explode, flatten */
object E09_NestedTypes extends App {
  val spark = SailSession()
  import spark.implicits._

  println("=== JSON with nested structs and arrays ===")
  val df = spark.read.json("data/employees.json")
  df.printSchema()
  df.show(truncate = false)

  println("=== Access nested struct fields ===")
  df.select($"name", $"address.city", $"address.zip").show()

  println("=== Explode array ===")
  df.select($"name", explode($"skills").as("skill")).show()

  println("=== Explode with position ===")
  df.select($"name", posexplode($"skills").as(Seq("idx", "skill"))).show()

  println("=== Array functions: size, contains, sort, distinct ===")
  df.select(
    $"name",
    size($"skills").as("num_skills"),
    array_contains($"skills", "scala").as("knows_scala"),
    sort_array($"skills").as("sorted_skills"),
    array_distinct($"skills").as("distinct_skills")
  ).show(truncate = false)

  println("=== Create and work with Maps ===")
  val withMap = df.select(
    $"name",
    map($"address.city", $"salary").as("city_salary_map")
  )
  withMap.show(truncate = false)
  withMap.select($"name", map_keys($"city_salary_map").as("cities")).show()

  println("=== Struct creation from columns ===")
  val structured = df.select(
    $"name",
    struct($"salary", $"dept").as("employment"),
    array($"address.city", $"address.zip").as("location_parts")
  )
  structured.show(truncate = false)
  structured.printSchema()

  println("=== Flatten nested arrays ===")
  val nested = Seq(
    (1, Seq(Seq(1, 2), Seq(3, 4))),
    (2, Seq(Seq(5, 6), Seq(7)))
  ).toDF("id", "nested_arr")
  nested.select($"id", flatten($"nested_arr").as("flat")).show()

  println("=== Arrays zip ===")
  val zipped = Seq(
    (Seq("a", "b", "c"), Seq(1, 2, 3)),
    (Seq("x", "y"), Seq(10, 20))
  ).toDF("letters", "numbers")
  zipped.select(arrays_zip($"letters", $"numbers").as("zipped")).show(truncate = false)

  spark.stop()
}
