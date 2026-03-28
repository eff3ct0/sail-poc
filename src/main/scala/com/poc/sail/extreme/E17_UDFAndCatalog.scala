package com.poc.sail.extreme

import com.poc.sail.SailSession
import org.apache.spark.sql.functions._

/**
 * E17: UDF registration, catalog operations, temp view management,
 * and dynamic SQL construction.
 */
object E17_UDFAndCatalog extends App {
  val spark = SailSession()
  import spark.implicits._

  // Register UDFs via SQL
  println("=== Register UDF via SQL ===")
  // Note: Spark Connect supports registering Python UDFs and
  // Scala UDFs serialized over the wire. Let's test what Sail handles.

  // DataFrame-based UDF
  val normalize = udf((s: String) => {
    Option(s).map(_.trim.toLowerCase.replaceAll("[^a-z0-9]", "_")).orNull
  })

  val classifySalary = udf((salary: Int) => salary match {
    case s if s >= 100000 => "senior"
    case s if s >= 75000 => "mid"
    case _ => "junior"
  })

  val df = Seq(
    (1, "  Alice Smith  ", "Engineering", 95000),
    (2, " Bob Jones", "Marketing", 72000),
    (3, "Charlie Brown ", "Engineering", 110000)
  ).toDF("id", "name", "dept", "salary")

  println("=== Apply UDFs ===")
  df.select(
    $"id",
    normalize($"name").as("normalized_name"),
    $"dept",
    $"salary",
    classifySalary($"salary").as("level")
  ).show()

  println("=== Catalog: list databases ===")
  spark.sql("SHOW DATABASES").show()

  println("=== Catalog: create and list temp views ===")
  df.createOrReplaceTempView("test_employees")
  Seq((1, "a"), (2, "b")).toDF("x", "y").createOrReplaceTempView("test_xy")

  spark.sql("SHOW TABLES").show()

  println("=== Catalog: describe table ===")
  spark.sql("DESCRIBE test_employees").show()

  println("=== Catalog: drop view ===")
  spark.sql("DROP VIEW IF EXISTS test_xy")
  spark.sql("SHOW TABLES").show()

  println("=== Dynamic column operations ===")
  val columns = Seq("name", "dept", "salary")
  val selected = df.select(columns.map(col): _*)
  selected.show()

  // Dynamic aggregation based on column types
  println("=== Dynamic aggregation ===")
  val numCols = df.schema.fields.filter(_.dataType.typeName == "integer").map(_.name)
  val aggExprs = numCols.flatMap(c => Seq(
    avg(col(c)).as(s"avg_$c"),
    max(col(c)).as(s"max_$c"),
    min(col(c)).as(s"min_$c")
  ))
  df.agg(aggExprs.head, aggExprs.tail: _*).show()

  spark.stop()
}
