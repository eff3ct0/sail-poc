package com.poc.sail.extreme

import com.poc.sail.SailSession
import org.apache.spark.sql.functions._

/**
 * E15: Push nested type operations to the limit.
 * Maps of arrays, arrays of structs, deeply nested access,
 * complex aggregations over nested data.
 */
object E15_ComplexTypeMadness extends App {
  val spark = SailSession()
  import spark.implicits._

  println("=== Deeply nested struct creation and access ===")
  val deep = Seq(("order-1", "Alice")).toDF("order_id", "customer")
    .withColumn("details", struct(
      lit(3).as("quantity"),
      lit(29.99).as("price"),
      struct(
        lit("123 Main St").as("street"),
        struct(
          lit("Madrid").as("city"),
          lit("28001").as("zip"),
          lit("Spain").as("country")
        ).as("location")
      ).as("shipping")
    ))
  deep.printSchema()
  deep.select(
    $"order_id",
    $"details.shipping.location.city",
    $"details.shipping.location.country",
    ($"details.quantity" * $"details.price").as("total")
  ).show()

  println("=== Array of structs: explode + aggregate back ===")
  val orders = Seq(
    (1, "Alice"),
    (2, "Bob")
  ).toDF("order_id", "customer")

  val lineItems = Seq(
    (1, "Widget A", 3, 10.0),
    (1, "Widget B", 1, 25.0),
    (1, "Widget C", 2, 15.0),
    (2, "Gadget X", 5, 8.0),
    (2, "Gadget Y", 1, 100.0)
  ).toDF("order_id", "product", "qty", "unit_price")

  // Collect into array of structs per order
  val orderLines = lineItems
    .withColumn("line", struct($"product", $"qty", $"unit_price", ($"qty" * $"unit_price").as("line_total")))
    .groupBy("order_id")
    .agg(collect_list("line").as("items"))
    .join(orders, "order_id")

  orderLines.printSchema()
  orderLines.show(truncate = false)

  // Explode, compute, re-aggregate
  println("=== Explode array of structs, enrich, re-aggregate ===")
  orderLines
    .select($"order_id", $"customer", explode($"items").as("item"))
    .select(
      $"order_id", $"customer",
      $"item.product", $"item.qty", $"item.unit_price", $"item.line_total",
      when($"item.line_total" > 50, "premium").otherwise("standard").as("tier")
    )
    .groupBy("order_id", "customer")
    .agg(
      count("*").as("num_lines"),
      round(sum("line_total"), 2).as("order_total"),
      collect_list(struct($"product", $"tier")).as("product_tiers"),
      max("line_total").as("max_line")
    )
    .show(truncate = false)

  println("=== Map from array aggregation ===")
  lineItems
    .groupBy("order_id")
    .agg(
      map_from_entries(collect_list(struct($"product", ($"qty" * $"unit_price").as("total")))).as("product_totals")
    )
    .select(
      $"order_id",
      $"product_totals",
      map_keys($"product_totals").as("products"),
      map_values($"product_totals").as("totals"),
      element_at($"product_totals", "Widget A").as("widget_a_total")
    )
    .show(truncate = false)

  println("=== Nested collect_list of collect_list (array of arrays) ===")
  val events = Seq(
    ("user1", "session1", "click"), ("user1", "session1", "scroll"),
    ("user1", "session2", "click"), ("user1", "session2", "purchase"),
    ("user2", "session1", "click"), ("user2", "session1", "click")
  ).toDF("user_id", "session", "event")

  val sessionEvents = events.groupBy("user_id", "session")
    .agg(collect_list("event").as("events"))

  val userSessions = sessionEvents.groupBy("user_id")
    .agg(
      collect_list(struct($"session", $"events")).as("sessions"),
      sum(size($"events")).as("total_events")
    )
  userSessions.show(truncate = false)
  userSessions.printSchema()

  spark.stop()
}
