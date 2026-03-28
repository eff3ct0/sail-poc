package com.poc.sail.advanced

import com.poc.sail.SailSession
import org.apache.spark.sql.functions._

/** E12: Date/Time functions - parsing, extraction, arithmetic, formatting */
object E12_DateTimeFunctions extends App {
  val spark = SailSession()
  import spark.implicits._

  val tx = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("data/transactions.csv")
    .withColumn("tx_date", to_date($"tx_date"))

  println("=== Date extraction ===")
  tx.select(
    $"tx_id", $"tx_date",
    year($"tx_date").as("year"),
    month($"tx_date").as("month"),
    dayofmonth($"tx_date").as("day"),
    dayofweek($"tx_date").as("dow"),
    dayofyear($"tx_date").as("doy"),
    weekofyear($"tx_date").as("week"),
    quarter($"tx_date").as("quarter")
  ).show()

  println("=== Date arithmetic ===")
  tx.select(
    $"tx_id", $"tx_date",
    date_add($"tx_date", 30).as("plus_30_days"),
    date_sub($"tx_date", 7).as("minus_1_week"),
    add_months($"tx_date", 3).as("plus_3_months"),
    datediff(current_date(), $"tx_date").as("days_ago"),
    months_between(current_date(), $"tx_date").as("months_ago"),
    last_day($"tx_date").as("month_end"),
    next_day($"tx_date", "Mon").as("next_monday"),
    trunc($"tx_date", "MM").as("month_start"),
    trunc($"tx_date", "Q").as("quarter_start")
  ).show()

  println("=== Date formatting ===")
  tx.select(
    $"tx_date",
    date_format($"tx_date", "dd/MM/yyyy").as("eu_format"),
    date_format($"tx_date", "EEEE, MMMM d, yyyy").as("long_format"),
    date_format($"tx_date", "yyyy-'W'ww").as("iso_week")
  ).show(truncate = false)

  println("=== Timestamp operations ===")
  val withTs = tx.withColumn("tx_timestamp",
    to_timestamp(concat($"tx_date".cast("string"), lit(" 14:30:00")))
  )
  withTs.select(
    $"tx_id",
    $"tx_timestamp",
    hour($"tx_timestamp").as("hour"),
    minute($"tx_timestamp").as("minute"),
    from_unixtime(unix_timestamp($"tx_timestamp")).as("from_unix"),
    window($"tx_timestamp", "30 days").as("time_window")
  ).show(truncate = false)

  println("=== Date-based windowing: running total per month ===")
  import org.apache.spark.sql.expressions.Window
  val monthWindow = Window.orderBy("month")
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

  tx.withColumn("month", date_format($"tx_date", "yyyy-MM"))
    .groupBy("month")
    .agg(round(sum("amount"), 2).as("monthly_total"))
    .orderBy("month")
    .withColumn("cumulative_total", round(sum("monthly_total").over(monthWindow), 2))
    .show()

  spark.stop()
}
