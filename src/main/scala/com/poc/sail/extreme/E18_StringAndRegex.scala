package com.poc.sail.extreme

import com.poc.sail.SailSession
import org.apache.spark.sql.functions._

/**
 * E18: String manipulation and regex operations pushed to the limit.
 * Tests DataFusion's string function compatibility with Spark.
 */
object E18_StringAndRegex extends App {
  val spark = SailSession()
  import spark.implicits._

  val df = Seq(
    ("John Doe", "john.doe@company.com", "+34 612 345 678", "192.168.1.100"),
    ("Jane Smith", "jane_smith@other.org", "+1 (555) 123-4567", "10.0.0.1"),
    ("Carlos García", "carlos@correo.es", "+34 699 888 777", "172.16.0.50"),
    ("María López-Ruiz", "m.lopez@empresa.co.uk", "+44 20 7946 0958", "2001:db8::1")
  ).toDF("name", "email", "phone", "ip")

  println("=== Basic string functions ===")
  df.select(
    upper($"name"), lower($"name"), length($"name"),
    trim($"name"), ltrim($"name"), rtrim($"name"),
    reverse($"name"),
    initcap($"name")
  ).show(truncate = false)

  println("=== Substring and position ===")
  df.select(
    $"name",
    substring($"name", 1, 4).as("first_4"),
    instr($"name", " ").as("space_pos"),
    locate("o", $"name").as("first_o"),
    substring_index($"email", "@", 1).as("email_user"),
    substring_index($"email", "@", -1).as("email_domain")
  ).show(truncate = false)

  println("=== Regex extract ===")
  df.select(
    $"email",
    regexp_extract($"email", "^([^@]+)", 1).as("user"),
    regexp_extract($"email", "@(.+)$", 1).as("domain"),
    regexp_extract($"email", "\\.(\\w+)$", 1).as("tld")
  ).show(truncate = false)

  println("=== Regex replace ===")
  df.select(
    $"phone",
    regexp_replace($"phone", "[^0-9+]", "").as("digits_only"),
    regexp_replace($"name", "\\s+", "_").as("underscored")
  ).show(truncate = false)

  println("=== Pattern matching with LIKE, RLIKE ===")
  df.filter($"email".like("%@%.co.%")).show()
  df.filter($"email".rlike(".*@.*\\.(com|org)$")).show()
  df.filter($"ip".rlike("^\\d+\\.\\d+\\.\\d+\\.\\d+$")).show()

  println("=== String aggregation: concat_ws + collect_list ===")
  val tags = Seq(
    (1, "scala"), (1, "spark"), (1, "kafka"),
    (2, "python"), (2, "sql"),
    (3, "rust"), (3, "datafusion"), (3, "arrow")
  ).toDF("user_id", "tag")

  tags.groupBy("user_id")
    .agg(
      concat_ws(", ", collect_list("tag")).as("all_tags"),
      size(collect_list("tag")).as("num_tags")
    )
    .show(truncate = false)

  println("=== Complex string parsing: extract structured data ===")
  val logs = Seq(
    """{"level":"ERROR","ts":"2024-01-15T10:30:00Z","msg":"connection refused","host":"db-01"}""",
    """{"level":"WARN","ts":"2024-01-15T10:31:00Z","msg":"slow query 2350ms","host":"app-02"}""",
    """{"level":"INFO","ts":"2024-01-15T10:32:00Z","msg":"request completed","host":"app-01"}""",
    """{"level":"ERROR","ts":"2024-01-15T10:33:00Z","msg":"timeout after 30s","host":"db-01"}"""
  ).toDF("raw_log")

  logs.select(
    get_json_object($"raw_log", "$.level").as("level"),
    get_json_object($"raw_log", "$.ts").as("timestamp"),
    get_json_object($"raw_log", "$.msg").as("message"),
    get_json_object($"raw_log", "$.host").as("host")
  ).show(truncate = false)

  // Parse numbers from strings
  println("=== Extract numbers from text ===")
  val messy = Seq(
    "Order #12345 - total $1,234.56",
    "Invoice INV-98765 amount: €5.678,90",
    "Ref: 2024/001 - 42 items"
  ).toDF("text")

  messy.select(
    $"text",
    regexp_extract($"text", "#?(\\d{4,})", 1).as("first_number"),
    regexp_extract($"text", "\\$(\\S+)", 1).as("dollar_amount")
  ).show(truncate = false)

  spark.stop()
}
