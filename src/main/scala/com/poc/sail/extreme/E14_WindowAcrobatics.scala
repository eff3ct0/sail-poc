package com.poc.sail.extreme

import com.poc.sail.SailSession

/**
 * E14: Extreme window function combinations - sessionization, gaps and islands,
 * running medians, conditional windows.
 */
object E14_WindowAcrobatics extends App {
  val spark = SailSession()
  import spark.implicits._

  // Web clickstream data
  Seq(
    ("user1", "2024-01-01 10:00:00", "/home"),
    ("user1", "2024-01-01 10:02:00", "/products"),
    ("user1", "2024-01-01 10:03:00", "/products/123"),
    ("user1", "2024-01-01 10:30:00", "/home"),        // new session (27 min gap)
    ("user1", "2024-01-01 10:31:00", "/cart"),
    ("user2", "2024-01-01 11:00:00", "/home"),
    ("user2", "2024-01-01 11:01:00", "/search"),
    ("user2", "2024-01-01 11:05:00", "/products/456"),
    ("user2", "2024-01-01 14:00:00", "/home"),         // new session
    ("user2", "2024-01-01 14:01:00", "/checkout")
  ).toDF("user_id", "ts", "page").createOrReplaceTempView("clicks")

  println("=== Sessionization: detect sessions with 10-min gap threshold ===")
  spark.sql("""
    WITH click_gaps AS (
      SELECT *,
        CAST(ts AS TIMESTAMP) as event_time,
        LAG(CAST(ts AS TIMESTAMP)) OVER (PARTITION BY user_id ORDER BY ts) as prev_time
      FROM clicks
    ),
    session_starts AS (
      SELECT *,
        CASE
          WHEN prev_time IS NULL THEN 1
          WHEN (UNIX_TIMESTAMP(event_time) - UNIX_TIMESTAMP(prev_time)) > 600 THEN 1
          ELSE 0
        END as is_new_session
      FROM click_gaps
    ),
    sessions AS (
      SELECT *,
        SUM(is_new_session) OVER (PARTITION BY user_id ORDER BY event_time) as session_id
      FROM session_starts
    )
    SELECT user_id, session_id, event_time, page,
           ROW_NUMBER() OVER (PARTITION BY user_id, session_id ORDER BY event_time) as click_in_session,
           COUNT(*) OVER (PARTITION BY user_id, session_id) as session_length
    FROM sessions
    ORDER BY user_id, event_time
  """).show(truncate = false)

  // Gaps and Islands problem
  Seq(
    (1, "2024-01-01"), (1, "2024-01-02"), (1, "2024-01-03"),
    (1, "2024-01-06"), (1, "2024-01-07"),
    (1, "2024-01-10"),
    (2, "2024-01-01"),
    (2, "2024-01-03"), (2, "2024-01-04"), (2, "2024-01-05")
  ).toDF("user_id", "dt").createOrReplaceTempView("activity")

  println("=== Gaps & Islands: find consecutive activity streaks ===")
  spark.sql("""
    WITH numbered AS (
      SELECT *,
        DATE(dt) as activity_date,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY dt) as rn
      FROM activity
    ),
    islands AS (
      SELECT *,
        DATE_SUB(activity_date, rn) as island_key
      FROM numbered
    )
    SELECT user_id,
           MIN(activity_date) as streak_start,
           MAX(activity_date) as streak_end,
           COUNT(*) as streak_length
    FROM islands
    GROUP BY user_id, island_key
    ORDER BY user_id, streak_start
  """).show()

  // Moving average with variable windows
  Seq(
    ("AAPL", "2024-01-02", 185.0), ("AAPL", "2024-01-03", 184.5),
    ("AAPL", "2024-01-04", 181.0), ("AAPL", "2024-01-05", 182.5),
    ("AAPL", "2024-01-08", 185.5), ("AAPL", "2024-01-09", 186.0),
    ("AAPL", "2024-01-10", 187.5), ("AAPL", "2024-01-11", 186.0),
    ("GOOG", "2024-01-02", 140.0), ("GOOG", "2024-01-03", 141.5),
    ("GOOG", "2024-01-04", 139.0), ("GOOG", "2024-01-05", 142.0),
    ("GOOG", "2024-01-08", 143.5), ("GOOG", "2024-01-09", 144.0),
    ("GOOG", "2024-01-10", 143.0), ("GOOG", "2024-01-11", 145.0)
  ).toDF("symbol", "dt", "close").createOrReplaceTempView("stocks")

  println("=== Multiple overlapping windows: SMA-3, SMA-5, Bollinger-like ===")
  spark.sql("""
    SELECT symbol, dt, close,
      ROUND(AVG(close) OVER (
        PARTITION BY symbol ORDER BY dt ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
      ), 2) as sma_3,
      ROUND(AVG(close) OVER (
        PARTITION BY symbol ORDER BY dt ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
      ), 2) as sma_5,
      ROUND(STDDEV(close) OVER (
        PARTITION BY symbol ORDER BY dt ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
      ), 2) as vol_5,
      ROUND(close - LAG(close, 1) OVER (PARTITION BY symbol ORDER BY dt), 2) as daily_change,
      ROUND(
        (close - MIN(close) OVER (PARTITION BY symbol ORDER BY dt ROWS BETWEEN 4 PRECEDING AND CURRENT ROW)) /
        NULLIF(MAX(close) OVER (PARTITION BY symbol ORDER BY dt ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) -
               MIN(close) OVER (PARTITION BY symbol ORDER BY dt ROWS BETWEEN 4 PRECEDING AND CURRENT ROW), 0)
        * 100, 2
      ) as williams_pct_r
    FROM stocks
    ORDER BY symbol, dt
  """).show()

  spark.stop()
}
