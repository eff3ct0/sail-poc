package com.poc.sail

import org.apache.spark.sql.SparkSession

object SailSession {
  def apply(): SparkSession = {
    val host = sys.env.getOrElse("SAIL_HOST", "localhost")
    val port = sys.env.getOrElse("SAIL_PORT", "50051")
    SparkSession.builder().remote(s"sc://$host:$port").build()
  }
}
