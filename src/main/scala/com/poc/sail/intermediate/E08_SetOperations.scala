package com.poc.sail.intermediate

import com.poc.sail.SailSession
import org.apache.spark.sql.functions._

/** E08: Union, UnionByName, Intersect, Except */
object E08_SetOperations extends App {
  val spark = SailSession()
  import spark.implicits._

  val team1 = Seq(
    ("Alice", "Engineering"), ("Bob", "Marketing"), ("Charlie", "Engineering")
  ).toDF("name", "dept")

  val team2 = Seq(
    ("Charlie", "Engineering"), ("Diana", "Marketing"), ("Eve", "Engineering")
  ).toDF("name", "dept")

  println("=== UNION ALL (duplicates kept) ===")
  team1.union(team2).show()

  println("=== UNION (distinct) ===")
  team1.union(team2).distinct().show()

  println("=== UNION BY NAME (different column order) ===")
  val team3 = Seq(("Engineering", "Frank"), ("Sales", "Grace"))
    .toDF("dept", "name")
  team1.unionByName(team3).show()

  println("=== INTERSECT ===")
  team1.intersect(team2).show()

  println("=== EXCEPT (in team1 but not team2) ===")
  team1.except(team2).show()

  println("=== EXCEPT ALL (keeps duplicates) ===")
  team1.exceptAll(team2).show()

  spark.stop()
}
