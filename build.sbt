name := "scala-sail-poc"
version := "0.1.0"
scalaVersion := "2.13.15"

val sparkVersion = "4.0.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-connect-client-jvm" % sparkVersion
)

// Fork the JVM for run to avoid classloader issues
fork := true
