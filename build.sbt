name := "ibm-spark-course"

version := "0.1"

scalaVersion := "2.11.9"

val sparkVersion = "2.3.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core"              % sparkVersion,
  "org.apache.spark" %% "spark-streaming"         % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql"               % sparkVersion,
  "org.apache.spark" %% "spark-hive"              % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming-twitter" % "1.6.3",
  "org.apache.spark" %% "spark-mllib"             % sparkVersion % "runtime",
  "org.twitter4j"    % "twitter4j-core"           % "4.0.7",
  "com.databricks"   %% "spark-csv"               % "1.5.0"
)