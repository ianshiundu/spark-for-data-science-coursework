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

initialCommands += """
  import org.apache.spark.{SparkConf, SparkContext}
  import org.apache.spark.SparkContext._
  import org.apache.spark.sql.SQLContext
  val conf = new SparkConf().
    setMaster("local[*]").
    setAppName("Console").
    set("spark.app.id", "Console").   // To silence Metrics warning.
    set("spark.sql.shuffle.partitions", "4")  // for smaller data sets.
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
  import sqlContext.implicits._
  """

cleanupCommands += """
  println("Closing the SparkContext:")
  sc.stop()
  """.stripMargin