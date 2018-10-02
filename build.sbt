name := "ibm-spark-course"

version := "0.1"

scalaVersion := "2.10.5"

val sparkVersion = "1.5.2"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core"              % sparkVersion withSources(),
  "org.apache.spark" %% "spark-streaming"         % sparkVersion withSources(),
  "org.apache.spark" %% "spark-sql"               % sparkVersion withSources(),
  "org.apache.spark" %% "spark-hive"              % sparkVersion withSources(),
  "org.apache.spark" %% "spark-streaming-twitter" % sparkVersion withSources(),
  "org.apache.spark" %% "spark-mllib"             % sparkVersion withSources(),
  "org.twitter4j"    %  "twitter4j-core"          % "3.0.3" withSources(),
  "com.databricks"   %% "spark-csv"               % "1.3.0"      withSources()
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