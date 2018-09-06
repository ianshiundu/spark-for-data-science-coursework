package com.module.dataframes

import com.module.mldata.{Airport, Flight}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
/**
  * Spark DataFrames, a Python Pandas-like API built on top of
  * SparkSQL with better performance than RDDs, due to the use of Catalyst
  * for "query" optimization.
  */
object SparkDataFrames {
  var out = Console.out
  var quiet = false

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Spark DataFrames")
    conf.set("spark.sql.shuffle.partitions", "4")
    conf.set("spark.app.id", "SparkDataFrames") // To silence Metrics warning.

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._ // Needed for column idioms like $"foo".desc

    try {

      val flightsPath = "data/airline-flights/2008.csv"
      val airportsPath = "data/airline-flights/airports.csv"

//      Don't pre-guess keys use the types as schemas
      val flightsRDD = for {
        line ← sc.textFile(flightsPath)
        flight ← Flight.parse(line)
      } yield flight

      val airportsRDD = for {
        line ← sc.textFile(airportsPath)
        airport ← Airport.parse(line)
      } yield airport

      val flights = sqlContext.createDataFrame(flightsRDD)
      val airports= sqlContext.createDataFrame(airportsRDD)
//      cache flights and airports
      flights.cache
      airports.cache

    }
  }

}
