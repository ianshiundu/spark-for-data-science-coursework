package com.module.dataframes

import com.module.mldata.{Airport, Flight}
import com.module.util.Printer
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

      // SELECT COUNT(*) FROM flights f WHERE f.canceled > 0;
      val canceled_flights = flights.filter(flights("canceled") > 0)
      Printer(out, "canceled flights", canceled_flights)
      if (!quiet) {
        out.println("\ncanceled_flights.explain(extended = false):")
        canceled_flights.explain(extended = false)
        out.println("\ncanceled_flights.explain(extended = true):")
        canceled_flights.explain(extended = true)
      }
      canceled_flights.cache

//      Reference columns
      if(!quiet) {
        flights.orderBy(flights("origin")).show
        flights.orderBy("origin").show
        flights.orderBy($"origin").show
        flights.orderBy($"origin".desc).show
        // The last one $"count".desc is the only (?) way to specify descending order.
        // The $"..." is not a Scala built-in feature, but Scala allows you to
        // implement "interpolated string" handlers with your own prefix ($ in
        // this case).
      }

      // SELECT cf.date.month AS month, COUNT(*)
      //   FROM canceled_flights cf
      //   GROUP BY cf.date.month
      //   ORDER BY month;
      val canceled_flights_by_month = canceled_flights.
        groupBy("date.month").count()
      Printer(out, "canceled flights by month", canceled_flights_by_month)
      if (!quiet) {
        out.println("\ncanceled_flights_by_month.explain(true):")
        canceled_flights_by_month.explain(true)
      }
      canceled_flights.unpersist

    }
  }

}
