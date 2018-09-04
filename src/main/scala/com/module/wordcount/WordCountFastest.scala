package com.module.wordcount

import com.module.util.Files
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object WordCountFastest {
  def main(args: Array[String]): Unit = {
    val inpath = "data/all-shakespeare.txt"
    val outpath = "output/word_count1"

    Files.rmrf(outpath) // delete old (DON'T IN PROD)

    val sc = new SparkContext("local[*]", "Word Count Fastest")
    try {
      val input: RDD[String] = sc.textFile(inpath)
      val wc = input
        .map(_.toLowerCase)
        .flatMap(_.split("""\W+"""))
        .map(word ⇒ (word, 1)) // RDD[(String, Int)]
        .reduceByKey((n1, n2) ⇒ n1 + n2) // or reduceByKey(_ + _)
//        .groupBy(word ⇒ word) // Like SQL GROUP BY: RDD[(String, Iterator[String])]
//        .mapValues(_.size) // RDD[(String, Int)]

      println(s"Writing output to: $outpath")
      wc.saveAsTextFile(outpath)
      println("Enter any key to finish the job...")
      Console.in.read()
    } finally {
      sc.stop()
    }
  }
}