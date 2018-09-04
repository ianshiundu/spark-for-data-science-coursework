package com.module.wordcount

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD


object WordCount {
  def main(args: Array[String]): Unit = {
    val inpath = "data/all-shakespeare.txt"
    val outpath = "output/word_count1"

    val sc = new SparkContext("local[*]", "Word Count")
    try {
      val input: RDD[String] = sc.textFile(inpath)
      val wc = input
        .map(_.toLowerCase)
        .flatMap(_.split("""\W+"""))
        .groupBy(word ⇒ word) // Like SQL GROUP BY: RDD[(String, Iterator[String])]
        .mapValues(_.size) // RDD[(String, Int)]

      println(s"Writing output to: $outpath")
      wc.saveAsTextFile(outpath)
      println("Enter any key to finish the job...")
      Console.in.read()
    } finally {
      sc.stop()
    }
  }
}
