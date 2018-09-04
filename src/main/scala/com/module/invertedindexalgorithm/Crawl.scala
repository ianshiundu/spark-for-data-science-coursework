package com.module.invertedindexalgorithm

import com.module.util.Files
import org.apache.spark.SparkContext

object Crawl {
  def main(args: Array[String]): Unit = {
    val inpath = "data/maildir/*"
    val outpath = "output/crawl"
    Files.rmrf(outpath) // delete old output (DON'T DO THIS IN PRODUCTION)

    val separator = java.io.File.separator

    val sc = new SparkContext("local[*]", "Crawl")

    try {
      val filesContents = sc.wholeTextFiles(inpath).map {
        case (id, text) ⇒
          val lastSep = id.lastIndexOf(separator)
          val id2 = if (lastSep < 0) id.substring(lastSep+1, id.length).trim
          val text2 = text.trim.replaceAll("""\s*\n\s*""", " ")
          (id2, text2)
      }
      println(s"Writing output to: $outpath")
      filesContents.saveAsTextFile(outpath)
    } finally {
      sc.stop()
    }

  }

}
