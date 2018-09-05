package com.module.invertedindexalgorithm

import com.module.util.Files
import org.apache.spark.SparkContext

object InvertedIndexSortByWordsAndCounts {
  def main(args: Array[String]): Unit = {
    val inpath = "output/crawl"
    val outpath = "output/inverted-index-sorted"
    Files.rmrf(outpath)

    val sc = new SparkContext("local[*]", "Invert Index Sort By Words And Counts ")

    try {
      // Load the input "crawl" data, where each line has the format:
      //   (document_id, text)
      // We use a regular expression to match on and ignore the outer
      // parentheses and use two "capture groups", one for the text between
      // between any initial whitespace and the first comma, and the second
      // group for the remaining text to the end of the line (trimming any
      // whitespace at the end. If the regex matches a line, we return a
      // tuple with the "name" (trimmed of whitespace) and the text, converted
      // to lower case.
      // NOTE: The inpath is a directory; Spark finds the correct data files,
      // named `part-NNNNN`.

      val lineRE = """^\s*\(([^,]+),(.*)\)\s*$""".r
      val input = sc.textFile(inpath).map {
        case lineRE(name, text) ⇒ (name.trim, text.toLowerCase)
        case badLine ⇒
          Console.err.println(s"Unexpected line: $badLine")
          // We have to return a tuple. It will be filtered out by subsequent
          // remove of whitespace (when we split into words).
          ("", "")
      }

      println(s"Writing output to: $outpath")

      // Split on non-alphanumeric sequences of character as before.
      // Rather than map to "(word, 1)" tuples, we treat the (word,path)
      // pairs as the unique keys, then count the occurrences.
      input
        .flatMap {
          case (path, text) ⇒
            // If we don't trim leading whitespace, the regex split creates
            // an undesired leading "" word! Also, setup the (word,path) as
            // the key for reducing, and an initial count of 1.
            // Use a refined regex to retain abbreviations, e.g., "there's".
            text.trim.split("""[^\w']""").map(word ⇒ ((word, path), 1))
        }
        .reduceByKey {
          // No "case" here, because we don't need to pattern match, as the
          // function takes two arguments, not one argument that needs to be
          // "deconstructed".
          (count1, count2) ⇒ count1 + count2
        }
        .map {
          // Set up for the final output with words as keys.
          case ((word, path), n) ⇒ (word, (path, n))
        }
        .groupByKey
        .mapValues {
          iterable ⇒
            val vect = iterable.toVector.sortBy {
              case (path, n) ⇒ (-n, path)
            }
            vect.mkString(", ")
        }
        .saveAsTextFile(outpath)
    } finally {
      println(
        """
          |=======================================================
          |
          |Before we close the SparkContext, open the Spark Web Console
          |and browse the information about this tasks.
          |
          |Hit <return> key to exit
        """.stripMargin)
      Console.in.read()
      sc.stop()
    }
  }

}
