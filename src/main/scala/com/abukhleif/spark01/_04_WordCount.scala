package com.abukhleif.spark01

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

/** Count up how many of each word occurs in a book, using regular expressions and sorting the final results */
object _04_WordCount extends App {
  // Set the log level to only print errors
  Logger.getLogger("org").setLevel(Level.ERROR)

  // Create a SparkContext using every core of the local machine
  val sc = new SparkContext("local[*]", "WordCount")

  // Read each line of my book into an RDD
  val input = sc.textFile("./resources/book.txt")

  // Split using a regular expression that extracts words
  val words = input.flatMap(x => x.split("\\W+"))

  // Normalize everything to lowercase
  val lowercaseWords = words.map(x => x.toLowerCase())

  // Count of the occurrences of each word
  val wordCounts = lowercaseWords.map(x => (x, 1)).reduceByKey((x, y) => x + y)

  // Flip (word, count) tuples to (count, word) and then sort by key (the counts)
  val sortedWordCounts = wordCounts.map(x => (x._2, x._1)).sortByKey()

  // Print the results, flipping the (count, word) results to word: count as we go.
  sortedWordCounts.collect().foreach(result => println(s"${result._2}: ${result._1}"))
}