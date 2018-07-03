package com.abukhleif.spark01

import org.apache.spark._
import org.apache.log4j._

/** Find the superhero with the most co-appearances. */
object _07_MostPopularSuperhero {
  def main(args: Array[String]): Unit = {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "MostPopularSuperhero")

    // Build up a hero ID -> name RDD
    // RDD will be available for all nodes/clusters
    val names = sc.textFile("./resources/marvel-names.txt")
    val namesRdd = names
      .map(_.split('"'))
      .filter(_.length > 1)
      .map(fields => (fields(0).trim().toInt, fields(1)))

    // Load up the superhero co-appearance data
    val lines = sc.textFile("./resources/marvel-graph.txt")

    // Convert to (heroID, number of connections) RDD
    val pairings = lines.map(countOccurences)

    // Combine entries that span more than one line
    val totalFriendsByCharacter = pairings.reduceByKey(_ + _)

    // Flip it to # of connections, hero ID
    val flipped = totalFriendsByCharacter.map(x => (x._2, x._1))

    // Find the max # of connections
    val mostPopular = flipped.max() // the maximum element of the RDD (max key!)

    // Look up the name (lookup returns an array of results, so we need to access the first result with (0) or 'head').
    val mostPopularName = namesRdd.lookup(mostPopular._2).head

    // Print out our answer!
    println(s"$mostPopularName is the most popular superhero with ${mostPopular._1} co-appearances.")
  }

  /** Function to extract the hero ID and number of connections from each line */
  def countOccurences(line: String): (Int, Int) = {
    val elements = line.split("\\s+") // s+ => white spaces
    (elements(0).toInt, elements.length - 1)
  }
}