package com.abukhleif.spark01

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec

/** Find the movies with the most ratings. */
object _06_PopularMovies {
  def main(args: Array[String]): Unit = {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "PopularMoviesNicer")

    // Create a broadcast variable of our ID -> movie name map
    val nameDictionary = sc.broadcast(loadMovieNames)

    // Read in each rating line
    val lines = sc.textFile("./resources/movie-ratings.tsv")

    // Map to (movieID, 1) tuples
    val movies = lines.map(x => (x.split("\t")(1).toInt, 1))

    // Count up all the 1's for each movie
    val movieCounts = movies.reduceByKey(_ + _)

    // Flip (movieID, count) to (count, movieID)
    val flipped = movieCounts.map(x => (x._2, x._1))

    // Sort
    val sortedMovies = flipped.sortByKey()

    // Fold in the movie names from the broadcast variable
    val sortedMoviesWithNames = sortedMovies.map(x => (nameDictionary.value(x._2), x._1))

    // Collect and print results
    val results = sortedMoviesWithNames.collect()

    results.foreach(println)
  }

  /** Load up a Map of movie IDs to movie names. */
  def loadMovieNames: Map[Int, String] = {

    // Handle character encoding issues:
    implicit val codec: Codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    // Create a map of (movieID -> movieTitle)
    val lines = Source.fromFile("./resources/movie-titles.txt").getLines()
    lines.map(_.split('|')).filter(_.length > 1).map(fields => fields(0).toInt -> fields(1)).toMap
  }
}