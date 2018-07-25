package com.abukhleif.spark01

import java.nio.charset.CodingErrorAction

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.io.{Codec, Source}

object _09_MovieSimilarities {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkContext = new SparkContext("local[*]", "MovieSimilarities")

    println("\nLoading movie names...")

    val nameDictionary = loadMovieNames()

    // RDD
    val data: RDD[String] = sparkContext.textFile("./resources/movie-ratings.tsv")

    // Map ratings to key / value pairs: user ID => movie ID, rating
    val ratings = data.map(_.split('\t')).map(line => line(0).toInt -> (line(1).toInt -> line(2).toDouble))

    // Emit every movie rated together by the same user.
    // Self-join to find every combination.
    val joinedRatings = ratings join ratings

    // At this point our RDD consists of userID => ((movieID, rating), (movieID, rating))

    // Filter out duplicate pairs
    val uniqueJoinedRatings = joinedRatings.filter(r => r._2._1._1 < r._2._2._1)

    // Now key by (movie1, movie2) pairs.
    val moviePairs = uniqueJoinedRatings.map(pair => (pair._2._1._1, pair._2._2._1) -> (pair._2._1._2, pair._2._2._2))

    // We now have (movie1, movie2) => (rating1, rating2)
    // Now collect all ratings for each movie pair and compute similarity
    val moviePairRatings = moviePairs.groupByKey

    // We now have (movie1, movie2) = > (rating1, rating2), (rating1, rating2) ...

    // Can now compute similarities
    val moviePairSimilarities = moviePairRatings.mapValues(computeCosineSimilarity).cache()

    // Save the results if desired
    // val sorted = moviePairSimilarities.sortByKey()
    // sorted.saveAsTextFile("movie-sims")

    // Extract similarities for the movie we care about that are "good".

    if (args.length > 0) {
      val scoreThreshold = 0.97
      val coOccurrenceThreshold = 50.0

      val movieID: Int = args(0).toInt

      // Filter for movies with this sim that are "good" as defined by
      // our quality thresholds above
      val filteredResults = filterResults(moviePairSimilarities, scoreThreshold, coOccurrenceThreshold, movieID)

      // Sort by quality score.
      val finalResults = filteredResults.map(result => (result._2, result._1)).sortByKey(ascending = false).take(10)

      printResults(nameDictionary, movieID, finalResults)
    }
  }

  private def printResults(nameDictionary: Map[Int, String], movieID: Int,
                           finalResults: Array[((Double, Int), (Int, Int))]): Unit = {
    println("\nTop 10 similar movies for " + nameDictionary(movieID))
    println("\nScore\tStrength\tMovie")
    println("-----\t--------\t-----")
    finalResults.foreach(result => {
      val movies = result._1
      val similarity = result._2

      val similarMovieID = if (movieID == similarity._1) similarity._2 else similarity._1

      println(s"${(movies._1 * 100).toInt}%\t\t${movies._2}\t\t\t${nameDictionary(similarMovieID)}")
    })
  }

  private def filterResults(moviePairSimilarities: RDD[((Int, Int), (Double, Int))], scoreThreshold: Double,
                            coOccurrenceThreshold: Double, movieID: Int): RDD[((Int, Int), (Double, Int))] = {
    moviePairSimilarities.filter(result => {
      val movies = result._1
      val similarity = result._2

      (movies._1 == movieID || movies._2 == movieID) &&
        similarity._1 > scoreThreshold &&
        similarity._2 > coOccurrenceThreshold
    })
  }

  private def loadMovieNames(): Map[Int, String] = {
    // Handle character encoding issues:
    implicit val codec: Codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    val lines = Source.fromFile("./resources/movie-titles.txt").getLines()
    lines.map(_.split('|')).filter(_.length > 1).map(fields => fields(0).toInt -> fields(1)).toMap
  }

  private type RatingPair = (Double, Double)
  private type RatingPairs = Iterable[RatingPair]

  private def computeCosineSimilarity(ratingPairs: RatingPairs): (Double, Int) = {
    var numPairs: Int = 0
    var sum_xx: Double = 0.0
    var sum_yy: Double = 0.0
    var sum_xy: Double = 0.0

    for (pair <- ratingPairs) {
      val ratingX = pair._1
      val ratingY = pair._2

      sum_xx += ratingX * ratingX
      sum_yy += ratingY * ratingY
      sum_xy += ratingX * ratingY
      numPairs += 1
    }

    val numerator: Double = sum_xy
    val denominator = math.sqrt(sum_xx) * math.sqrt(sum_yy)

    var score: Double = 0.0
    if (denominator != 0) {
      score = numerator / denominator
    }

    score -> numPairs
  }
}
