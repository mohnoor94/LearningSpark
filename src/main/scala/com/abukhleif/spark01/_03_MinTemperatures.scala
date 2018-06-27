package com.abukhleif.spark01

import org.apache.spark._
import org.apache.log4j._
import scala.math.min

/** Find the minimum temperature by weather station */
object _03_MinTemperatures {
  def main(args: Array[String]): Unit = {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "MinTemperatures")

    // Read each line of input data
    val lines = sc.textFile("./resources/temperatures.csv")

    // Convert to (stationID, entryType, temperature) tuples
    val parsedLines = lines.map(parseLine)

    // Filter out all but TMIN entries
    val minTemps = parsedLines.filter(x => x._2 == "TMIN")

    // Convert to (stationID, temperature)
    val stationTemps = minTemps.map(x => (x._1, x._3))

    // Reduce by stationID retaining the minimum temperature found
    val minTempsByStation = stationTemps.reduceByKey((x, y) => min(x, y))

    // Collect, format, and print the results
    val results = minTempsByStation.collect()

    results.sorted.foreach(result => println(f"${result._1} minimum temperature: ${result._2}%.2f F"))
  }

  def parseLine(line: String): (String, String, Float) = {
    val fields = line.split(",")
    val stationID = fields(0)
    val entryType = fields(2)
    val temperature = fields(3).toFloat * 0.1f * (9.0f / 5.0f) + 32.0f
    (stationID, entryType, temperature)
  }
}