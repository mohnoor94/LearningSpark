package com.abukhleif.spark01

import org.apache.log4j._
import org.apache.spark._

/** Compute the total amount spent per customer in some fake e-commerce data. */
object _05_TotalSpentByCustomer {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "FriendsByAge")
    val lines = sc.textFile("./resources/customer-orders.csv")
    val rdd = lines.map(parseLine)
    val totalPerCustomer = rdd.reduceByKey(_ + _).map(x => (x._2, x._1)).sortByKey()

    totalPerCustomer.collect().foreach(result => println(s"${result._2}:\t${result._1}"))
  }

  /**
    * @return (id, order)
    */
  def parseLine(line: String): (Int, Float) = {
    val fields = line.split(",")
    (fields(0).toInt, fields(2).toFloat)
  }
}