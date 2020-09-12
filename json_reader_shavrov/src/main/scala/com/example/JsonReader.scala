package com.example

import org.apache.spark.sql.SparkSession
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import org.json4s.{NoTypeHints, _}

object JsonReader {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("Spark session").getOrCreate()

    val path = args match {
      case Array(p) => p
      case Array() => throw new AssertionError("Path to data must be specified as the first argument")
      case _ => throw new AssertionError("The only one argument for path to data is allowed")
    }

    val result = spark.sparkContext
      .textFile(path)
      .map(rawUser => {
        implicit val formats = Serialization.formats(NoTypeHints)
        parse(rawUser).extract[User]
      })

    result.foreach(println)
    println(s"Total count: ${result.count()}")
  }
}


