package com.example

import org.apache.spark.sql.SparkSession

trait SparkSessionTestWrapper {

  implicit lazy val spark: SparkSession = {
    val pathToLog4j = getClass.getResource("/log4j.properties").getPath
    println(pathToLog4j)
    SparkSession
      .builder()
      .master("local[*]")
      .appName("spark session")
      .config("spark.driver.extraJavaOptions", s"-Dlog4j.configuration=file://$pathToLog4j")
      .config("spark.executor.extraJavaOptions", s"-Dlog4j.configuration=file://$pathToLog4j")
      .getOrCreate()
  }

  def getAbsolutePath(resourcePath: String): String = {
    getClass.getResource(resourcePath).getPath
  }
}
