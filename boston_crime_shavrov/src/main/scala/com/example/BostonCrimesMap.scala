package com.example

import org.apache.spark.sql.SparkSession

object BostonCrimesMap {

  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession
      .builder()
      .appName("Spark session")
      .getOrCreate()

    val analyzeBoston = args match {
      case Array(crimes, offenceCodes, outputFolder) => new AnalyzeBostonCrimes(crimes, offenceCodes, outputFolder)
      case _ => throw new AssertionError("Paths to crimes, offence codes, and output are required")
    }

    analyzeBoston.process()
  }
}


