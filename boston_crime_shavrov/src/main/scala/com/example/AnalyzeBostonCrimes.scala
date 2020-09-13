package com.example

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

class AnalyzeBostonCrimes(crimesPath: String, offenseCodesPath: String, outputPath: String)(implicit spark: SparkSession) {

  private def crimes: DataFrame = {
    val crimesDF = spark.read.option("header", "true").csv(crimesPath)
    val offenseCodesDF = spark.read.option("header", "true").csv(offenseCodesPath)

    crimesDF.show()
    offenseCodesDF.show()
    crimesDF
  }

  private def saveResult(df: DataFrame): Unit = {
    df.write.mode(SaveMode.Overwrite).parquet(outputPath)
  }

  def process(): Unit = {
    saveResult(crimes)
  }
}
