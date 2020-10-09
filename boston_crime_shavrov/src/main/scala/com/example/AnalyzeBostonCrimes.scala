package com.example

import com.example.BostonCrimes.{CRIMES_TOTAL, DISTRICT, INCIDENT_NUMBER, YEAR_MONTH, dateFormatYearMonth}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object BostonCrimes {
  val dateFormatYearMonth = "yyyy-MM"
  val DISTRICT = "DISTRICT"
  val INCIDENT_NUMBER = "INCIDENT_NUMBER"
  val YEAR_MONTH = "YEAR_MONTH"
  val CRIMES_TOTAL = "CRIMES_TOTAL"
  val CRIMES_MONTHLY = "CRIMES_MONTHLY"
}

class AnalyzeBostonCrimes(crimesPath: String, offenseCodesPath: String, outputPath: String)(implicit spark: SparkSession) {

  private def crimes: DataFrame = {
    val crimesDF = spark.read.option("header", "true").csv(crimesPath)
    val offenseCodesDF = spark.read.option("header", "true").csv(offenseCodesPath)

    val crimesTotal = crimesDF
      .groupBy(DISTRICT)
      .agg(
        count(INCIDENT_NUMBER).as(CRIMES_TOTAL)
      )

    val crimesMonthly = crimesDF
      .withColumn(YEAR_MONTH, to_date(col("OCCURRED_ON_DATE"), dateFormatYearMonth))
      .groupBy(DISTRICT, YEAR_MONTH)
      .agg(
        count(INCIDENT_NUMBER).as("crimes_count_in_month")
      )
      .select(DISTRICT, "crimes_count_in_month")
      .groupBy(DISTRICT)
      .agg(
        callUDF("percentile_approx", col("crimes_count_in_month"), lit(0.5)).as("crimes_montly_avg")
      )

    val crimeTypesWindow = Window.partitionBy(DISTRICT).orderBy(col("OFFENSE_CODE_COUNT").desc)
    val frequentCrimeTypes = crimesDF
      .groupBy(DISTRICT, "OFFENSE_CODE")
      .agg(
        count("OFFENSE_CODE").as("OFFENSE_CODE_COUNT")
      )
      // TODO: withdraw offence code name as described in the task
      .withColumn("rank", dense_rank() over crimeTypesWindow)
      .where("rank <= 3")

    frequentCrimeTypes.show()
    //    crimesTotal.show()
    //    crimesMonthly.show()

    crimesDF
  }

  private def saveResult(df: DataFrame): Unit = {
    df.write.mode(SaveMode.Overwrite).parquet(outputPath)
  }

  def process(): Unit = {
    saveResult(crimes)
  }
}
