package com.example

import com.example.BostonCrimes._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


class AnalyzeBostonCrimes(crimesPath: String, offenseCodesPath: String, outputPath: String)(implicit spark: SparkSession) {
  private val crimesDF = spark.read.option("header", "true").csv(crimesPath)
  private val offenseCodesDF = {
    val offenceCodesSchema = StructType(Seq(StructField("code", IntegerType), StructField("name", StringType)))
    spark.read.schema(offenceCodesSchema).option("header", "true").csv(offenseCodesPath)
  }

  private val crimesTotal = crimesDF
    .groupBy(DISTRICT)
    .agg(
      count(INCIDENT_NUMBER).as(CRIMES_TOTAL)
    )

  private val crimesMonthly = crimesDF
    .withColumn(YEAR_MONTH, to_date(col("OCCURRED_ON_DATE"), dateFormatYearMonth))
    .groupBy(DISTRICT, YEAR_MONTH)
    .agg(
      count(INCIDENT_NUMBER).as("crimes_count_in_month")
    )
    .select(DISTRICT, "crimes_count_in_month")
    .groupBy(DISTRICT)
    .agg(
      callUDF("percentile_approx", col("crimes_count_in_month"), lit(0.5)).as(CRIMES_MONTHLY)
    )

  private val frequentCrimeTypes = {
    val crimeTypeParser = udf((offenseName: String) => offenseName.split("-").head.trim)
    val crimeTypesWindow = Window.partitionBy(DISTRICT).orderBy(col("OFFENSE_CODE_COUNT").desc)
    crimesDF
      .withColumn(CODE, col(OFFENSE_CODE).cast("INT"))
      .groupBy(DISTRICT, CODE)
      .agg(
        count(CODE).as("OFFENSE_CODE_COUNT")
      )
      .withColumn("rank", dense_rank() over crimeTypesWindow)
      .where("rank <= 3")
      .join(broadcast(offenseCodesDF), Seq(CODE), "inner")
      .withColumn(FREQUENT_CRIME_TYPES, crimeTypeParser(col(NAME)))
      .orderBy(DISTRICT, "rank")
      .select(DISTRICT, "offense_code_count", "rank", FREQUENT_CRIME_TYPES)
  }

  private val coordinates = crimesDF
    .groupBy(DISTRICT)
    .agg(
      avg("lat").as("lat"),
      avg("long").as("lng")
    )

  private def joinAggregates: DataFrame = {
    crimesTotal
      .join(crimesMonthly, Seq(DISTRICT), "left_outer")
      .join(frequentCrimeTypes, Seq(DISTRICT), "left_outer")
      .join(coordinates, Seq(DISTRICT), "left_outer")
      .dropDuplicates()
  }

  private def saveResult(df: DataFrame): Unit = {
    df.write.mode(SaveMode.Overwrite).parquet(outputPath)
  }

  def process(): Unit = {
    saveResult(joinAggregates)
  }
}
