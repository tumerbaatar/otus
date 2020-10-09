package com.example

import com.example.BostonCrimes.{CRIMES_TOTAL, DISTRICT, INCIDENT_NUMBER}
import org.scalatest.FunSpec
import org.apache.spark.sql.functions._
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.Row

class AnalyzeBostonCrimesSpec
    extends FunSpec
    with SparkSessionTestWrapper
    with DataFrameComparer {

  import spark.implicits._

  describe("Boston crimes analyze") {

    it("appends a happy column to a DataFrame") {
      val crimesPath = getAbsolutePath("/kaggle/crimes/crime.csv")
      val offenseCodesPath = getAbsolutePath("/kaggle/offence_codes/datasets_49781_90476_offense_codes.csv")
      // TODO: create tmp dir
      val outputPath = "/tmp/boston"

      new AnalyzeBostonCrimes(crimesPath, offenseCodesPath, outputPath).process()

//      val actualDF = spark.read.parquet(crimesPath)
//      val expectedDF = spark.read.csv(outputPath)
//      expectedDF.show()

//      actualDF

//      assertSmallDataFrameEquality(actualDF, expectedDF)
    }
  }

}