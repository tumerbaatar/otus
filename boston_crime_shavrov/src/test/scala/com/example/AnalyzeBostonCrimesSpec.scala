package com.example

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.scalatest.FunSpec

class AnalyzeBostonCrimesSpec
    extends FunSpec
    with SparkSessionTestWrapper
    with DataFrameComparer {

  describe("Boston crimes analyze") {

    it("creates custom aggregate over Boston crimes") {
      val crimesPath = getAbsolutePath("/kaggle/crimes/crimes_sample.csv")
      val offenseCodesPath = getAbsolutePath("/kaggle/offence_codes/datasets_49781_90476_offense_codes.csv")
      val outputPath = "/tmp/boston"

      new AnalyzeBostonCrimes(crimesPath, offenseCodesPath, outputPath).process()

      val actualDF = spark.read.parquet(outputPath)
      actualDF.show()
    }
  }

}