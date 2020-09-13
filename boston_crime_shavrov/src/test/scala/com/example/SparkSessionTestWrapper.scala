package com.example

import org.apache.spark.sql.SparkSession

trait SparkSessionTestWrapper {

  implicit lazy val spark: SparkSession = {
    SparkSession.builder().master("local").appName("spark session").getOrCreate()
  }

  def getAbsolutePath(resourcePath: String): String = {
    getClass.getResource(resourcePath).getPath
  }
}
