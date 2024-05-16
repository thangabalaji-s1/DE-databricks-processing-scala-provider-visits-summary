package com.availity.spark.datamodel

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.ScalaReflection.schemaFor
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, SparkSession}

case class Visits(
    visitID : Long,
    providerID: Long,
    serviceDate: Date
)

object Visits extends Logging {
    def load(spark: SparkSession, isLocalMode: Boolean, dataFiles: String): Dataset[Visits] = {

    logInfo("Reading Visits Data from : " + dataFiles)

    if (isLocalMode)
      readFromTextFile(spark, dataFiles)
    else
     --Future Implemnation for Delata or Parquet format
}

def readFromTextFile(spark: SparkSession, dataFiles: String): Dataset[Visits] = {

    logWarning("Running in local mode , so reading Visits Data from csv file")

    import spark.implicits._
    spark
      .read
      .option("header", "false")
      .option("delimiter", ",")
      .option("inferSchema", "false")
      .schema(schemaFor[Visits].dataType.asInstanceOf[StructType])
      .csv(dataFiles)
      .as[Visits]
  }