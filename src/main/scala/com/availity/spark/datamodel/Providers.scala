package com.availity.spark.datamodel

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.ScalaReflection.schemaFor
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, SparkSession}

case class Providers(
    provider_id : Long,
    provider_specialty: String,
    first_name: String,
    middle_name: String,
    last_name: String
)

object Providers extends Logging {
    def load(spark: SparkSession, isLocalMode: Boolean, dataFiles: String): Dataset[Providers] = {

    logInfo("Reading Providers Data from : " + dataFiles)

    if (isLocalMode)
      readFromTextFile(spark, dataFiles)
    else
     //for Future Implemnation for Delata or Parquet format requirement
}

def readFromTextFile(spark: SparkSession, dataFiles: String): Dataset[Providers] = {

    logWarning("Running in local mode , so reading Providers Data from csv file")

    import spark.implicits._
    spark
      .read
      .option("header", "true")
      .option("delimiter", "|")
      .option("inferSchema", "false")
      .schema(schemaFor[Providers].dataType.asInstanceOf[StructType])
      .csv(dataFiles)
      .as[Providers]
  }