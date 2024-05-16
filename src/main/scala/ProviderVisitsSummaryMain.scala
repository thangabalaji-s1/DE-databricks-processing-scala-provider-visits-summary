package com.availity.spark.batch

import com.availity.datamodel._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

object ProviderVisitsSummaryMain extends Logging {
    
    def main(args: Array[String]): Unit = {

    val runArgs = ProviderVisitsSummaryRunAruguments(
      args,
      providersInputDirectoryDefault = "input/providers/",
      visitsInputDirectoryDefault = "input/visits/"
    )

    logInfo("providers Input Directory set to" + runArgs.providersInputDirectory)
    logInfo("visits Input Directory  set to" + runArgs.visitsInputDirectory)

    val spark = SparkSession
      .builder()
      .master(runArgs.sparkMaster)
      .appName("Providers-Visits-Summary-Batch-Processing")
      .getOrCreate()

      spark.sparkContext.setLogLevel("WARN")

      val providersRaw = Providers.load(spark, runArgs.isLocalMode, runArgs.providersInputDirectory)

      val visitsRaw = Visits.load(spark, runArgs.isLocalMode, runArgs.visitsInputDirectory)

      providersRaw.show()
      visitsRaw.show();

}
}