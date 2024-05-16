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
    
      // Load the providers dataset
      val providersRaw = Providers.load(spark, runArgs.isLocalMode, runArgs.providersInputDirectory)
      // Load the visits dataset
      val visitsRaw = Visits.load(spark, runArgs.isLocalMode, runArgs.visitsInputDirectory)

      // Calculate the total number of visits per provider
      val providerVisitsSummary = ProviderVisitsSummaryProcessing.calculateVisits(spark, providersRaw, visitsRaw)

       // Save the result to JSON files
      resultDF.write.partitionBy("provider_specialty").format("json").save("output/visits_per_provider")
    
      // Calculate the total number of visits per provider per month
      val providerVisitsSummary = ProviderVisitsSummaryProcessing.calculateVisitsperMonth(spark, providersRaw, visitsRaw)

       // write to JSON files
        visitsPerProviderPerMonthDF.write.format("json").mode("overwrite").save("output/visits_per_provider_per_month") 

}
}