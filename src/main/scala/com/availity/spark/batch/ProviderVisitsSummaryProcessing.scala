package com.availity.spark.batch

import com.footlocker.datamodel._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object ProviderVisitsSummaryProcessing extends Logging {

    def calculateVisits(spark: SparkSession,
    providersRaw : Dataset[Providers],
    visitsRaw: Dataset[visits]
    ) = {

        // Join the two datasets on provider ID
         val joinedDF = visitsDF.join(providersDF, visitsDF("provider_id") === providersDF("provider_id"))

         // Calculate the total number of visits per provider
        val visitsPerProviderDF = joinedDF
                                    .groupBy("provider_id", "first_name", "last_name", "provider_specialty")
                                    .agg(
                                        count("visitID").as("total_visits")
                                        )
         // Partition the result by specialty
        val resultDF = visitsPerProviderDF.repartition($"provider_specialty")

        resultDF
    }
       
    def calculateVisitsperMonth(spark: SparkSession,
                                providersRaw : Dataset[Providers],
                                visitsRaw: Dataset[visits]
                                ) = {
        // join visits with providers on provider_id
        val joinedDF = visitsDF.join(providersDF, "provider_id")

        // extract month from service_date
        val monthDF = joinedDF.withColumn("month", month(joinedDF("service_date")))

        // group by provider_id, month and count visits
        val visitsPerProviderPerMonthDF = monthDF
        .groupBy("provider_id", "month")
        .agg(
            count("visit_id").as("total_visits")
            )

        visitsPerProviderPerMonthDF

  }    
}