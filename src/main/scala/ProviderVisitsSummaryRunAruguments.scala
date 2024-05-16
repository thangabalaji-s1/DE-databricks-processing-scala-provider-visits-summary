package com.availity.spark.batch

import org.apache.spark.internal.Logging

case class ProviderVisitsSummaryRunAruguments(
                                   isLocalMode: Boolean,
                                   sparkMaster: String,
                                   providersInputDirectory: String,
                                   visitsInputDirectory: String
                                    )

object ProviderVisitsSummaryRunAruguments extends Logging {

    def apply(args: Array[String],
                providersInputDirectoryDefault: String,
                visitsInputDirectoryDefault: String) : ProviderVisitsSummaryRunAruguments = {
                
        val isLocalMode = if (args.length == 0) true else false
        val sparkMaster = if (isLocalMode) "local[*]" else "yarn"
        val providersInputDirectory = if (isLocalMode) providersInputDirectoryDefault else args(0)
        val visitsInputDirectory = if (isLocalMode) visitsInputDirectoryDefault else args(1)
        

        ProviderVisitsSummaryRunAruguments(
            isLocalMode,
            sparkMaster,
            providersInputDirectory,
            visitsInputDirectoryDefault
        )
}
    
}

