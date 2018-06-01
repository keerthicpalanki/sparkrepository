package com.microsoft.jobs.subscription

import java.io.File

import com.microsoft.common.DataFrameUtil
import com.microsoft.config.ConfigReader
import com.microsoft.framework.VNextUtil
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SubscriptionDim {

  val log = Logger.getLogger(getClass().getName())
  var spark: SparkSession = null
  var inputArgsMap: Map[String, String] = null
  val configReader = new ConfigReader("subscription.conf")

  def main(args: Array[String]): Unit = {

    if (args.length < 1) {
      log.error("Usage : Application requires minimum 1 argument")
      System.exit(-1)
    }

    val warehouseLocation = new File("spark-warehouse").getAbsolutePath

    spark = SparkSession.builder
      .getOrCreate()
    inputArgsMap = VNextUtil.getInputParametersAsKeyValue(args)
    processSubscriptionDim()
  }

  def processSubscriptionDim() ={

    val subscriptionIdFile = inputArgsMap.get("orderSubscription").get
    val stageSubscription =  inputArgsMap.get("subscription").get
    val subscriptionDimPath =  inputArgsMap.get("subscriptionDim").get

    val subscriptionDF = spark.read.parquet(subscriptionIdFile)
    val stageSubscriptionDF = DataFrameUtil.readDataFrameFromBlobAsTSV(configReader, "subscription.ingest.tables.subscription", "subscription", inputArgsMap, spark)

    val subscriptionDim = subscriptionDF.join(stageSubscriptionDF, lower(subscriptionDF.col("AzureSubscriptionId")) === lower(stageSubscriptionDF.col("AI_SubscriptionKey")), "inner")

    subscriptionDim.write.json(subscriptionDimPath)

  }
}