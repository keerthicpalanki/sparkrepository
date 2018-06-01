package com.microsoft.jobs.order

import java.io.File
import java.util
import java.util.Properties

import com.microsoft.common.DataFrameUtil
import com.microsoft.config.ConfigReader
import com.microsoft.framework.{HiveUtils, VNextUtil}
import com.microsoft.jobs.subscription.SubscriptionDim.{getClass, processSubscriptionDim}
import org.apache.log4j.Logger
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, sum, _}

object SellerinsightsOrders {

  val log = Logger.getLogger(getClass().getName())
  var spark: SparkSession = null
  var inputArgsMap: Map[String, String] = null
  val warehouseLocation = new File("spark-warehouse").getAbsolutePath
  val configReader = new ConfigReader("sellerInsights.conf")

  def main(args: Array[String]): Unit = {

    val sqlQueries = new Properties

    inputArgsMap = VNextUtil.getInputParametersAsKeyValue(args)

    spark = SparkSession.builder
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()

    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    spark.conf.set("hive.mapred.supports.subdirectories", "true")
    spark.conf.set("mapred.input.dir.recursive", "true")

    sqlQueries.load(this.getClass.getClassLoader.getResourceAsStream("sellerInsights.properties"))

    prepareTables(sqlQueries)

    createSellerInsights(sqlQueries)

    val step1DF = createSellerInsightForUsage(sqlQueries)

    val step2DF = createSellerInsightForUsage2(sqlQueries)

    val unionDF = step1DF.union(step2DF)


  }

  def prepareTables(sqlProps : Properties):Unit ={

    val dbName = inputArgsMap.get("dbName").get

    val offerDF = DataFrameUtil.readDataFrameFromBlob("OfferDim", inputArgsMap, spark)
    HiveUtils.saveAsHiveTable(offerDF, "OfferDim", spark, dbName)
    offerDF.show()

    val orderDF = DataFrameUtil.readDataFrameFromBlob("OrderDim", inputArgsMap, spark)
    HiveUtils.saveAsHiveTable(orderDF, "OrderDim", spark, dbName)
    orderDF.show()

    val publisherDF = DataFrameUtil.readDataFrameFromBlob("PublisherDim", inputArgsMap, spark)
    HiveUtils.saveAsHiveTable(publisherDF, "PublisherDim", spark, dbName)
    publisherDF.show()

    val accountSubscriptionDF = DataFrameUtil.readDataFrameFromBlobAsTSV(configReader, "sellerInsights.ingest.tables.accountSubscription", "accountSubscription", inputArgsMap,  spark)
    HiveUtils.saveAsHiveTable(accountSubscriptionDF, "AccountSubscriptionDim", spark, dbName)
    accountSubscriptionDF.show()

    val enrollmentAccountDF = DataFrameUtil.readDataFrameFromBlobAsTSV(configReader, "sellerInsights.ingest.tables.enrollmentAccount", "enrollmentAccount", inputArgsMap,  spark)
    HiveUtils.saveAsHiveTable(enrollmentAccountDF, "EnrollmentAccountDim", spark, dbName)
    enrollmentAccountDF.show()

    val enrollmentDF = DataFrameUtil.readDataFrameFromBlob("EnrollmentDim", inputArgsMap, spark)
    HiveUtils.saveAsHiveTable(enrollmentDF, "EnrollmentDim", spark, dbName)
    enrollmentDF.show()

    val subscriptionDF = DataFrameUtil.readDataFrameFromBlob("SubscriptionDim", inputArgsMap, spark)
    HiveUtils.saveAsHiveTable(subscriptionDF, "SubscriptionDim", spark, dbName)
    subscriptionDF.show()

    val weeksTableDF = DataFrameUtil.readDataFrameFromBlob("WeeksTable", inputArgsMap, spark)
    HiveUtils.saveAsHiveTable(weeksTableDF, "WeeksTable", spark, dbName)

    val activeDF = DataFrameUtil.readDataFrameFromBlob("ActiveOrders", inputArgsMap, spark)
    HiveUtils.saveAsHiveTable(activeDF, "ActiveOrders", spark, dbName)

    val testAccountsDF = DataFrameUtil.readDataFrameFromBlob("TestAccounts", inputArgsMap, spark)
    HiveUtils.saveAsHiveTable(testAccountsDF, "TestAccounts", spark, dbName)

    val iaasCoreUsageDF = DataFrameUtil.readDataFrameFromBlob("IaasCoreUsage", inputArgsMap, spark)
    HiveUtils.saveAsHiveTable(iaasCoreUsageDF, "IaasCoreUsage", spark, dbName)

  }

  def createSellerInsights(sqlQueries:Properties) : DataFrame = {

    val dbName = inputArgsMap.get("dbName").get

    val rdfe1SQL = sqlQueries.get("rdfe1").toString

    val rdfe2SQL = sqlQueries.get("rdfe2").toString

    val orderV3SQL = sqlQueries.get("orderV3SQL").toString

    val finalOrderSQL = sqlQueries.get("ordersWeeklyforMktplacePortalSQL").toString

    HiveUtils.recreateHiveTable( "RDFE1", spark, rdfe1SQL, dbName)

    HiveUtils.recreateHiveTable( "RDFE2", spark, rdfe2SQL, dbName)

    HiveUtils.recreateHiveTable( "OrderV3Temp", spark, orderV3SQL, dbName)

    val df = HiveUtils.recreateHiveTable( "OrdersWeeklyMktPlacePortal", spark, finalOrderSQL, dbName)

    df
  }

  def createSellerInsightForUsage(sqlQueries:Properties) : DataFrame = {

    val dbName = inputArgsMap.get("dbName").get

    val orderUsageStep1InnerSQL= sqlQueries.get("firstUsage1Inner").toString
    val orderUsageStep1InnerDF = HiveUtils.recreateHiveTable( "ORDER_USAGE_STEP1_INNER", spark, orderUsageStep1InnerSQL, dbName)

    val weekSQL = sqlQueries.get("weeksSQL").toString
    val weeksDF = HiveUtils.executeHiveQuery(weekSQL, spark, dbName)

    val orderUsageStep1DF = orderUsageStep1InnerDF
      .withColumn("UsageDateTs", orderUsageStep1InnerDF.col("UsageDate").cast(DateType))
      .withColumn("UsageAfterCancelDateTs", orderUsageStep1InnerDF.col("UsageAfterCancelDateTs").cast(DateType))

    val weeks1DF = weeksDF
      .withColumn("startDateTs", weeksDF.col("startDate").cast(DateType))
      .withColumn("endDate", weeksDF.col("endDateTs").cast(DateType))

    val joinedStep1DF = orderUsageStep1DF.join(weeksDF, orderUsageStep1DF.col("UsageDate") >= weeksDF.col("startDateTS")  &&
      orderUsageStep1DF.col("UsageDate")  <= weeksDF.col("endDateTs"), "inner").select(
      orderUsageStep1DF.col("OrderId"),
      orderUsageStep1DF.col("OrderOrderId"),
      orderUsageStep1DF.col("OrderKey "),
      orderUsageStep1DF.col("OFFERNAMEOVERRIDE"),
      orderUsageStep1DF.col("OfferName"),
      orderUsageStep1DF.col("AzureSubscriptionId"),
      weeks1DF.col("StartDate").as("StartofWeek"),
      weeks1DF.col("EndDate").as("EndofWeek"),
      weeks1DF.col("RunningNumber").as("ISOWeek"),
      //weeksDF.col("Mon").as("MonthStartDate"),
      weeks1DF.col("VMSize"),
      orderUsageStep1DF.col("MeteredResourceGUID"),
      orderUsageStep1DF.col("UsageUnits"),
      orderUsageStep1DF.col("RawUsage"),
      orderUsageStep1DF.col("NormalizedUsage"),
      orderUsageStep1DF.col("Quantity"),
      orderUsageStep1DF.col("NumberOfCores"),
      orderUsageStep1DF.col("UsageDate"),
      orderUsageStep1DF.col("IsMultiSolution"),
      orderUsageStep1DF.col("ResourceURI"),
      orderUsageStep1DF.col("VMId"),
      orderUsageStep1DF.col("IsVMUsage")
    )

    val groupedStep1DF = joinedStep1DF
      .groupBy(
        joinedStep1DF.col("OrderId"),
        joinedStep1DF.col("AzureSubscriptionId"),
        joinedStep1DF.col("StartDate").as("StartofWeek"),
        joinedStep1DF.col("EndDate").as("EndofWeek"),
        joinedStep1DF.col("RunningNumber").as("ISOWeek"),
        //weeksDF.col("Mon").as("MonthStartDate"),
        joinedStep1DF.col("VMSize"),
        joinedStep1DF.col("MeteredResourceGUID"),
        joinedStep1DF.col("UsageUnits"),
        joinedStep1DF.col("NumberOfCores"),
        joinedStep1DF.col("UsageDate"),
        joinedStep1DF.col("OrderOrderId"),
        joinedStep1DF.col("OfferNameOverride"),
        joinedStep1DF.col("OfferName"),
        joinedStep1DF.col("IsMultiSolution"),
        joinedStep1DF.col("ResourceURI"),
        joinedStep1DF.col("VMId"),
        joinedStep1DF.col("IsVMUsage"),
        joinedStep1DF.col("OrderKey "),
        joinedStep1DF.col("RawUsage"),
        joinedStep1DF.col("NormalizedUsage")
      ).agg(sum(joinedStep1DF.col("Quantity")).as("MeteredUsage"))

    groupedStep1DF
  }

  def createSellerInsightForUsage2(sqlQueries : Properties ): DataFrame = {

    val dbName = inputArgsMap.get("dbName").get

    val orderUsageStep2InnerSQL= sqlQueries.get("secondUsageInner").toString
    val orderUsageStep2InnerDF = HiveUtils.recreateHiveTable( "ORDER_USAGE_STEP2_INNER", spark, orderUsageStep2InnerSQL, dbName)

    val weekSQL = sqlQueries.get("weeksSQL").toString
    val weeksDF = HiveUtils.executeHiveQuery(weekSQL, spark, dbName)

    val orderUsageStep2DF = orderUsageStep2InnerDF
      .withColumn("UsageDateTs", orderUsageStep2InnerDF.col("UsageDate").cast(DateType))
      .withColumn("UsageAfterCancelDateTs", orderUsageStep2InnerDF.col("UsageAfterCancelDateTs").cast(DateType))

    val weeks2DF = weeksDF
      .withColumn("startDateTs", weeksDF.col("startDate").cast(DateType))
      .withColumn("endDate", weeksDF.col("endDateTs").cast(DateType))

    val joinedStep2DF = orderUsageStep2DF.join(weeksDF, orderUsageStep2DF.col("UsageDate") >= weeksDF.col("startDateTS")  &&
      orderUsageStep2DF.col("UsageDate")  <= weeksDF.col("endDateTs"), "inner").select(
      orderUsageStep2DF.col("OrderId"),
      orderUsageStep2DF.col("OrderOrderId"),
      orderUsageStep2DF.col("OrderKey "),
      orderUsageStep2DF.col("OFFERNAMEOVERRIDE"),
      orderUsageStep2DF.col("OfferName"),
      orderUsageStep2DF.col("AzureSubscriptionId"),
      weeks2DF.col("StartDate").as("StartofWeek"),
      weeks2DF.col("EndDate").as("EndofWeek"),
      weeks2DF.col("RunningNumber").as("ISOWeek"),
      //weeksDF.col("Mon").as("MonthStartDate"),
      orderUsageStep2DF.col("VMSize"),
      orderUsageStep2DF.col("MeteredResourceGUID"),
      orderUsageStep2DF.col("UsageUnits"),
      orderUsageStep2DF.col("Quantity"),
      orderUsageStep2DF.col("NumberOfCores"),
      orderUsageStep2DF.col("MeteredUsage"),
      orderUsageStep2DF.col("UsageDate"),
      orderUsageStep2DF.col("IsMultiSolution"),
      orderUsageStep2DF.col("ResourceURI"),
      orderUsageStep2DF.col("VMId"),
      orderUsageStep2DF.col("IsVMUsage")
    )

    val groupedStep2DF = joinedStep2DF
      .groupBy(
        joinedStep2DF.col("OrderId"),
        joinedStep2DF.col("AzureSubscriptionId"),
        joinedStep2DF.col("StartDate"),
        joinedStep2DF.col("EndDate"),
        joinedStep2DF.col("RunningNumber"),
        joinedStep2DF.col("VMSize"),
        joinedStep2DF.col("MeteredResourceGUID"),
        joinedStep2DF.col("NumberOfCores"),
        joinedStep2DF.col("UsageDate"),
        joinedStep2DF.col("OrderOrderId"),
        joinedStep2DF.col("Quantity"),
        joinedStep2DF.col("OfferNameOverride"),
        joinedStep2DF.col("OfferName"),
        joinedStep2DF.col("IsMultiSolution"),
        joinedStep2DF.col("ResourceURI"),
        joinedStep2DF.col("VMId"),
        joinedStep2DF.col("IsVMUsage"),
        joinedStep2DF.col("OrderKey "),
        joinedStep2DF.col("UsageUnits"),
        joinedStep2DF.col("RawUsage"),
        joinedStep2DF.col("NormalizedUsage")
      ).agg(sum(joinedStep2DF.col("Quantity")).as("RawUsage"), sum(joinedStep2DF.col("Quantity") * joinedStep2DF.col("NumberOfCores")).as("NormalizedUsage"))

    groupedStep2DF

  }

  def processStep3(sqlQueries : Properties) : DataFrame = {

    val dbName = inputArgsMap.get("dbName").get

    val step3UsageSQL= sqlQueries.get("step3UsageSQL").toString
    val step3UsageDF = HiveUtils.recreateHiveTable( "UsageStep3Table", spark, step3UsageSQL, dbName)
    step3UsageDF
  }

}

