package com.microsoft.jobs.usage.sql

import java.util.{Calendar, Properties, UUID}

import com.microsoft.common.DataFrameUtil
import com.microsoft.config.ConfigReader
import com.microsoft.framework.VNextUtil
import com.microsoft.jobs.order.ProcessOrders._
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, RelationalGroupedDataset, SparkSession}
import org.apache.spark.sql.functions.{when, _}
import org.apache.spark.sql.types._
import java.io.File

import com.microsoft.azure.storage.CloudStorageAccount
import com.microsoft.azure.storage.blob.CloudBlobClient
import com.microsoft.azure.storage.blob.CloudBlobContainer
import com.microsoft.azure.storage.blob.CloudBlobDirectory
import com.microsoft.azure.storage.blob.ListBlobItem
import java.io.File
import java.net.URI
import java.text.SimpleDateFormat
import java.util

import org.apache.spark.sql._

import scala._
import scala.collection.immutable.Map
import scala.collection.immutable.StringOps
import scala.collection.mutable
//import scala.reflect.ClassTag$
import scala.runtime._


object SqlUsageDelta {

  var spark: SparkSession = _
  val configReader = new ConfigReader("orderUsageDeltaSql.conf")
  var inputArgsMap: Map[String, String] = _
  val sqlQueries = new Properties
  private val log = Logger.getLogger(getClass().getName())
  val warehouseLocation = new File("spark-warehouse").getAbsolutePath
  var hiveDB = "default"
  val TABLE_ORDER_USAGE_FACT = "OrderUsageFact"
  val MAX_USAGE_DATE_PATTERN = "yyyy-MM-dd"

  var DataLineageID = UUID.randomUUID().toString

  val format = new SimpleDateFormat("yyyy-MM-dd")

  val dtStr = format.format(Calendar.getInstance().getTime())
  val now = format.parse(dtStr)
  val currDate = Calendar.getInstance()
  currDate.setTime(now)
  currDate.add(Calendar.DATE, -6)

  val MAX_USAGE_DATE_DEFAULT = format.format(currDate.getTime).toString

  var maxUsageDatesMap = scala.collection.mutable.Map[String, String] (
    "MAX_VM_USAGE_DATE" -> MAX_USAGE_DATE_DEFAULT,
    "MAX_CORE_USAGE_DATE" -> MAX_USAGE_DATE_DEFAULT,
    "MAX_MS_USAGE_DATE" -> MAX_USAGE_DATE_DEFAULT,
    "MAX_3P_USAGE_DATE" -> MAX_USAGE_DATE_DEFAULT
  )

  private def readMaxUsageArgs() : Unit = {
    maxUsageDatesMap.foreach {
      case (key, value) => {
        if (inputArgsMap.get(key) != null && inputArgsMap.get(key) != None) {
          maxUsageDatesMap(key) = inputArgsMap.get(key).get
          val error = validateDateFormat(inputArgsMap.get(key).get, MAX_USAGE_DATE_PATTERN)
          if (!error.isEmpty) {
            log.error(error)
            System.exit(-1)
          }
        }
      }
    }
  }

  private def readArgument(argName : String, defaultValue : String) : String = {
    var argValue = defaultValue
    if (inputArgsMap.get(argName) != null && inputArgsMap.get(argName) != None) {
      argValue = inputArgsMap.get(argName).get
    }
    argValue
  }
  /**
    * This function reads/sets configurations and does initializations
    */
  def setup(args: Array[String]) : Unit = {

    spark = SparkSession.builder
//        .master("local[*]")
        .appName("OrderUsageDelta")
        .config("spark.sql.warehouse.dir", warehouseLocation)

      .enableHiveSupport()
      .getOrCreate()

    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    spark.conf.set("hive.mapred.supports.subdirectories", "true")
    spark.conf.set("mapred.input.dir.recursive", "true")
//    spark.conf.set("hive.merge.mapfiles", "true")
//    spark.conf.set("hive.merge.mapredfiles", "true")
//    spark.conf.set("hive.merge.tezfiles", "true")

    spark.sql("ADD JAR wasb://jsonserde@azuredevopsdevrawstore1.blob.core.windows.net/json/json-serde-1.3.9.jar")
    sqlQueries.load(this.getClass.getClassLoader.getResourceAsStream("sqlQueries.properties"))

    // Input sanitizations
    inputArgsMap = VNextUtil.getInputParametersAsKeyValue(args)
    val readPathFromConfig = VNextUtil.isReadPathsFromConfig(inputArgsMap)
    // Update maxUsageDate if passed as command line parameter.
    if (! readPathFromConfig) {
      readMaxUsageArgs()
      hiveDB = readArgument("hiveDB", hiveDB)
      if (inputArgsMap.get("DataLineageID") != null && inputArgsMap.get("DataLineageID") != None){
        DataLineageID = inputArgsMap.get("DataLineageID").get
      }
    }
    log.info("******* maxUsageDatesMap : " + maxUsageDatesMap)
    log.info("******* hiveDB : " + hiveDB)
  }

  private def usage()={
    log.error("Following arguments are mandatory maxUsageDate=<Max-Usage-Date> container=<container name> account=<account name> groupSize=<batch size of files to read>")
  }

  def main(args: Array[String]): Unit = {

    if (args.length < 1) {
      usage
      System.exit(-1)
    }

    val startTime = System.nanoTime
    try {
      setup(args)
/*
      // Preparing for steps.
      preliminarySteps()

      // Step 1 queries
      prepareForStep1()
      val orderUsageFactStep1DF = executeHiveQuery("queryStep1")
//      log.debug("******* orderUsageFactStep1DF.count() : " + orderUsageFactStep1DF.count())
      //orderUsageFactStep1DF.printSchema()

	  // Step 2 queries
      prepareForStep2()
      val orderUsageFactStep2DF = executeHiveQuery("queryStep2")
//      log.debug("******* orderUsageFactStep2DF.count() : " + orderUsageFactStep2DF.count())
      //orderUsageFactStep2DF.printSchema()

      // Union both the above DFs.
      val orderUsageFactStep12DF = orderUsageFactStep1DF.union(orderUsageFactStep2DF)
//      log.debug("******* orderUsageFactStep12DF.count() : " + orderUsageFactStep12DF.count())
     // orderUsageFactStep12DF.show()

      // Step 3 queries


      //recreateHiveTable("OrderAPIPricesBCP")

      val orderUsageFactCoreOrderDF = executeHiveQuery("queryStep3CoreOrder")
      //Now, union with DF from last step.
      val orderUsageFactStep1_3DF = orderUsageFactStep12DF.union(orderUsageFactCoreOrderDF)


      // Step 4 (MultiSolution) queries
      prepareForStep4()
/*
      val CUDdf = spark.sql("SELECT * FROM Cosmos_Marketplace_UsageDaily")
      val CUDRepartitionDF = CUDdf.repartition(20)
      val cosmosMultisolutionUsageDF = DataFrameUtil.readDataFrameFromBlobAsTSV(configReader, "ordersUsage.ingest.tables.multiSolutionTemplate", "multiSolutionTemplate", inputArgsMap, spark)

      val resourceUriDF = cosmosMultisolutionUsageDF.select(lower(col("resourceUri")).as("resourceUri1")).distinct()

      val CUDFinalDF  = CUDRepartitionDF.join(resourceUriDF, lower(CUDRepartitionDF.col("resourceuri")) === lower(resourceUriDF.col("resourceUri1")))
      log.debug("*************** CUDFinalDF.count() : " +  CUDFinalDF.count())
      CUDFinalDF.write.mode("overwrite").saveAsTable("Cosmos_UsageDaily_MultiSolution")
*/
      recreateHiveTable("Cosmos_UsageDaily_MultiSolution_Compute_DIM")
      val orderUsageFactMsDF = executeHiveQuery("queryStep4OrderUsageFact")

      // Union with last DF
      val orderUsageFactStep1_4DF = orderUsageFactStep1_3DF.union(orderUsageFactMsDF)
      //log.debug("******* orderUsageFactStep1_4DF.count() : " + orderUsageFactStep1_4DF.count())


//      orderUsageFactStep1_4DF.createOrReplaceTempView("OrderUsageFact_Temp")
      orderUsageFactStep1_4DF.write.mode(SaveMode.Overwrite).saveAsTable("OrderUsageFact_Temp1")

      // Step 5 : VMSizes and Prices
      recreateHiveTable("VMSizes")

      recreateHiveTable("Prices")

      val orderUsageFactUpdatedDF = executeHiveQuery("query3rdPartyMultiSol")
      orderUsageFactUpdatedDF.write.mode(SaveMode.Overwrite).saveAsTable("OrderUsageFact_Stg")

      //recreateHiveTable("RankedUsageLocation")
*/
      val orderUsageFactDF = executeHiveQuery("queryOrderUsageFact")
      spark.sql("DROP TABLE IF EXISTS OrderUsageFactTemp")
      orderUsageFactDF.withColumn("DataLineageID", lit(DataLineageID)).write.mode(SaveMode.Overwrite).saveAsTable("OrderUsageFactTemp")
//      spark.sql("DROP TABLE IF EXISTS OrderUsageFact1")
//      orderUsageFactDF.withColumn("DataLineageID", lit(DataLineageID)).write.mode(SaveMode.Overwrite).partitionBy("usageDate","IsMissing").saveAsTable("OrderUsageFact2")
//      spark.sql("INSERT INTO TABLE OrderUsageFact3 PARTITION (UsageDate, ismissing) SELECT OrderId,MeteredResourceGUID,SubscriptionGuid,ResourceURI,ResourceURIHash,AzureRegion,Project,ServiceInfo1,ServiceInfo2,NumberOfCores,VMSize,PartNumber,Tags,Properties,Quantity,CoreQuantity,IsCoreImage,IsMultiSolution,IsVMUsage,TemplateUri,CorrelationId,MultiSolutionOfferName,UsageDataSource,UsageAfterCancelDate,VMUsageDefaultPrice,InfluencedCost,VMComputeStack,Cluster,OrderState,AdditionalInfo,publishername,UsageType,VMId,DataLineageID,UsageDate,IsMissing FROM OrderUsageFactTemp")

    /*
      executeHiveQuery("createUsageFactIfNotExists")
      log.info("******* Executed createUsageFactIfNotExists successfully.")
      spark.sql("USE " + hiveDB)
      spark.sql("INSERT INTO TABLE OrderUsageFact PARTITION (UsageDate, ismissing) SELECT OrderId,MeteredResourceGUID,SubscriptionGuid,ResourceURI,ResourceURIHash,AzureRegion,Project,ServiceInfo1,ServiceInfo2,NumberOfCores,VMSize,PartNumber,Tags,Properties,Quantity,CoreQuantity,IsCoreImage,IsMultiSolution,IsVMUsage,TemplateUri,CorrelationId,MultiSolutionOfferName,UsageDataSource,UsageAfterCancelDate,VMUsageDefaultPrice,InfluencedCost,VMComputeStack,Cluster,OrderState,AdditionalInfo,publishername,UsageType,VMId,DataLineageID,UsageDate,IsMissing FROM OrderUsageFactTemp")
      log.info("******* Data written to the (recreated) table " + TABLE_ORDER_USAGE_FACT)
*/
//      spark.sql("DROP TABLE IF EXISTS OrderUsageFact_Temp1")
//      spark.sql("DROP TABLE IF EXISTS OrderUsageFact_Stg")


    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }

    val elapsed = (System.nanoTime - startTime) / 1e9d
    log.info("Completed. Elapsed Time : " + elapsed + " seconds")

  }

  private def preliminarySteps() : Unit = {
    // Initial queries.
    //val ordersDF = executeHiveQuery("queryOrderDim")
    //val apiPriceDF = executeHiveQuery("queryOrderApiPrices")
    recreateHiveTable("OrderAPIOrderIds")
  }

  private def prepareForStep1() : Unit = {
    recreateHiveTable("OrdersAPIPricesTmp")
    recreateHiveTable("OrdersAPILatestPrices")
    recreateHiveTable("VMUsage")
    recreateHiveTable("OrdersAPILatestPricesTmp")
  }

  private def prepareForStep2() : Unit = {
    recreateHiveTable("MeteredUsage")
  }
  private def prepareForStep3() : Unit = {
    val cosmosCoreUsageBCP_DF = DataFrameUtil.readDataFrameFromBlob(configReader, "ordersUsage.ingest.tables.cosmosComputeUsageBCP", "cosmosComputeUsageBCP", inputArgsMap, spark)
//    log.debug("******* cosmosCoreUsageBCP_DF.count() : " + cosmosCoreUsageBCP_DF.count())
    saveAsHiveTable(cosmosCoreUsageBCP_DF, "Cosmos_Marketplace_Core_ComputeUsage_BCP")

    recreateHiveTable("OrderAPIPricesBCP")
  }

  private def prepareForStep4() : Unit = {
    val cosmosMultisolutionUsageDF = DataFrameUtil.readDataFrameFromBlobAsTSV(configReader, "ordersUsage.ingest.tables.multiSolutionTemplate", "multiSolutionTemplate", inputArgsMap, spark)
//    log.debug("******* cosmosMultisolutionUsageDF.count() : " + cosmosMultisolutionUsageDF.count())
    val resourceUriDF = cosmosMultisolutionUsageDF.select(lower(col("resourceUri")).as("resourceUri1")).distinct()
//    log.debug("******* resourceUriDF.count() : " + resourceUriDF.count())
    saveAsHiveTable(resourceUriDF, "MultiSolutionTemplate")

    // Join the above table MultiSolutionTemplate with Cosmos_Marketplace_UsageDaily
    recreateHiveTable("Cosmos_UsageDaily_MultiSolution")
  }

  /**
    * Recreates a Hive table after dropping it using configured SQL.
    * @param hiveTableName
    */
  def recreateHiveTable(hiveTableName : String): Unit = {
    val df = executeHiveQuery("hiveQuery" + hiveTableName)
    saveAsHiveTable(df, hiveTableName)
  }

  /**
    *  To execute a Hive query. It replaces MAX_USAGE_DATE first.
    * @param queryName
    * @return
    */
  def executeHiveQuery(queryName : String) : DataFrame = {
    var query = sqlQueries.getProperty(queryName)
    maxUsageDatesMap foreach {
      case (key, value) => {
        query = query.replaceAllLiterally(key, value)
      }
    }

    log.info("******* " + queryName + " : " + query)
    spark.sql("USE " + hiveDB)
    val df = spark.sql(query)

    df
  }

  /**
    * Saves a dataframe as the specified Hive table.
    * @param df
    * @param hiveTableName
    */
  def saveAsHiveTable(df: DataFrame, hiveTableName : String): Unit = {
    if(df != null) {
      val dropQuery = getDropQuery(hiveTableName)
      log.info("******* dropQuery  : " + dropQuery)
      spark.sql("USE " + hiveDB)
      spark.sql(dropQuery)
      df.write.mode(SaveMode.Overwrite).saveAsTable(hiveTableName)
      log.info("******* Data written to the (recreated) table " + hiveTableName)
    }
  }

  /**
    * Inserts a dataframe data to an existing Hive table.
    * @param df
    * @param hiveTableName
    */
  def insertIntoHiveTable(df: DataFrame, hiveTableName : String): Unit = {
    if(df != null && df.count() > 0) {
      val tmpTable = "tmp" + hiveTableName
      df.createOrReplaceTempView(tmpTable)
      val insertTableQuery = "insert into table " + hiveTableName + " select * from " + tmpTable
      log.info("******* insertTableQuery  : " + insertTableQuery)
      spark.sql("USE " + hiveDB)
      spark.sql(insertTableQuery)
    }
  }

  def getDropQuery(hiveTableName : String) : String = {
    "DROP TABLE IF EXISTS " + hiveTableName
  }

  /**
    * Validate date format based upon the specified date-pattern, e.g., MM/dd/yyyy or yyyy-MM-dd
    * @param inputDate
    * @param datePattern
    * @return
    */
  def validateDateFormat(inputDate : String, datePattern : String) : String = {
    var err = ""
    try {
      import org.joda.time.format._
      val fmt = DateTimeFormat forPattern datePattern
      val output = fmt parseDateTime inputDate
    } catch {
      case i: IllegalArgumentException => {
        err = "Invalid date format : " + inputDate
      }
      case e: Exception => {
        err = "Invalid date format : (" + inputDate + "). The error is : " + e.getMessage
      }
    }
    err
  }

}
