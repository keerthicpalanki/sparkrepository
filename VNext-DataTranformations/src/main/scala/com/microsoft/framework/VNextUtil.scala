package com.microsoft.framework

import java.io.FileInputStream
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import com.microsoft.azure.storage.CloudStorageAccount
import com.microsoft.azure.storage.blob.CloudBlobClient
import com.microsoft.common.{DataFrameUtil, PushToSQL}
import com.microsoft.config.ConfigReader
import com.microsoft.jobs.publisher.ProcessPublisherDim.{configReader, generateRenamedDF, parseSelectExprForAlias}
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, LongType, StringType}

import scala.util.Try

object VNextUtil extends  java.io.Serializable {

  val TIMESTAMP_MAX_VAL = 253370745000000l // 01 Jan 9999
  val log = Logger.getLogger(getClass().getName())

  def getInputParametersAsKeyValue(args: Array[String]): Map[String, String] = {
    log.info("The input parameters are ")

    if (args.length == 0)
      null
    else {
      args.foreach(x => log.info(x))
      args.filter(x => x.contains("=")).map(x => x.split("=", 2)).map(x => (x(0), x(1))).toMap
    }
  }

  def isReadPathsFromConfig(inputArgsMap: Map[String, String]): Boolean = {
    if (inputArgsMap.get("readPathFromConfig") != null && inputArgsMap.get("readPathFromConfig").get == "true")
      true
    else
      false
  }

  def getFileFromBlob(configReader: ConfigReader, spark: SparkSession, filePath: String): DataFrame = {

    try {
      val df = spark.read.json(filePath)
      df
    } catch {
      case e: Exception => {
        e.printStackTrace(); val df = null; df
      }
    }
  }

  def removeDuplicates(finalDF : DataFrame): DataFrame = {

    try {
      var finalMergedDF = finalDF.withColumn("Level", when(lower(col("DataSource")) === "pubportal",1).otherwise(2))

      finalMergedDF= DataFrameUtil.partitionDataFrame(finalMergedDF, "rowKey", "Level Asc", "SourceRank", 1)

      finalMergedDF =  finalMergedDF.filter(col("SourceRank") === 1).drop("SourceRank").drop("Level")
      finalMergedDF
    } catch {
      case e: Exception => {
        e.printStackTrace(); val df = null; df
      }
    }
  }

  def getODataOffers(finalDF: DataFrame, spark: SparkSession, inputArgsMap: Map[String, String]): DataFrame = {

    try {

      var filePath = getPropertyValue(inputArgsMap, "oDataPath")

      var filteredDF = spark.read.json(filePath)

      filteredDF = filteredDF.withColumn("IsMultiSolution", filteredDF.col("IsMultiSolution").cast(StringType))
      filteredDF = filteredDF.withColumn("IsExternallyBilled", filteredDF.col("IsExternallyBilled").cast(StringType))
      filteredDF = filteredDF.withColumn("IsUBB", filteredDF.col("IsUBB").cast(StringType))
      filteredDF = filteredDF.withColumn("version", filteredDF.col("version").cast(StringType))
      filteredDF = filteredDF.withColumn("versionrank", filteredDF.col("versionrank").cast(StringType))
      filteredDF = DataFrameUtil.convertDateColumnsToString(filteredDF, Seq("OfferCreatedDate"))

      val newfinalDF = DataFrameUtil.union(finalDF, filteredDF)

      newfinalDF

    }catch {
      case e: Exception => {
        e.printStackTrace(); val df = null; df
      }
    }
  }

  def getODataPublishers(finalDF: DataFrame, spark: SparkSession, inputArgsMap: Map[String, String]): DataFrame = {

    try {

      var filePath = getPropertyValue(inputArgsMap, "oDataPath")

      var filteredDF = spark.read.json(filePath)

      filteredDF = filteredDF.withColumn("IsInternalPublisher", filteredDF.col("IsInternalPublisher").cast(LongType))

      filteredDF = filteredDF.withColumn("IsSCDActive", filteredDF.col("IsSCDActive").cast(IntegerType))

      val newfinalDF = DataFrameUtil.union(finalDF, filteredDF)

      newfinalDF

    } catch {
      case e: Exception => {
        e.printStackTrace();
        val df = null;
        df
      }
    }
  }
  def getAMDMOrders(finalDF: DataFrame, spark: SparkSession, inputArgsMap: Map[String, String]): DataFrame = {

    try {

      var filePath = getPropertyValue(inputArgsMap, "oDataPath")

      var filteredDF = spark.read.json(filePath)

      filteredDF = DataFrameUtil.convertDateColumnsToString(filteredDF, Seq("CancelDate", "PurchaseDate", "ScheduledCancellationDateFromSO", "TrialEndDate"))

      filteredDF = filteredDF.withColumn("IsMultiSolution", filteredDF.col("IsMultiSolution").cast(IntegerType))
      filteredDF = filteredDF.withColumn("IsTrial", filteredDF.col("IsTrial").cast(IntegerType))
      filteredDF = filteredDF.withColumn("PurchaseDateLong", filteredDF.col("PurchaseDateLong").cast(LongType))
      filteredDF = filteredDF.withColumn("CancelDateLong", filteredDF.col("CancelDateLong").cast(LongType))
      filteredDF = filteredDF.withColumn("IsPreview", filteredDF.col("IsPreview").cast(StringType))
      filteredDF = filteredDF.withColumn("TrialEndDateLong", filteredDF.col("TrialEndDateLong").cast(LongType))
      filteredDF = filteredDF.withColumn("ResourceURIHash", md5(lower(filteredDF("ResourceURI"))).cast("string"))
      filteredDF = filteredDF.withColumn("OfferRowKey",concat_ws("_", when(filteredDF.col("PublisherName").isNotNull ,
        filteredDF.col("PublisherName")).otherwise(""),when(filteredDF.col("OfferName").isNotNull ,
        filteredDF.col("OfferName")).otherwise(""),when(filteredDF.col("ServicePlanName").isNotNull ,
        filteredDF.col("ServicePlanName")).otherwise("")))
      val newfinalDF = DataFrameUtil.union(finalDF, filteredDF)

      newfinalDF

    }catch {
      case e: Exception => {
        e.printStackTrace(); val df = null; df
      }
    }
  }

  def processFinalDataFrameNew(configReader: ConfigReader, spark: SparkSession, finalDF1: DataFrame, historyPath: String,
                               mergePath: String, inputArgsMap: Map[String, String], sqlTableName:String) : Unit = {

    var finalDF = finalDF1.dropDuplicates("rowKey")

    finalDF.cache()

    var historyFilePath = getPropertyValue(inputArgsMap, "historicalPath")

    log.info("The current history path is " + historyFilePath)

    val historyFilePathPrevious = extractPreviousPath(inputArgsMap, "historicalPath")

    log.info("The history file path is (for merge) " + historyFilePathPrevious)

    val historyDF = if (historyFilePathPrevious != null) VNextUtil.getFileFromBlob(configReader, spark, historyFilePathPrevious) else null

    var mergedFilePath = getPropertyValue(inputArgsMap, "mergedPath")

    log.info("The current merged path is " + mergedFilePath)

    val mergedFilePathPrevious = extractPreviousPath(inputArgsMap, "mergedPath")

    log.info("The older merged file path is " + mergedFilePathPrevious)

    val mergedDF = if ( mergedFilePathPrevious != null ) VNextUtil.getFileFromBlob(configReader, spark, mergedFilePathPrevious) else null

    if ((mergedDF == null && historyDF == null)  && (sqlTableName == "OfferDim" || sqlTableName == "PublisherDim" || sqlTableName== "OrderDim")) {

      if(sqlTableName == "OfferDim") {
        finalDF = VNextUtil.getODataOffers(finalDF, spark, inputArgsMap)
      }
      else if(sqlTableName == "PublisherDim") {
        finalDF = VNextUtil.getODataPublishers(finalDF, spark, inputArgsMap)
      }
      else {
        finalDF = VNextUtil.getAMDMOrders(finalDF, spark, inputArgsMap)
      }
    }

    if(hasColumn(finalDF, "PayoutAccountId") && sqlTableName == "PublisherDim") {
      finalDF = finalDF.drop("PayoutAccountId")
    }

    var finalMergedDF = if (mergedDF == null) finalDF else MergeDF.insertUpdateDataFrames(mergedDF, finalDF, null )

    var finalHistoricalDF = if (historyDF == null) finalDF else MergeDF.getHistoricalDF(historyDF, finalDF)

    if(historyDF == null)
      finalHistoricalDF = finalHistoricalDF.withColumn("StartDate", unix_timestamp()).withColumn("EndDate", lit(TIMESTAMP_MAX_VAL).cast(LongType))

    log.info("************** NUMBER OF PARTITIONS ARE ************* " + finalMergedDF.rdd.getNumPartitions)

    if(sqlTableName == "OfferDim" || sqlTableName == "PublisherDim") {
      finalMergedDF = removeDuplicates(finalMergedDF)
      finalHistoricalDF = removeDuplicates(finalHistoricalDF)
    }

    //if(hasColumn(finalMergedDF, "PayoutAccountId") && hasColumn(finalHistoricalDF, "PayoutAccountId")) {
     // finalMergedDF = finalMergedDF.drop("PayoutAccountId")
     // finalHistoricalDF = finalHistoricalDF.drop("PayoutAccountId")
   // }

    finalMergedDF.write.mode(SaveMode.Overwrite).json(mergedFilePath)

    finalHistoricalDF.write.mode(SaveMode.Overwrite).json(historyFilePath)
    finalDF.unpersist()

    var pushToSQL = true
    if ( inputArgsMap.get("pushToSQL") != None && inputArgsMap.get("pushToSQL").get.equalsIgnoreCase("false")) {
      pushToSQL = false
    }

    if (pushToSQL && sqlTableName != null) {
      log.info("Started Writing to sql server table " + sqlTableName)
      PushToSQL.DataPullFromDF(finalMergedDF,sqlTableName,"O",spark)
      log.info("Finished Writing to sql server table " + sqlTableName)
    }

    if (pushToSQL && sqlTableName != null) {
      log.info("Started Writing to sql server table " + sqlTableName)
      PushToSQL.DataPullFromDF(finalHistoricalDF,sqlTableName + "Historical","O",spark)
      log.info("Finished Writing to sql server table " + sqlTableName)
    }

  }

  def processFinalDataFrameApi(configReader: ConfigReader, spark: SparkSession, finalDF1: DataFrame, historyPath: String,
                            mergePath: String, inputArgsMap: Map[String, String], sqlTableName:String) : Unit = {

    val finalDF = finalDF1.dropDuplicates("rowKey")

    finalDF.cache()

    var historyFilePath = getPropertyValue(inputArgsMap, "apiHistoricalPath")

    log.info("The current history path is " + historyFilePath)

    val historyFilePathPrevious = extractPreviousPath(inputArgsMap, "apiHistoricalPath")

    log.info("The history file path is (for merge) " + historyFilePathPrevious)

    val historyDF = if (historyFilePathPrevious != null) VNextUtil.getFileFromBlob(configReader, spark, historyFilePathPrevious) else null

    var mergedFilePath = getPropertyValue(inputArgsMap, "apiMergedPath")

    log.info("The current merged path is " + mergedFilePath)

    val mergedFilePathPrevious = extractPreviousPath(inputArgsMap, "apiMergedPath")

    log.info("The older merged file path is " + mergedFilePathPrevious)

    val mergedDF = if ( mergedFilePathPrevious != null ) VNextUtil.getFileFromBlob(configReader, spark, mergedFilePathPrevious) else null

    val finalMergedDF = if (mergedDF == null) finalDF else MergeDF.insertUpdateDataFrames(mergedDF, finalDF, null )

    var finalHistoricalDF = if (historyDF == null) finalDF else MergeDF.getHistoricalDF(historyDF, finalDF)

    if(historyDF == null)
      finalHistoricalDF = finalHistoricalDF.withColumn("StartDate", unix_timestamp()).withColumn("EndDate", lit(TIMESTAMP_MAX_VAL).cast(LongType))

    log.info("************** NUMBER OF PARTITIONS ARE ************* " + finalMergedDF.rdd.getNumPartitions)

    finalMergedDF.write.mode(SaveMode.Overwrite).json(mergedFilePath)

    finalHistoricalDF.write.mode(SaveMode.Overwrite).json(historyFilePath)
    finalDF.unpersist()

    var pushToSQL = true
    if ( inputArgsMap.get("pushToSQL") != null && inputArgsMap.get("pushToSQL").get.equalsIgnoreCase("false")) {
      pushToSQL = false
    }

    if (pushToSQL && sqlTableName != null) {
      log.info("Started Writing to sql server table " + sqlTableName)
      PushToSQL.DataPullFromDF(finalMergedDF,sqlTableName,"O",spark)
      log.info("Finished Writing to sql server table " + sqlTableName)
    }

    if (pushToSQL && sqlTableName != null) {
      log.info("Started Writing to sql server table " + sqlTableName)
      PushToSQL.DataPullFromDF(finalHistoricalDF,sqlTableName + "Historical","O",spark)
      log.info("Finished Writing to sql server table " + sqlTableName)
    }

  }

  /**
    * Reads property value from commandline or config file based upon readPathFromConfig flag
    *
    * @param inputArgsMap
    *
    * @param argKey
    *
    * @return
    */
  def getPropertyValue(inputArgsMap: Map[String, String], argKey : String) : String = {

      inputArgsMap.get(argKey).get
  }

  def extractPreviousPath(inputArgsMap: Map[String, String], pathName: String): String = {

    // Get the slice date from the path
    // should be something like wasb://azops-dataingestioncontainer@azuredevopsdevrawstore1.blob.core.windows.net/DevTest/Table-Offers/Slice=20180402/Offers
    var mergedPath : String = null
    val path = inputArgsMap.get(pathName).get
    val slicePattern = inputArgsMap.get("slicePattern").get
    var mergedFilePath: String = null

    val slices = path.split("Slice=", 2)
    val basePath = slices(0)            // /should be wasb://azops-dataingestioncontainer@azuredevopsdevrawstore1.blob.core.windows.net/DevTest/Table-Offers
    val slice2 = slices(1)              // /should be 20180402/Offers/
    val slices2 = slice2.split("/", 2)     // try to get the date string
    val slice2ndPart = slices2(0)
    val remainingPath = if (slices2.size > 1 ) slices2(1) else ""

    // create a date from the slice date string
    val dateFormat = new SimpleDateFormat(slicePattern)
    val sliceDate = dateFormat.parse(slice2ndPart)


    // ignore the protocol like https, http, wasb or wasbs
    val basePathEndIndex = basePath.indexOf("/", 9)
    val finalBasePath = basePath.substring(0,basePathEndIndex)
    val subFoldersStr = basePath.substring(basePathEndIndex+1)


    val accountStr = inputArgsMap.get("account").get
    val containerStr = inputArgsMap.get("container").get

    val storageConnectionString: String =
      "DefaultEndpointsProtocol=https;" +
        "AccountName=" + accountStr + ";" +
        "AccountKey="


    val storageAccount: CloudStorageAccount = CloudStorageAccount.parse(storageConnectionString)
    val blobClient: CloudBlobClient = storageAccount.createCloudBlobClient()
    val container = blobClient.getContainerReference(containerStr)

    if (container.exists()) {
      val blobs = container.getDirectoryReference(subFoldersStr)
      val iter = blobs.listBlobs().iterator()

      var maxDate = null
      var maxSliceDate : Date  = null
      while( iter.hasNext) {
        val path = iter.next().getUri.toString
        val subFolderDateStr = path.split("Slice=")(1)
        var endIndex = if (subFolderDateStr.indexOf("/") == -1) subFolderDateStr.length else subFolderDateStr.indexOf("/")
        val folderName = subFolderDateStr.substring(0, endIndex)
        val mergedSliceDate  = dateFormat.parse(folderName)
        maxSliceDate = if ( mergedSliceDate.before(sliceDate)   // The slice date should be earlier than the current slice
          && (maxSliceDate == null || (!mergedSliceDate.equals(sliceDate) && maxSliceDate.before(mergedSliceDate)))) {
          // If the max slice date is null OR
          // Update the slice date i.e, the max slice date only if the merge Slice Date is after the max Slice Date
          mergedSliceDate
        } else {
          maxSliceDate
        }
      }
      if ( maxSliceDate != null)
        mergedPath = basePath + "Slice=" + dateFormat.format(maxSliceDate) + "/" + remainingPath
    }
    mergedPath
  }

  def printDF(df: DataFrame, dfName: String): Unit = {
    log.error("*** " + dfName + " count : " + df.count())
    df.show(df.count().toInt)
  }
  def hasColumn(df: DataFrame, path: String) = Try(df(path)).isSuccess
}
