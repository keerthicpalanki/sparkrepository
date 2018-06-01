package com.microsoft.jobs.usage

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SparkSession => _, _}
import java.sql.{CallableStatement, Connection, DriverManager, ResultSet}
import java.util
import java.util.{Calendar, Date, Properties, UUID}

import org.apache.log4j.Logger
import com.microsoft.azure.storage.CloudStorageAccount
import com.microsoft.azure.storage.StorageCredentials
import com.microsoft.azure.storage.blob._

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import java.io.File
import java.text.SimpleDateFormat

import com.microsoft.common.{DataFrameUtil, VNextException}
import com.microsoft.config.ConfigReader
import com.microsoft.framework.VNextUtil
import com.microsoft.jobs.sliceindex.SliceIndexUtilityHive
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

object CosmosMarketplaceUsageDaily {

  var spark: SparkSession = null
  var inputArgsMap: Map[String, String] = null
  val configReader = new ConfigReader("orderUsageDelta.conf")
  var jsonFiles: ListBuffer[String] = null

  val log = Logger.getLogger(getClass().getName())

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      log.error("Usage : Application requires minimum 1 argument")
      System.exit(-1)
    }
    val warehouseLocation = new File("spark-warehouse").getAbsolutePath

    val spark = SparkSession.builder
      .appName("inProgress")

      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    import spark.sql

    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    spark.conf.set("hive.mapred.supports.subdirectories", "true")
    spark.conf.set("mapred.input.dir.recursive", "true")
//      spark.conf.set("hive.metastore.warehouse.dir", params.hiveHost + "user/hive/warehouse")


    inputArgsMap = VNextUtil.getInputParametersAsKeyValue(args)
    val readPathFromConfig = VNextUtil.isReadPathsFromConfig(inputArgsMap)

    val Schema = StructType(Seq(
      StructField("RecordsCount",StringType,false),       StructField("SubscriptionGuid", StringType, false),
      StructField("resourceId", StringType, false),        StructField("sourceRegion", StringType, false),
      StructField("usageDate", StringType, false),        StructField("resourceUri", StringType, false),
      StructField("tags", StringType, false),      StructField("PAVersion", StringType, false),
      StructField("totalUnits", StringType, false),      StructField("ts", StringType, false),
      StructField("resourceSubType", StringType, false),      StructField("resourceType", StringType, false),
      StructField("orderNumber", StringType, false),      StructField("partNumber", StringType, false),
      StructField("properties", StringType, false),      StructField("additionalInfo", StringType, false),
      StructField("infoFields", StringType, false),      StructField("project", StringType, false),
      StructField("ServiceInfo2", StringType, false),      StructField("ServiceInfo1", StringType, false)
    ))

    var DataLineageID = UUID.randomUUID().toString

    if (! readPathFromConfig) {
      if (inputArgsMap.get("DataLineageID") != null && inputArgsMap.get("DataLineageID") != None){
        DataLineageID = inputArgsMap.get("DataLineageID").get
      }
    }
    val path = inputArgsMap.get("sourceUsageFact").get
    val daysToLoad = inputArgsMap.get("NoOfDaysToLoad").get
    val dtStr = path.takeRight(10)
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val sliceDate = dateFormat.parse(dtStr)
    val currDate = Calendar.getInstance
    val last30Date = Calendar.getInstance
    currDate.setTime(sliceDate)
    last30Date.setTime(sliceDate)
    last30Date.add(Calendar.DATE, -30)

    var finalDF: DataFrame = null


    for (i <- 1 to daysToLoad.toInt){

      val ans = dateFormat.format(currDate.getTime)

      val newPath = path.replace(path.takeRight(10), ans.toString)

      inputArgsMap += ("path"+i -> newPath)
      val latestFolder = getLatestFolder(inputArgsMap, "path"+i)

      if (latestFolder == "") {
        throw new VNextException(s"""The folder does not exist for the path """+ newPath)
      }

      val finalPath = newPath +"/" + "aggregatedts="+ latestFolder
      getAzureFiles(finalPath)

      val groupSize = inputArgsMap.get("groupSize").get.toInt
      val groupedJsonFiles = jsonFiles.grouped(groupSize)

      for ( jsonFileList <- groupedJsonFiles) {

        val dfJsonInput = spark.read.schema(Schema).json(jsonFileList: _*)


        if (finalDF == null)
          finalDF = dfJsonInput
        else
          finalDF = DataFrameUtil.union(finalDF, dfJsonInput)

      }
      currDate.add(Calendar.DATE, -1)

    }
    println("Total Usage Count is ************** "+finalDF.count())

    //sql("CREATE TABLE IF NOT EXISTS Cosmos_Marketplace_UsageDaily(RecordsCount string,SubscriptionGuid string, resourceId string, sourceRegion string, resourceUri string,tags string, PAVersion string, totalUnits string, ts string, resourceSubType string,resourceType string,orderNumber  string,partNumber string,properties string,additionalInfo string,infoFields string, project string,ServiceInfo2 string,ServiceInfo1 string) PARTITIONED BY (usageDate string, IsMissing boolean) STORED AS PARQUET")

    //finalDF.withColumn("IsMissing", lit(0)).write.mode(SaveMode.Overwrite).saveAsTable("Cosmos_Marketplace_UsageDaily")
   // finalDF.withColumn("IsMissing", lit(0)).write.mode(SaveMode.Overwrite).partitionBy("usageDate","IsMissing").insertInto("Cosmos_Marketplace_UsageDaily")
    finalDF.withColumn("DataLineageID", lit(DataLineageID)).withColumn("IsMissing", lit(0)).write.mode(SaveMode.Overwrite).partitionBy("usageDate","IsMissing").saveAsTable("Cosmos_Marketplace_UsageDaily")

    val queryStr = "INSERT INTO TABLE Cosmos_Marketplace_UsageDaily PARTITION (usageDate ,IsMissing) SELECT RecordsCount,reportingId,resourceId,sourceRegion,resourceUri,tags,PAVersion,totalUnits,aggregatedts,resourceSubType,resourceType,orderNumber,partNumber,properties,additionalInfo,infoFields, project,ServiceInfo2,ServiceInfo1, '" + DataLineageID + "' AS DataLineageID, usageDate, 1 AS IsMissing FROM Cosmos_Marketplace_MissingUsage WHERE usageDate BETWEEN '" + dateFormat.format(last30Date.getTime) + "' AND '" + dtStr + "'"

    sql("ADD JAR wasb://jsonserde@azuredevopsdevrawstore1.blob.core.windows.net/json/json-serde-1.3.9.jar")
    sql(queryStr)

    //Logging sliceindex for usagedaily and missingusage.
    val sliceDateFormat = new SimpleDateFormat("yyyyMMdd")
    SliceIndexUtilityHive.sliceIndex(sliceDateFormat.format(sliceDate.getTime), DataLineageID, spark, "select count(*) as RecordCount from Cosmos_Marketplace_UsageDaily", "Usage")
    SliceIndexUtilityHive.sliceIndex(sliceDateFormat.format(sliceDate.getTime), DataLineageID, spark, s"select count(*) as RecordCount from Cosmos_Marketplace_MissingUsage WHERE usageDate BETWEEN '${dateFormat.format(last30Date.getTime)}' AND '$dtStr'", "MissingUsage")
  }
  def getAzureFiles(finalPath: String) : ListBuffer[String] ={

    val basePathEndIndex = finalPath.indexOf("/", 9)

    val subFoldersStr = finalPath.substring(basePathEndIndex+1)

    val inputFolder = subFoldersStr
    val containerName = inputArgsMap.get("container").get
    val account = inputArgsMap.get("account").get
    val connStr = inputArgsMap.get("DEFAULT_Storage").get

    val storageConnectionString : String = connStr


    val storageAccount :CloudStorageAccount = CloudStorageAccount.parse(storageConnectionString)
    val blobClient : CloudBlobClient = storageAccount.createCloudBlobClient()
    val container = blobClient.getContainerReference(containerName)

    if ( container.exists()) {
      val blobs = container.getDirectoryReference(subFoldersStr)

      val iter = blobs.listBlobs().iterator()
      val prefix = "wasb://" + container.getName + "@" + account + ".blob.core.windows.net/" + inputFolder
      jsonFiles = new ListBuffer[String]
      log.info("List of Files : -")
      while (iter.hasNext) {
        val uri = iter.next().getUri().toString
        val index = uri.lastIndexOf('/')
        val substr = uri.substring(index)
        jsonFiles.append(prefix + substr)
      }
      jsonFiles.foreach(println)
    }
    jsonFiles
  }

  def getLatestFolder(inputArgsMap: Map[String, String], pathName: String): String = {

    val accountStr = inputArgsMap.get("account").get
    val containerStr = inputArgsMap.get("container").get
    val path = inputArgsMap.get(pathName).get
    val connStr = inputArgsMap.get("DEFAULT_Storage").get

    val storageConnectionString: String = connStr

    val storageAccount: CloudStorageAccount = CloudStorageAccount.parse(storageConnectionString)
    val blobClient: CloudBlobClient = storageAccount.createCloudBlobClient()
    val container = blobClient.getContainerReference(containerStr)
    var folderName = ""

    val basePathEndIndex = path.indexOf("/", 9)

    val subFoldersStr = path.substring(basePathEndIndex+1)

    var currFolderName = ""
    if (container.exists()) {
      val blobs = container.getDirectoryReference(subFoldersStr)

      val iter = blobs.listBlobs().iterator()

      while (iter.hasNext) {

        val path = iter.next().getUri.toString

        val subFolderDateStr = path.split("aggregatedts=")(1)
        var endIndex = if (subFolderDateStr.indexOf("/") == -1) subFolderDateStr.length else subFolderDateStr.indexOf("/")
        folderName = subFolderDateStr.substring(0, endIndex)
        if (folderName > currFolderName)
        {
          currFolderName = folderName
        }

      }

    }
    currFolderName
  }
}