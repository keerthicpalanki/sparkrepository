package com.microsoft.jobs.offers

import java.net.URI
import java.util.Properties

import com.microsoft.azure.storage.CloudStorageAccount
import com.microsoft.azure.storage.blob.CloudBlobClient
import com.microsoft.common.DataFrameUtil
import com.microsoft.config.ConfigReader
import com.microsoft.framework.VNextUtil.getPropertyValue
import com.microsoft.framework.{OffersSchema, VNextUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.azure.AzureNativeFileSystemStore
import org.apache.hadoop.fs.{FileSystem, LocatedFileStatus, Path}
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{lit, when, _}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}

import scala.collection.mutable.{ListBuffer, WrappedArray}

object VMTable {

  var spark: SparkSession = null

  var jsonFiles: ListBuffer[String] = null

  val configReader = new ConfigReader("processOffers.conf")

  var inputArgsMap: Map[String, String] = null
  val log = Logger.getLogger(getClass().getName())

  def setup() = {

    log.info("***************** In the set up method ********************")
    val configProperties = new Properties
    val inputStream = this.getClass.getClassLoader.getResourceAsStream("global.properties")
    configProperties.load(inputStream)

    log.info("***************** In the set up method ******************** 1")

    val masterURL = configProperties.getProperty("spark.master.url")
    val appName = configReader.getValueForKey("offersDim.appName").toString

    spark = SparkSession.builder
      .getOrCreate()

  }

  def usage()={
    log.error("Following arguments are mandatory input=<Input Folder> container=<container name> account=<account name> groupSize=<batch size of files to read>")
  }

  def main(args: Array[String]): Unit = {

    if (args.length < 1) {
      usage
      System.exit(-1)
    }

    val startTime = System.nanoTime
    setup()

    try {

      inputArgsMap = VNextUtil.getInputParametersAsKeyValue(args)

      assert(inputArgsMap.get("input") != null, usage)
      assert(inputArgsMap.get("account") != null,usage)
      assert(inputArgsMap.get("container") != null, usage)

      val readPathFromConfig = VNextUtil.isReadPathsFromConfig(inputArgsMap)

      getAzureFiles()

      val groupSize = inputArgsMap.get("groupSize").get.toInt
      val groupedJsonFiles = jsonFiles.grouped(groupSize)

      for ( jsonFileList <- groupedJsonFiles) {

        val dfJsonInput = spark.read.json(jsonFileList:_*)

        val dfOffers = dfJsonInput.select(explode(dfJsonInput("offers")))

        //        getOffers(dfOffers, readPathFromConfig)
        getVMImages(dfOffers, readPathFromConfig)
      }

    }
    catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }
    val elapsed = (System.nanoTime - startTime) / 1e9d
    log.info("Done. Elapsed Time : " + elapsed + " seconds")

  }

  def getAzureFiles() : ListBuffer[String] ={

    val inputFolder = inputArgsMap.get("input").get
    val containerName = inputArgsMap.get("container").get
    val account = inputArgsMap.get("account").get

    val storageConnectionString : String =
      "DefaultEndpointsProtocol=https;" +
        "AccountName="+ account + ";" +
        "AccountKey="


    val storageAccount :CloudStorageAccount = CloudStorageAccount.parse(storageConnectionString)
    val blobClient : CloudBlobClient = storageAccount.createCloudBlobClient()
    val container = blobClient.getContainerReference(containerName)

    if ( container.exists()) {
      val blobs = container.getDirectoryReference(inputFolder)

      val iter = blobs.listBlobs().iterator()
      val prefix = "wasb://" + container.getName + "@" + account + ".blob.core.windows.net/" + inputFolder
      jsonFiles = new ListBuffer[String]
      log.warn("List of Files : -")
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

  /**
    * Extracts VirtualMachineImagesByServicePlan from JSON data file.
    */
  def getVMImages(dfOffers: DataFrame, readPathFromConfig: Boolean) = {
    val dfVmData = dfOffers.select("col.VirtualMachineImagesByServicePlan", "col.Version", "col.OfferId", "col.PublisherId", "col.PublisherNaturalIdentifier", "col.ServiceNaturalIdentifier")

    val dfVmDetailData = dfVmData.select("VirtualMachineImagesByServicePlan.*", "Version", "OfferId", "PublisherId", "PublisherNaturalIdentifier", "ServiceNaturalIdentifier")

    val dfVmColumns = dfVmData.select("VirtualMachineImagesByServicePlan.*")

    var finalDF: DataFrame = null

    dfVmColumns.columns.foreach(col => {
      val dfVm = dfVmDetailData.select(explode(dfVmDetailData(col)).as("VM_INFO"), dfVmDetailData.col("Version"),
        dfVmDetailData.col("OfferId"), dfVmDetailData.col("PublisherId"), dfVmDetailData.col("PublisherNaturalIdentifier"), dfVmDetailData.col("ServiceNaturalIdentifier"))

      val dfNew = dfVm.select(dfVm.col("Version"), dfVm.col("OfferId").as("OfferID"),
        dfVm.col("PublisherId").as("PublisherID"), dfVm.col("PublisherNaturalIdentifier").as("PublisherNID"),
        dfVm.col("ServiceNaturalIdentifier").as("ServiceNID"), dfVm.col("VM_INFO.OperatingSystem").as("OS"),
        dfVm.col("VM_INFO.OperatingSystemFamily").as("OSFamily"), dfVm.col("VM_INFO.Version").as("VM_Version")).withColumn("VM_Name", lit(col))

      // Get the partial data related to this vm
      val dfCurrentVmData = dfNew.filter(dfNew.col("VM_Name").isNotNull).select("PublisherNID", "ServiceNID", "Version", "OfferID", "PublisherID",
        "VM_Name", "VM_Version", "OSFamily", "OS")

        if (finalDF == null)
          finalDF = dfCurrentVmData
        else
          finalDF = DataFrameUtil.union(finalDF, dfCurrentVmData)
    })

    writeTableData("VirtualMachines", finalDF, inputArgsMap, readPathFromConfig)

  }


  def writeTableData(tableName: String, df: DataFrame, inputArgsMap: Map[String, String], readPathFromConfig: Boolean): Unit = {
    var path = getPropertyValue(inputArgsMap, tableName)
    df.write.mode("append").json(path)
  }
}
