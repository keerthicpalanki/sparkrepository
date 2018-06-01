package com.microsoft.jobs.offers

import java.net.URI
import java.util.Properties

import com.microsoft.azure.storage.CloudStorageAccount
import com.microsoft.azure.storage.blob.CloudBlobClient
import com.microsoft.common.DataFrameUtil
import com.microsoft.config.ConfigReader
import com.microsoft.framework.{OffersSchema, VNextUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.azure.AzureNativeFileSystemStore
import org.apache.hadoop.fs.{FileSystem, LocatedFileStatus, Path}
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{lit, when, _}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}

import scala.collection.mutable.{ListBuffer, WrappedArray}

object OffersTable {

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

        getOffers(dfOffers, readPathFromConfig)
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

  def getInputFileList(spark: SparkSession) = {


    var fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    try {
      var files = fs.listFiles(new Path("wasb:///InputFolder/"), false)
      while (files.hasNext) {
        val file = files.next()
        log.info(" ************* Default HDFS " + file.getPath)
      }
    }
    catch {
      case e: Exception => e.printStackTrace()
    }

    var fs1 = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    try {
      var files = fs1.listFiles(new Path("wasb://InputFolder/"), false)
      while (files.hasNext) {
        val file = files.next()
        log.info(" ************* <> " + file.getPath + "*************<>")
      }
    }
    catch {
      case e: Exception => e.printStackTrace()
    }

  }

  /**
    * Extracts OffersLanguage from JSON data file.
    */
  def getOffersLanguage(dfOffers: DataFrame, readFromConfig: Boolean) = {

    val dfLangData = dfOffers.select(
      when(col("Languages").isNull, lit(null)).otherwise(col("Languages")).as("Languages"),
      when(col("Version").isNull, lit(null).as("String")).otherwise(col("Version")).as("Version"),
      when(col("OfferId").isNull, lit(null).as("String")).otherwise(col("OfferId")).as("OfferId"),
      when(col("PublisherId").isNull, lit(null).as("String")).otherwise(col("PublisherId")).as("PublisherId"),
      when(col("ServiceNaturalIdentifier").isNull, lit(null).as("String")).otherwise(col("ServiceNaturalIdentifier")).as("ServiceNaturalIdentifier"),
      when(col("PublisherNaturalIdentifier").isNull, lit(null).as("String")).otherwise(col("PublisherNaturalIdentifier").as("PublisherNaturalIdentifier")))

    val dfLangDetailData = dfLangData.select(
      when(col("Languages").isNull, lit(null)).otherwise(col("Languages.en-US")).as("en-US"),
      col("Version"),
      col("OfferId"),
      col("PublisherId"),
      col("ServiceNaturalIdentifier"),
      col("PublisherNaturalIdentifier"))

    val dfLang = dfLangDetailData.select(
      col("Version"), col("OfferId"), col("PublisherId"), col("PublisherNaturalIdentifier"), col("ServiceNaturalIdentifier"),
      when(col("en-US").isNull, lit(null).as("String")).otherwise(col("en-US.Title")).as("Title"),
      when(col("en-US"), lit(null).as("String")).otherwise(col("en-US.Summary")).as("Description"),
      when(col("en-US"), lit(null).as("String")).otherwise(explode(col("en-US.ServicePlanDescriptions"))).as("SP"))

    val dfLang1 = dfLang.select(
      col("Version"), col("OfferId"), col("PublisherId"), col("PublisherNaturalIdentifier").as("PublisherNID"), col("Description"),
      col("ServiceNaturalIdentifier").as("ServiceNID"),
      col("Title").as("Title"),
      when(col("SP").isNull, lit(null).as("String")).otherwise(col("SP.Title")).as("SP_Title"),
      when(col("SP").isNull, lit(null).as("String")).otherwise(col("SP.Description")).as("SP_Description"),
      when(col("SP").isNull, lit(null).as("String")).otherwise(col("SP.Summary")).as("SP_Summary"),
      when(col("SP").isNull, lit(null).as("String")).otherwise(col("SP.ServicePlanNaturalIdentifier")).as("SP_ServicePlanNID"),
      when(col("SP").isNull, lit(null).as("String")).otherwise(col("SP.Features")).as("SP_Features"),
      when(col("SP").isNull, lit(null).as("String")).otherwise(col("SP.RequiresExternalLicense")).as("SP_RequiresExtLicense"),
      when(col("SP").isNull, lit(null).as("String")).otherwise(col("SP.RequiresExternalLicense")).as("SP_RequiresExtLicense"))
      .withColumn("Language", lit("en-US")).withColumn("IsExternallyBilled", when(col("SP_RequiresExtLicense") === true, 1).otherwise(0))

    val convertConditionUDF = udf(checkSpaceAndConcat)

    //val dfLang3 = dfLang1.withColumn("SP_Features", convertConditionUDF(col("SP_Features"))) Please test. Giving an error

    writeTableData("OfferDetails", dfLang1, inputArgsMap, readFromConfig)
  }

  /**
    * Extracts ServicePlansByMarket from JSON data file.
    */
  def getServicePlansByMarket(dfOffers: DataFrame, readFromConfig: Boolean) = {

    val dfSpmData = dfOffers.select("col.ServicePlansByMarket", "col.Version", "col.OfferId", "col.PublisherId", "col.ServiceNaturalIdentifier",
      "col.PublisherNaturalIdentifier")

    val dfSpmDetailedData = dfSpmData.select("ServicePlansByMarket.*", "Version", "OfferId", "PublisherId", "ServiceNaturalIdentifier", "PublisherNaturalIdentifier")

    val dfSpmColumns = dfSpmData.select("ServicePlansByMarket.*")

    var finalDF: DataFrame = null

    val convertConditionUDF = udf(convertConditionsToStr)

    dfSpmColumns.columns.foreach(col => {

      val dfSpm = dfSpmDetailedData.select(explode(dfSpmDetailedData(col)).as("SPM_INFO"),
        dfSpmDetailedData.col("Version"), dfSpmDetailedData.col("OfferId"),
        dfSpmDetailedData.col("PublisherId"), dfSpmDetailedData.col("ServiceNaturalIdentifier"),
        dfSpmDetailedData.col("PublisherNaturalIdentifier"))

      val dfNew = dfSpm.select(
        dfSpm.col("Version"), dfSpm.col("OfferId"),
        dfSpm.col("PublisherId"), dfSpm.col("PublisherNaturalIdentifier"), dfSpm.col("ServiceNaturalIdentifier"),
        dfSpm.col("SPM_INFO.CurrencyCode"), dfSpm.col("SPM_INFO.NaturalIdentifier"), explode(dfSpm("SPM_INFO.PricePlan")).as("PP"))

      val dfNew2 = dfNew.select(dfNew.col("Version"),
        dfNew.col("OfferId"),
        dfNew.col("PublisherId"),
        dfNew.col("PublisherNaturalIdentifier").as("PublisherNID"),
        dfNew.col("ServiceNaturalIdentifier").as("ServiceNID"),
        dfNew.col("NaturalIdentifier").as("ServicePlanNID"), // ServicePlanNaturalIdentifier -- to be done by Ajmal
        dfNew.col("CurrencyCode"),
        dfNew.col("PP.Amount").as("PP_Amount"),
        dfNew.col("PP.AppliesTo").as("PP_AppliesTo"),
        dfNew.col("PP.Effect").as("PP_Effect")) //, explode(dfNew.col("PP.Conditions")).as("Conditions"))
        .withColumn("Country", lit(col))

      //val dfNew3 = dfNew2.withColumn("ConditionsNew", convertConditionUDF(dfNew2.col("Conditions"))).drop("Conditions").withColumnRenamed("ConditionsNew", "Conditions")

      if (finalDF == null)
        finalDF = dfNew2
      else
        finalDF = DataFrameUtil.union(finalDF, dfNew2)

    })

    writeTableData("latestServicePlans", finalDF, inputArgsMap, readFromConfig)

  }

  def getOffers(dfOffers1: DataFrame, readFromConfig: Boolean) = {

    val dfOffers = dfOffers1.select(col("col.*"))

    val columns = Seq(
      "VirtualMachineImagesByServicePlan",
      "Version",
      "OfferId",
      "PublisherId",
      "PublisherName",
      "PublisherLegalName",
      "PublisherWebsite",
      "Store",
      "Timestamp",
      "PublisherNaturalIdentifier",
      "ServiceNaturalIdentifier",
      "SupportUri",
      "IntegrationContactName",
      "IntegrationContactEmail",
      "IntegrationContactPhoneNumber",
      "Categories",
      "DataServiceId",
      "DataServiceVersion",
      "DataServiceListingUrl",
      "ResourceType",
      "ResourceProviderIdentifier",
      "ResourceManifest",
      "FulfillmentServiceId",
      "AvailableDataCenters"
    )

    val missingColums = columns.toList.diff(dfOffers.columns)

    val presentColumns = columns.toList.diff(missingColums)

    val selectColumns = new ListBuffer[Column]

    presentColumns.foreach(x => selectColumns.append(dfOffers.col(x)))

    val df = dfOffers.select(selectColumns: _*)
      .withColumn("IsPreview", lit(0).cast(IntegerType))

    var dfWithMissingCol = df
    missingColums.foreach(x => dfWithMissingCol = dfWithMissingCol.withColumn(x, lit(null).as("String")))

    val offerTypeUDF = udf(transformOfferType)

    val df2 = dfWithMissingCol.withColumn("OfferType", offerTypeUDF(col("VirtualMachineImagesByServicePlan"), col("DataServiceId"), col("FulfillmentServiceId")))
      .drop("VirtualMachineImagesByServicePlan").drop("DataServiceId").drop("FulfillmentServiceId")
      .withColumn("OfferName", col("ServiceNaturalIdentifier"))
      .withColumn("PublisherNID", col("PublisherNaturalIdentifier"))

    writeTableData("latestOffers", df2, inputArgsMap, readFromConfig)
  }

  def transformOfferType = (virtualMachineImagesByServicePlan: Row, dataServiceId: String, fulfillmentServiceId: String) => {
    /* CASE WHEN impOffer.VirtualMachineImagesByServicePlan != '' THEN 'VirtualMachine'
     WHEN impOffer.DataServiceId != '' THEN 'DataService'
     WHEN impOffer.FulfillmentServiceId != '' THEN 'AppService' END AS OfferType*/

    var offerType: String = null
    if (virtualMachineImagesByServicePlan != null) {
      offerType = "VirtualMachine"
    } else if (dataServiceId != null && !dataServiceId.isEmpty) {
      offerType = "DataService"
    } else if (fulfillmentServiceId != null && !fulfillmentServiceId.isEmpty) {
      offerType = "AppService"
    }

    offerType
  }

  /**
    * Extracts VirtualMachineImagesByServicePlan from JSON data file.
    */
  def getVMImages_old(dfOffers: DataFrame, readPathFromConfig: Boolean) = {
    val dfVmData = dfOffers.select("col.VirtualMachineImagesByServicePlan", "col.Version", "col.OfferId", "col.PublisherId", "col.PublisherNaturalIdentifier", "col.ServiceNaturalIdentifier")

    val dfVmDetailData = dfVmData.select("VirtualMachineImagesByServicePlan.*", "Version", "OfferId", "PublisherId", "PublisherNaturalIdentifier", "ServiceNaturalIdentifier")

    val dfVmColumns = dfVmData.select("VirtualMachineImagesByServicePlan.*")

    var finalDF: DataFrame = null

    dfVmColumns.columns.foreach(col => {
      val dfVm = dfVmDetailData.select(explode(dfVmDetailData(col)).as("VM_INFO"), dfVmDetailData.col("Version"),
        dfVmDetailData.col("OfferId"), dfVmDetailData.col("PublisherId"), dfVmDetailData.col("PublisherNaturalIdentifier"), dfVmDetailData.col("ServiceNaturalIdentifier"))

      val dfNew = dfVm.select(dfVm.col("Version"), dfVm.col("OfferId"),
        dfVm.col("PublisherId"), dfVm.col("PublisherNaturalIdentifier"), dfVm.col("ServiceNaturalIdentifier"), dfVm.col("VM_INFO.OperatingSystem"),
        dfVm.col("VM_INFO.OperatingSystemFamily"), dfVm.col("VM_INFO.Version").as("VM-Version")).withColumn("VMName", lit(col))

      // Get the partial data related to this vm
      val dfCurrentVmData = dfNew.filter(dfNew.col("VMName").isNotNull).select("PublisherNaturalIdentifier", "ServiceNaturalIdentifier", "Version", "OfferId", "PublisherId",
        "VMName", "VM-Version", "OperatingSystemFamily", "OperatingSystem")

      // Append to the final DF.
      if (finalDF == null)
        finalDF = dfCurrentVmData
      else
        finalDF = DataFrameUtil.union(finalDF, dfCurrentVmData)
    })

    //    finalDF.printSchema()
    //    finalDF.coalesce(4).write.mode("append").json("Table-VM")
    writeTableData("VirtualMachines", finalDF, inputArgsMap, readPathFromConfig)
  }

  /**
    * Extracts VirtualMachineImagesByServicePlan from JSON data file.
    */
  def getVMImages(dfOffers: DataFrame, readPathFromConfig: Boolean) = {
    val dfVmData = dfOffers.select("col.VirtualMachineImagesByServicePlan", "col.Version", "col.OfferId", "col.PublisherId", "col.PublisherNaturalIdentifier", "col.ServiceNaturalIdentifier")
    //    printDF(dfVmData, "dfVmData")


    val dfVmDetailData = dfVmData.select("VirtualMachineImagesByServicePlan.*", "Version", "OfferId", "PublisherId", "PublisherNaturalIdentifier", "ServiceNaturalIdentifier")
    //    printDF(dfVmDetailData, "dfVmDetailData")

    val dfVmColumns = dfVmData.select("VirtualMachineImagesByServicePlan.*")
    printDF(dfVmColumns, "dfVmColumns")

    var finalDF: DataFrame = null

    dfVmColumns.columns.foreach(col => {
      val dfVm = dfVmDetailData.select(explode(dfVmDetailData(col)).as("VM_INFO"), dfVmDetailData.col("Version"),
        dfVmDetailData.col("OfferId"), dfVmDetailData.col("PublisherId"), dfVmDetailData.col("PublisherNaturalIdentifier"), dfVmDetailData.col("ServiceNaturalIdentifier"))

      val dfNew = dfVm.select(dfVm.col("Version"), dfVm.col("OfferId").as("OfferID"),
        dfVm.col("PublisherId").as("PublisherID"), dfVm.col("PublisherNaturalIdentifier").as("PublisherNID"),
        dfVm.col("ServiceNaturalIdentifier").as("ServiceNID"), dfVm.col("VM_INFO.OperatingSystem").as("OS"),
        dfVm.col("VM_INFO.OperatingSystemFamily").as("OSFamily"), dfVm.col("VM_INFO.Version").as("VM_Version")).withColumn("VM_Name", lit(col))
      //      printDF(dfNew, "dfNew")

      // Get the partial data related to this vm
      val dfCurrentVmData = dfNew.filter(dfNew.col("VM_Name").isNotNull).select("PublisherNID", "ServiceNID", "Version", "OfferID", "PublisherID",
        "VM_Name", "VM_Version", "OSFamily", "OS")
      //      printDF(dfCurrentVmData, "dfCurrentVmData")

      // Append to the final DF.
      if (dfCurrentVmData.count() > 0) {
        if (finalDF == null)
          finalDF = dfCurrentVmData
        else
          finalDF = DataFrameUtil.union(finalDF, dfCurrentVmData)
      }
    })

    //    finalDF.printSchema()
    //    printDF(finalDF, "finalDF")
    //    finalDF.coalesce(4).write.mode("append").json("Table-VM")
    writeTableData("VirtualMachines", finalDF, inputArgsMap, readPathFromConfig)

  }


  def printDF(df: DataFrame, dfName: String): Unit = {
    log.info("*** " + dfName + " count : " + df.count())
    df.show(df.count().toInt)
  }

  def writeTableData(tableName: String, df: DataFrame, inputArgsMap: Map[String, String], readPathFromConfig: Boolean): Unit = {
    var path = VNextUtil.getPropertyValue(inputArgsMap, tableName)
    df.write.mode("append").json(path)
  }

  def convertConditionsToStr = (row: Row) => {
    row match {
      case null => null
      case _ => row.getString(0) + "|" + row.getBoolean(1) + "|" + row.getString(2) + "|" + row.getString(3) + "|" + row.getString(4)
    }
  }

  def checkSpaceAndConcat = (arr: WrappedArray[String]) => {

    if (arr != null) {
      arr.length match {
        case 0 => null
        case _ => {
          val arr2 = arr.map(x => x.trim).filter(_.length > 0)
          arr2.mkString("|")
        }
      }
    } else {
      null
    }
  }
}
