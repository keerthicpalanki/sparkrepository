package com.microsoft.jobs.offers

import java.util.Properties

import com.microsoft.azure.storage.CloudStorageAccount
import com.microsoft.azure.storage.blob.CloudBlobClient
import com.microsoft.config.ConfigReader
import com.microsoft.framework.{OffersSchema, VNextUtil}
import org.apache.hadoop.fs.{FileSystem, LocatedFileStatus, Path}
import org.apache.spark.sql.functions.{lit, when, _}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}

import scala.collection.mutable.{ListBuffer, WrappedArray}

object OffersLangTable {

  var spark: SparkSession = null

  var jsonFiles: ListBuffer[String] = null

  val configReader = new ConfigReader("processOffers.conf")

  var inputArgsMap: Map[String, String] = null

  def setup() = {

    val configProperties = new Properties
    val inputStream = this.getClass.getClassLoader.getResourceAsStream("global.properties")
    configProperties.load(inputStream)

    spark = SparkSession.builder
      .getOrCreate()
  }

  def usage() = {
    println("Following arguments are mandatory input=<Input Folder> container=<container name> account=<account name> groupSize=<batch size of files to read>")
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
      assert(inputArgsMap.get("account") != null, usage)
      assert(inputArgsMap.get("container") != null, usage)

      val readPathFromConfig = VNextUtil.isReadPathsFromConfig(inputArgsMap)

      getAzureFiles()

      val groupSize = inputArgsMap.get("groupSize").get.toInt
      val groupedJsonFiles = jsonFiles.grouped(groupSize)

      for (jsonFileList <- groupedJsonFiles) {

        val dfJsonInput = spark.read.json(jsonFileList: _*)

        val dfOffers = dfJsonInput.select(explode(dfJsonInput("offers")))

        getOffersLanguage(dfOffers, readPathFromConfig)
      }

    }
    catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }
    val elapsed = (System.nanoTime - startTime) / 1e9d
    println("Done. Elapsed Time : " + elapsed + " seconds")

  }

  def getAzureFiles(): ListBuffer[String] = {

    val inputFolder = inputArgsMap.get("input").get
    val containerName = inputArgsMap.get("container").get
    val account = inputArgsMap.get("account").get

    val storageConnectionString : String =
      "DefaultEndpointsProtocol=https;" +
        "AccountName="+ account + ";" +
        "AccountKey="


    val storageAccount: CloudStorageAccount = CloudStorageAccount.parse(storageConnectionString)
    val blobClient: CloudBlobClient = storageAccount.createCloudBlobClient()
    val container = blobClient.getContainerReference(containerName)

    if (container.exists()) {
      val blobs = container.getDirectoryReference(inputFolder)

      val iter = blobs.listBlobs().iterator()
      val prefix = "wasb://" + container.getName + "@" + account + ".blob.core.windows.net/" + inputFolder
      jsonFiles = new ListBuffer[String]
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
    * Extracts OffersLanguage from JSON data file.
    */
  def getOffersLanguage(dfOffers1: DataFrame, readFromConfig: Boolean) = {

    val dfOffers = dfOffers1.select("col.*")
    val firstLevelColumns = Seq("Languages", "Version", "OfferId", "PublisherId", "ServiceNaturalIdentifier", "PublisherNaturalIdentifier")
    val missingColums = firstLevelColumns.toList.diff(dfOffers.columns)
    val presentColumns = firstLevelColumns.toList.diff(missingColums)
    val filteredDF = generateRequiredDF(presentColumns, missingColums, dfOffers)

    var enUSPresent = true
    var dfWithEngLang = if (presentColumns.contains("Languages")) {
      var languageCols = filteredDF.select("Languages.*").columns
      var dfWithENUS = if (languageCols.contains("en-US")) {
        filteredDF.select(
          col("Languages.en-US").as("en-US"),
          col("Version"),
          col("OfferId"),
          col("PublisherId"),
          col("ServiceNaturalIdentifier"),
          col("PublisherNaturalIdentifier"))
      }
      else {
        enUSPresent = false
        filteredDF.withColumn("en-US", lit("").cast("String"))
      }
      dfWithENUS
    } else {
      enUSPresent = false
      filteredDF.withColumn("en-US", lit("").cast("String"))
    }

    val finalDF = if (enUSPresent == true) {
      val columns = dfWithEngLang.select("en-US.*").columns

      var dfWithExplodedEnUS = dfWithEngLang
      if (columns.contains("Title"))
        dfWithExplodedEnUS = dfWithExplodedEnUS.withColumn("Title", col("en-US.Title"))
      else
        dfWithExplodedEnUS = dfWithExplodedEnUS.withColumn("Title", lit(""))

      if (columns.contains("Summary"))
        dfWithExplodedEnUS = dfWithExplodedEnUS.withColumn("Description", col("en-US.Summary"))
      else
        dfWithExplodedEnUS = dfWithExplodedEnUS.withColumn("Description", lit(""))

      var dfLang = dfWithExplodedEnUS.select(dfWithExplodedEnUS.col("Version"), dfWithExplodedEnUS.col("OfferId"),
        dfWithExplodedEnUS.col("PublisherId"), dfWithExplodedEnUS.col("PublisherNaturalIdentifier"), dfWithExplodedEnUS.col("ServiceNaturalIdentifier"),
        dfWithExplodedEnUS.col("Title"), dfWithExplodedEnUS.col("Description"),
        explode(col("en-US.ServicePlanDescriptions")).as("SP"))


      var mainCols = new ListBuffer[Column]
      val mainColumnStr = Seq("Version", "OfferId", "PublisherId", "PublisherNaturalIdentifier", "ServiceNaturalIdentifier", "Title", "Description")
      val mainColList = mainColumnStr.map(x => dfLang.col(x))

      val spColumns = dfLang.select("SP.*").columns
      val spColumnTypeList :Array[Column] = spColumns.map(x => dfLang.col("SP." +x ).as("SP_" + x))

      var dfLang1 = dfLang.select((mainColList ++ spColumnTypeList):_*)

      if (spColumns.contains("Title"))
        dfLang1 = dfLang1.withColumn("SP_Title", col("SP_Title"))
      else
        dfLang1 = dfLang1.withColumn("SP_Title", lit(""))

      if (spColumns.contains("Description"))
        dfLang1 = dfLang1.withColumn("SP_Description", col("SP_Description"))
      else
        dfLang1 = dfLang1.withColumn("SP_Description", lit(""))

      if (spColumns.contains("Summary"))
        dfLang1 = dfLang1.withColumn("SP_Summary", col("SP_Summary"))
      else
        dfLang1 = dfLang1.withColumn("SP_Summary", lit(""))

      if (spColumns.contains("ServicePlanNaturalIdentifier"))
        dfLang1 = dfLang1.withColumn("SP_ServicePlanNID", col("SP_ServicePlanNaturalIdentifier"))
      else
        dfLang1 = dfLang1.withColumn("SP_ServicePlanNID", lit(""))

      if (spColumns.contains("Features"))
        dfLang1 = dfLang1.withColumn("SP_Features", col("SP_Features"))
      else
        dfLang1 = dfLang1.withColumn("SP_Features", lit(""))

      if (spColumns.contains("RequiresExternalLicense"))
        dfLang1 = dfLang1.withColumn("SP_RequiresExtLicense", col("SP_RequiresExternalLicense"))
      else
        dfLang1 = dfLang1.withColumn("SP_RequiresExtLicense", lit(""))

      dfLang1 = dfLang1.withColumnRenamed("PublisherNaturalIdentifier", "PublisherNID")
        .withColumnRenamed("ServiceNaturalIdentifier", "ServiceNID")
        .withColumn("Language", lit("en-US"))
        .withColumn("IsExternallyBilled", when(col("SP_RequiresExtLicense") === true, 1).otherwise(lit(0)))

      dfLang1

    } else {
      dfWithEngLang
        .withColumnRenamed("PublisherNaturalIdentifier", "PublisherNID")
        .withColumnRenamed("ServiceNaturalIdentifier", "ServiceNID")
        .withColumn("Title", lit(""))
        .withColumn("Description", lit(""))
        .withColumn("SP_Title", lit(""))
        .withColumn("SP_Description", lit(""))
        .withColumn("SP_Summary", lit(""))
        .withColumn("SP_Features", lit(""))
        .withColumn("SP_RequiresExtLicense", lit(""))
        .withColumn("IsExternallyBilled", lit(0))
    }

    val convertConditionUDF = udf(checkSpaceAndConcat)

    //val dfLang3 = dfLang1.withColumn("SP_Features", convertConditionUDF(col("SP_Features"))) Please test. Giving an error

    writeTableData("OfferDetails", finalDF, inputArgsMap, readFromConfig)
  }

  def printDF(df: DataFrame, dfName: String): Unit = {
    println("*** " + dfName + " count : " + df.count())
    df.show(df.count().toInt)
  }

  def writeTableData(tableName: String, df: DataFrame, inputArgsMap: Map[String, String], readPathFromConfig: Boolean): Unit = {
    var path = VNextUtil.getPropertyValue( inputArgsMap, tableName)
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

  def generateRequiredDF(presentColumns: List[String], missingColums: List[String], df: DataFrame): DataFrame = {

    val selectColumns = new ListBuffer[Column]

    presentColumns.foreach(x => selectColumns.append(df.col(x)))

    val df2 = df.select(selectColumns: _*)

    var dfWithMissingCol = df

    missingColums.foreach(x => dfWithMissingCol = dfWithMissingCol.withColumn(x, lit(null).cast("String")))

    dfWithMissingCol

  }
}
