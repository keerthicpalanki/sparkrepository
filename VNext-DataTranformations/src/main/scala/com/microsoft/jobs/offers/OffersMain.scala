package com.microsoft.jobs.offers

import java.util.Properties

import com.microsoft.config.ConfigReader
import com.microsoft.framework.VNextUtil
import org.apache.spark.sql.functions.{lit, _}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}

import scala.collection.mutable.{ListBuffer, WrappedArray}

object OffersMain {


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



  def main(args: Array[String]): Unit = {

    if (args.length < 1) {
      print("Usage : Application requires minimum 1 argument")
      System.exit(-1)
    }

    val startTime = System.nanoTime
    setup()

    try {

      inputArgsMap = VNextUtil.getInputParametersAsKeyValue(args)
      inputArgsMap.foreach(x => println(x._1 + " " + x._2))

      val inputPath = inputArgsMap.get("input").get

      print("input path is " + inputPath)

      val readPathFromConfig = VNextUtil.isReadPathsFromConfig(inputArgsMap)

      val dfJsonInput = spark.read.json(inputPath)

      val dfOffers = dfJsonInput.select(explode(dfJsonInput("offers")))

      getOffers(dfOffers, readPathFromConfig)

    }
    catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }
    val elapsed = (System.nanoTime - startTime) / 1e9d
    println("Done. Elapsed Time : " + elapsed + " seconds")

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

    presentColumns.foreach ( x => selectColumns.append(dfOffers.col(x)))

    val df = dfOffers.select(selectColumns:_*)
      .withColumn("IsPreview", lit(0).cast(IntegerType))

    var dfWithMissingCol = dfOffers
    missingColums.foreach( x => dfWithMissingCol = dfWithMissingCol.withColumn(x, lit(null).as("String")))

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


  def printDF(df: DataFrame, dfName: String): Unit = {
    println("*** " + dfName + " count : " + df.count())
    df.show(df.count().toInt)
  }

  def writeTableData(tableName: String, df: DataFrame, inputArgsMap: Map[String, String], readPathFromConfig: Boolean): Unit = {
    var path = if (readPathFromConfig == false)
      inputArgsMap.get(tableName).get
    else
      configReader.getValueForKey("offersDim.ingest.tables." + tableName + ".path").toString
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