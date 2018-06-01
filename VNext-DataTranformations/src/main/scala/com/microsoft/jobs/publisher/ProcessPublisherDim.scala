package com.microsoft.jobs.publisher

import java.util.Properties

import com.microsoft.common.DataFrameUtil
import com.microsoft.config.ConfigReader
import com.microsoft.framework.{MergeDF, VNextUtil}
import com.microsoft.jobs.Enrollment.{configReader, inputArgsMap}
import com.microsoft.jobs.offers.ProcessOffersDim.{configReader, inputArgsMap, setup}
import com.microsoft.jobs.order.ProcessOrders.configReader
import com.microsoft.jobs.publisher.ProcessPublisherDim.readODataOfferPublishers
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions.Like
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window

import scala.collection.mutable.ListBuffer

object ProcessPublisherDim {

  var spark: SparkSession = null
  val configReader = new ConfigReader("publisherDim.conf")
  var inputArgsMap : Map[String, String] = null

  def setup() = {

    val configProperties = new Properties
    val inputStream = this.getClass.getClassLoader.getResourceAsStream("global.properties")
    configProperties.load(inputStream)

    val masterURL = configProperties.getProperty("spark.master.url")
    val appName = configReader.getValueForKey("publisherDim.appName").toString

    spark = SparkSession.builder
      .getOrCreate()


  }


  def main(args: Array[String]): Unit = {
    inputArgsMap = VNextUtil.getInputParametersAsKeyValue(args)
    setup()
    startTransformations()
  }

  def startTransformations() = {

    //    val df = spark.read.json("wasb://azops-dataingestioncontainer@azuredevopsdevrawstore1.blob.core.windows.net/PublisherDimHistorical.json")
    // First transformation or dataset
    //df.show()

    val offersListedDF = readOffersListedTable("offersListed")
    val offersCurrentDF = readTables("offersListed")
    val pubPortalDF = readPubPortalTable("pubPortal")
    val manualInternalPublishersDF = readTables("manualInternalPublishers")
    val manualPublisherNameOverridesDF = readTables("manualPublisherNameOverrides")

    val odataPublishersDF = readODataOfferPublishers("odataOffersOnBoardingPublisher")
    val devcenterDevAccountsDF = DataFrameUtil.readDataFrameFromBlobAsTSV(configReader, "publisherDim.ingest.tables.devcenterDevAccounts", "devcenterDevAccounts", inputArgsMap, spark)
    val devcenterCommercialInfoDF = DataFrameUtil.readDataFrameFromBlobAsTSV(configReader, "publisherDim.ingest.tables.devcenterCommercialInfo","devcenterCommercialInfo", inputArgsMap, spark)
    val devcenterContactInfoDF = DataFrameUtil.readDataFrameFromBlobAsTSV(configReader, "publisherDim.ingest.tables.devcenterContactInfo","devcenterContactInfo", inputArgsMap, spark)


    val offersListedCurrentDF = offersCurrentDF.select(col("PublisherId"), col("PublisherLegalName"), col("PublisherName"), col("Timestamp"))
      .withColumn("PublisherRank", row_number().over(Window.partitionBy(col("PublisherId")).orderBy(col("Timestamp").desc))).distinct()

    val mainDF = offersListedDF
      .join(odataPublishersDF, offersListedDF("PublisherId") === odataPublishersDF("PublisherId"),"left_outer")
      .join(offersListedCurrentDF, offersListedDF("PublisherId") === offersListedCurrentDF("PublisherId") && offersListedCurrentDF("PublisherRank") === 1,"left_outer")
      .join(pubPortalDF,offersListedDF("PublisherId") === pubPortalDF("Id"), "left_outer")
      .join(devcenterDevAccountsDF, pubPortalDF("Puid") === devcenterDevAccountsDF("AccountPuid"), "left_outer")
      .join(devcenterCommercialInfoDF, devcenterDevAccountsDF("SellerId") === devcenterCommercialInfoDF("SellerId"), "left_outer")
      .join(devcenterContactInfoDF, devcenterDevAccountsDF("SellerId") === devcenterContactInfoDF("SellerId"), "left_outer")
      .join(manualInternalPublishersDF, offersListedDF("PublisherId") === manualInternalPublishersDF("PublisherID"), "left_outer")
      .join(manualPublisherNameOverridesDF, offersListedDF("PublisherNaturalIdentifier") === manualPublisherNameOverridesDF("PublisherOverrideValue"), "left_outer")
      .select(concat(offersListedDF("PublisherId"),lit(":"),offersListedDF("PublisherNaturalIdentifier")).as("PublisherKey"),
        offersListedDF("PublisherId"), offersListedDF("PublisherNaturalIdentifier").as("PublisherName"),
        coalesce(offersListedCurrentDF("PublisherLegalName"), odataPublishersDF("LegalName")).as("LegalName"),
        coalesce(offersListedCurrentDF("PublisherName"), odataPublishersDF("ShortName")).as("ShortName"),
        //odataPublishersDF("OwnerEmail"),
        coalesce(devcenterContactInfoDF("UserEmail"),odataPublishersDF("OwnerEmail")).as("OwnerEmail"),
        devcenterContactInfoDF("UserCountryCode").as("CountryCode"),
        odataPublishersDF("OwnerId"),
        odataPublishersDF("RegionCode"),
        odataPublishersDF("CreatedOnUtc").as("CreatedDate"),
        odataPublishersDF("ApprovedOnUtc").as("ApprovedDate"),
        pubPortalDF("PayoutAccountId"), coalesce(pubPortalDF("TaxProfileId"),odataPublishersDF("TaxProfileId")).as("TaxProfileId"),
        //      devcenterDevAccountsDF("businessemail").as("OwnerEmail"),
        coalesce(devcenterCommercialInfoDF("CTPAccountid"),odataPublishersDF("PayoutAccountId")).as("BDKID"),
        devcenterCommercialInfoDF("Universalaccountid").as("UniversalAccountId"),
        manualPublisherNameOverridesDF("PublisherNaturalIdentifier").as("PublisherNameOverride"),
        manualInternalPublishersDF("IsInternalPublisher").as("isIP"),
        manualInternalPublishersDF("PublisherID").as("ipPublisherID")
      )



    val finalDF1 = mainDF.select(col("PublisherKey"), col("PublisherId"), col("PublisherName"), col("LegalName"), col("PayoutAccountId")
      ,col("TaxProfileId")//col("OwnerEmail"),
      , col("OwnerEmail"),col("RegionCode"),col("CountryCode"),
      col("OwnerId"),col("CreatedDate"),
      col("ApprovedDate"),
      col("BDKID"), col("UniversalAccountId"), col("PublisherNameOverride"), col("isIP"), col("ipPublisherID"), col("ShortName"))
      .withColumn("RevenueSplitPercentage", lit(.80))
      .withColumn("DataSource", lit("PubPortal"))
      .withColumn("IsInternalPublisher",
        when(col("ipPublisherID").isNotNull, col("isIP"))
          .otherwise( when(col("PublisherName").like("%microsoft%") ||
            col("LegalName").like("%microsoft%")
                        || col("OwnerEmail").like("%microsoft.com%")
            , lit(1)).otherwise(lit(0))))
      .withColumn("IsSCDActive", lit(1))
      .withColumn("rowkey", col("PublisherName"))
    //.withColumn("OwnerEmail", lit("Change This"))


    val finalDF = finalDF1.select(col("BDKID"), col("DataSource"), col("IsInternalPublisher"), col("IsSCDActive")
      , col("LegalName"), col("OwnerEmail"), col("RegionCode"), col("OwnerId"), col("CreatedDate"),col("ApprovedDate")//, col("OwnerEmail")
      ,col("PayoutAccountId"), col("PublisherId"), col("CountryCode")
      ,col("PublisherName"), col("RevenueSplitPercentage"), col("TaxProfileId"), col("UniversalAccountId"), col("rowkey"), col("PublisherNameOverride"), col("ShortName"))

    VNextUtil.processFinalDataFrameNew(configReader, spark, finalDF,"publisherDim.files.historicalPath","publisherDim.files.mergedPath",  inputArgsMap,  "PublisherDim")

  }

  def readODataOfferPublishers(tableName: String): DataFrame = {

    var inputFilePath =inputArgsMap.get(tableName).get

    val df = spark.read.json(inputFilePath)

    val selectColumns = configReader.getValueForKey("publisherDim.ingest.tables." + tableName + ".columns").toString
    val selectColAliasList = parseSelectExprForAlias(selectColumns)

    val renamedDF = generateRenamedDF(selectColAliasList, df)
    var filteredDF = renamedDF
    //    filteredDF.show(100)

    val partitionBy = configReader.getValueForKey("publisherDim.ingest.tables." + tableName + ".partitionBy").toString
    val partitionCols = new ListBuffer[Column]
    partitionBy.split(",").foreach(x => partitionCols.append(filteredDF.col(x)))
    filteredDF
    //    versionDF.show(40)
    /* val PublisherAllDF = filteredDF.withColumn("PublisherNID",
       when(upper(col("PublisherId")) === "DA8888B8-2418-499B-9C0E-38D328F9A0B2","ABBYY")
         .otherwise(when(upper(col("PublisherId")) === "059AFC24-07DE-4126-B004-4E42A51816FE", "059afc24-07de-4126-b004-4e42a51816fe")
           .otherwise(when(upper(col("PublisherId")) === "9E4111A6-2681-418E-8FD9-556E6EECEECA", "MelissaData")
             .otherwise(when(upper(col("PublisherId")) === "12DDA869-A9A0-4C6A-82EC-5C8F0F430CFC","infogroup")
               .otherwise(when(upper(col("PublisherId")) === "10D16C70-F7F7-48A0-8B0C-23BDFB3A291A", "adobe").otherwise(col("PublisherNID")))))))

     val finalDF = PublisherAllDF.select(col("PublisherId"), col("PublisherNID").as("PublisherNaturalIdentifier")).distinct().where(col("PublisherId").isNotNull)
     finalDF*/

  }
  def readOffersListedTable(tableName: String): DataFrame = {

    var inputFilePath = inputArgsMap.get(tableName).get

    val df = spark.read.json(inputFilePath)

    val selectColumns = configReader.getValueForKey("publisherDim.ingest.tables." + tableName + ".columns").toString
    val selectColAliasList = parseSelectExprForAlias(selectColumns)

    val renamedDF = generateRenamedDF(selectColAliasList, df)
    var filteredDF = renamedDF
    //    filteredDF.show(100)

    val partitionBy = configReader.getValueForKey("publisherDim.ingest.tables." + tableName + ".partitionBy").toString
    val partitionCols = new ListBuffer[Column]
    partitionBy.split(",").foreach(x => partitionCols.append(filteredDF.col(x)))

    //    versionDF.show(40)
    val PublisherAllDF = filteredDF.withColumn("PublisherNID",
      when(upper(col("PublisherId")) === "DA8888B8-2418-499B-9C0E-38D328F9A0B2","ABBYY")
        .otherwise(when(upper(col("PublisherId")) === "059AFC24-07DE-4126-B004-4E42A51816FE", "059afc24-07de-4126-b004-4e42a51816fe")
          .otherwise(when(upper(col("PublisherId")) === "9E4111A6-2681-418E-8FD9-556E6EECEECA", "MelissaData")
            .otherwise(when(upper(col("PublisherId")) === "12DDA869-A9A0-4C6A-82EC-5C8F0F430CFC","infogroup")
              .otherwise(when(upper(col("PublisherId")) === "10D16C70-F7F7-48A0-8B0C-23BDFB3A291A", "adobe").otherwise(col("PublisherNID")))))))

    val finalDF = PublisherAllDF.select(col("PublisherId"), col("PublisherNID").as("PublisherNaturalIdentifier")).distinct().where(col("PublisherId").isNotNull)
    //finalDF.show(40)
    finalDF

  }

  def readPubPortalTable(tableName: String): DataFrame = {

    var path = inputArgsMap.get(tableName).get
    val finalDF = spark.read.json(path)

    finalDF

  }

  def readTables(tableName: String): DataFrame = {

    var path = inputArgsMap.get(tableName).get

    val df = spark.read.json(path)

    val selectColumns = configReader.getValueForKey("publisherDim.ingest.tables." + tableName + ".columns").toString

    val selectColAliasList = parseSelectExprForAlias(selectColumns)

    val renamedDF = generateRenamedDF(selectColAliasList, df)
    var filteredDF = renamedDF
    //    filteredDF.show(100)

    val partitionBy = configReader.getValueForKey("publisherDim.ingest.tables." + tableName + ".partitionBy").toString
    val partitionCols = new ListBuffer[Column]
    partitionBy.split(",").foreach(x => partitionCols.append(filteredDF.col(x)))

    filteredDF

  }



  def parseSelectExprForAlias(selectExpr: String): List[(String, String)] = {

    val selectColAliasList: ListBuffer[(String, String)] = new ListBuffer[(String, String)]

    val selectCols = selectExpr.split(",")
    selectCols.foreach(col => {
      val colAlias = col.split(" as ")
      val tuple = colAlias.length match {
        case 1 => (colAlias(0).trim, null)
        case 2 => (colAlias(0).trim, colAlias(1).trim)
      }
      selectColAliasList.append(tuple)
    })
    selectColAliasList.toList
  }

  def generateRenamedDF(selectColAliasList: List[(String, String)], df: DataFrame): DataFrame = {
    var transformedDF = df
    for ((name, alias) <- selectColAliasList) {
      alias match {
        case null => None
        case aliasName: String => transformedDF = transformedDF.withColumnRenamed(name, aliasName)
      }
    }
    transformedDF
  }

  def getHistoricalFile(): DataFrame = {
    try {
      val path = configReader.getValueForKey("publisherDim.files.historicalPath").toString
      val df = spark.read.json(path)
      df
    } catch {
      case e : Exception => { e.printStackTrace(); val df = null ; df }
    }
  }

  def getMergedFile(): DataFrame = {
    try {
      val path = configReader.getValueForKey("publisherDim.files.mergedPath").toString
      val df = spark.read.json(path)
      df
    } catch {
      case e: Exception => { e.printStackTrace(); val df = null ; df }
    }
  }


}