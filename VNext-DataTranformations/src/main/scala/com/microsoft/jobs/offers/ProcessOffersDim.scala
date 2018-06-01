package com.microsoft.jobs.offers

import java.util.Properties

import com.microsoft.common.DataFrameUtil
import com.microsoft.config.ConfigReader
import com.microsoft.framework.VNextUtil
import com.microsoft.framework.VNextUtil.getPropertyValue
import com.microsoft.jobs.publisher.ProcessPublisherDim.{readTables, _}
import org.apache.avro.generic.GenericData.StringType
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BooleanType, DateType, StringType}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType}
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.functions._
/**
  * This class is used for handling the Offers Dimension Stored Procedure
  */

object ProcessOffersDim {

  var spark: SparkSession = null
  val configReader = new ConfigReader("processOffers.conf")
  var inputArgsMap : Map[String, String] = null
  /**
    * This function reads the configuration from
    */
  def setup() = {

    val configProperties = new Properties
    val inputStream = this.getClass.getClassLoader.getResourceAsStream("global.properties")
    configProperties.load(inputStream)

    spark = SparkSession.builder
      .getOrCreate()

  }

  def main(args: Array[String]): Unit = {

    inputArgsMap = VNextUtil.getInputParametersAsKeyValue(args)
    setup()
    startTransformations()
  }


  def startTransformations() = {

    // First transformation or dataset
    val latestServicePlansDF = readServicePlanTable("latestServicePlans", transformPublisher, null)

    val latestServicePlansMeteredDF = readServicePlanTable("latestServicePlansMetered", transformPublisher, "PP_Effect")

    val latestOffersDF = readLatestOffersTable("latestOffers",transformPublisher)

    val distinctOffersDF = readDistinctOffersTable("OfferDetails", "distinctOffers2", transformPublisher)

    val manualOfferNameOverridesDF = readPubPortalTable("manualOfferNameOverrides")
    val manualServicePlanNameOverridesDF = readPubPortalTable("manualServicePlanNameOverrides")

    val vmDF = readVMTable("virtualMachine")

    val multosolutionDF = readMultiSolutionTable("multiSolution")

    val oDataDF = getODataOffers(spark, inputArgsMap)

    //oDataDF.show(100)

    val finalDF = joinAllDF(latestServicePlansDF, latestServicePlansMeteredDF, latestOffersDF, distinctOffersDF, vmDF, multosolutionDF, oDataDF, manualServicePlanNameOverridesDF, manualOfferNameOverridesDF)

   // println("About to write to blob")
   // finalDF.show(200)
    VNextUtil.processFinalDataFrameNew(configReader, spark, finalDF,"offersDim.files.historicalPath","offersDim.files.mergedPath", inputArgsMap, "OfferDim")
  }

  def readServicePlanTable(tableName: String, transformFn: (String, String) => (String), ppEffect: String): DataFrame = {

    val tablePath =inputArgsMap.get(tableName).get

    val df = spark.read.json(tablePath)

    val selectColumns = configReader.getValueForKey("offersDim.ingest.tables." + tableName + ".columns").toString
    val selectColAliasList = parseSelectExprForAlias(selectColumns)

    val renamedDF = generateRenamedDF(selectColAliasList, df)
    var filteredDF = renamedDF
    if ("PP_Effect".equalsIgnoreCase(ppEffect)) {
      filteredDF = filteredDF.where(filteredDF.col("PP_Effect") === "UsageChargeForMeteredResource")
    }
    val transform = udf(transformFn)
    val transformedDF = filteredDF.withColumn("PublisherName", transform(filteredDF.col("PublisherId"), filteredDF.col("PublisherNID"))).drop("PublisherNID").drop("PublisherNID")

    val partitionBy = configReader.getValueForKey("offersDim.ingest.tables." + tableName + ".partitionBy").toString
    val partitionCols = new ListBuffer[Column]
    partitionBy.split(",").foreach(x => partitionCols.append(transformedDF.col(x)))

    import org.apache.spark.sql.expressions.Window

    val byPartitionWindow = Window.partitionBy(partitionCols: _*).orderBy(transformedDF("version").desc)

    val versionDF = transformedDF.withColumn("versionrank", row_number().over(byPartitionWindow))

    val finalDF = versionDF.select("PublisherName", "ServicePlanName", "OfferName", "Version","versionrank").where("versionrank = 1")

    finalDF
  }

  def transformPublisher = (publisherId: String, publishernId: String) => {

    publisherId.toUpperCase() match {
      case "DA8888B8-2418-499B-9C0E-38D328F9A0B2" => "ABBYY"
      case "059AFC24-07DE-4126-B004-4E42A51816FE" => "059afc24-07de-4126-b004-4e42a51816fe"
      case "9E4111A6-2681-418E-8FD9-556E6EECEECA" => "MelissaData"
      case "12DDA869-A9A0-4C6A-82EC-5C8F0F430CFC" => "infogroup"
      case "10D16C70-F7F7-48A0-8B0C-23BDFB3A291A" => "adobe"

      case _ => publishernId
    }
  }

  def transformTimestampToDate = (timestamp: String) => {

    timestamp match {
      case str: String => val dateStr = timestamp.split(" "); dateStr(0)
      case _ => " "
    }
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

  def generateSelectCols(df: DataFrame, selectColsList: String*): ListBuffer[Column] = {

    val colList: ListBuffer[Column] = new ListBuffer[Column]
    selectColsList.foreach(col => colList.append(df.col(col)))
    colList
  }

  def readLatestOffersTable(tableName: String, transformFn: (String, String) => (String)): DataFrame = {

    var tablePath =inputArgsMap.get(tableName).get

    val df = spark.read.json(tablePath)

    val selectColumns = configReader.getValueForKey("offersDim.ingest.tables." + tableName + ".columns").toString
    val selectColAliasList = parseSelectExprForAlias(selectColumns)

    val renamedDF = generateRenamedDF(selectColAliasList, df)
    var filteredDF = renamedDF

    val transform = udf(transformFn)
    val trimVersion = (preview: String) => {

      val trimPreview = preview match {
        case null => "0"
        case _ => preview.trim
      }
      trimPreview.trim match {
        case "true" => 1
        case "1" => 1
        case _ => 0
      }
    }
    val trimPreviewUDF = udf(trimVersion)
    val timestampUDF = udf(DataFrameUtil.convertDateStringUDF)

    val transformedDF = filteredDF
      .withColumn("PublisherName", transform(filteredDF.col("PublisherId"), filteredDF.col("PublisherNID")))
      .withColumn("OfferCreatedDate", timestampUDF(filteredDF.col("Timestamp"), lit("yyyy-MM-dd HH:mm:ss.SSS"),  lit("[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}.[0-9]{2}.[0-9]{2}.[0-9]{3}")))
      .withColumn("IsPreview", trimPreviewUDF(filteredDF.col("IsPreview")))
      .drop("PublisherId")
      .drop("PublisherNID")
      .drop("TimeStamp")

    val partitionBy = configReader.getValueForKey("offersDim.ingest.tables." + tableName + ".partitionBy").toString
    val partitionCols = new ListBuffer[Column]
    partitionBy.split(",").foreach(x => partitionCols.append(transformedDF.col(x)))

    import org.apache.spark.sql.expressions.Window

    val byPartitionWindow = Window.partitionBy(partitionCols: _*).orderBy(transformedDF("Version").desc)
    val versionDF = transformedDF.withColumn("versionrank", row_number().over(byPartitionWindow))
      .withColumnRenamed("IsVersionNew", "IsVersion")

    val selectColsList = generateSelectCols(versionDF, "PublisherName", "OfferId", "OfferName", "OfferType", "versionrank", "OfferCreatedDate", "IsPreview", "Version")

    val finalDF = versionDF.select(selectColsList: _*).where(versionDF.col("versionrank") === 1 && versionDF("OfferName").isNotNull)//.drop("versionrank")

    finalDF

  }

  def readDistinctOffersTable(tableName: String, tableName2: String, transformFn: (String, String) => (String)): DataFrame = {

    var tablePath = inputArgsMap.get(tableName).get

    val df = spark.read.json(tablePath)
    df.cache()

    val filteredDF = df.select("*").where(!upper(df.col("PublisherId")).isin("DA8888B8-2418-499B-9C0E-38D328F9A0B2",
      "059AFC24-07DE-4126-B004-4E42A51816FE",
      "9E4111A6-2681-418E-8FD9-556E6EECEECA",
      "12DDA869-A9A0-4C6A-82EC-5C8F0F430CFC",
      "10D16C70-F7F7-48A0-8B0C-23BDFB3A291A"
    ))


    val transform = udf(transformFn)
    val transformedDF = filteredDF.withColumn("PublisherName", transform(filteredDF.col("PublisherId"), filteredDF.col("PublisherNID")))

    val partitionBy = configReader.getValueForKey("offersDim.ingest.tables." + tableName + ".partitionBy").toString
    val partitionCols = new ListBuffer[Column]
    partitionBy.split(",").foreach(x => partitionCols.append(transformedDF.col(x)))

    import org.apache.spark.sql.expressions.Window

    val byPartitionWindow = Window.partitionBy(partitionCols: _*).orderBy(transformedDF("Version").desc)

    val versionDF = transformedDF.withColumn("RN", row_number().over(byPartitionWindow))
    val finalDF = versionDF.where("RN = 1") //.drop("PublisherId").drop("PublisherNID")


    // Part 2
    val filteredDF2 = df.select("*").where(upper(df.col("PublisherId")).isin("DA8888B8-2418-499B-9C0E-38D328F9A0B2",
      "059AFC24-07DE-4126-B004-4E42A51816FE",
      "9E4111A6-2681-418E-8FD9-556E6EECEECA",
      "12DDA869-A9A0-4C6A-82EC-5C8F0F430CFC",
      "10D16C70-F7F7-48A0-8B0C-23BDFB3A291A"
    ))

    val transformedDF2 = filteredDF2.withColumn("PublisherName", transform(filteredDF2.col("PublisherId"), filteredDF2.col("PublisherNID")))

    val partitionBy2 = configReader.getValueForKey("offersDim.ingest.tables." + tableName + ".partitionBy").toString
    val partitionCols2 = new ListBuffer[Column]
    partitionBy2.split(",").foreach(x => partitionCols2.append(transformedDF.col(x)))

    import org.apache.spark.sql.expressions.Window

    val byPartitionWindow2 = Window.partitionBy(partitionCols: _*).orderBy(transformedDF2("Version").desc)

    val versionDF2 = transformedDF2.withColumn("RN", row_number().over(byPartitionWindow2))
    val finalDF2 = versionDF2.where("RN = 1") //.drop("PublisherId").drop("PublisherNID")

    val unionDF = DataFrameUtil.union(finalDF, finalDF2)

    unionDF
  }

  def readVMTable(tableName : String ) : DataFrame = {

    val df = DataFrameUtil.readDataFromBlobAndPartitionBy(configReader, "offersDim.ingest.tables.virtualMachine", 0, "virtualMachine", inputArgsMap, spark)

    df.select("*").where(col("OfferServicePlanVersion") === 1)
  }

  def readMultiSolutionTable(tableName : String ) : DataFrame = {

    val df = DataFrameUtil.readDataFrameFromBlob(configReader, "offersDim.ingest.tables.multiSolution","multiSolution", inputArgsMap,  spark)

    var selectCols = Seq(df.col("TemplateUri"),
      df.col("OfferName"),
      df.col("VersionPublished"),
      df.col("OfferTitle"),
      df.col("OfferDescription"),
      df.col("PublisherName"),
      df.col("categoryIds"))

    var filterDF =  df.select(selectCols: _*).where(col("categoryIds").like("%multiResourceSolution%"))

    filterDF = filterDF.drop("categoryIds")

    val nullCols = Seq("OfferId", "OfferNameOverride", "OfferExpiryDate", "VirtualMachineRepositoryId", "OfferCreatedDate", "LastStagingPublishDate", "LastProductionPublishDate",
      "ServicePlanId", "ServicePlanName", "ServicePlanTitle", "ServicePlanSummary", "ServicePlanDescription", "ServicePlanPaymentType", "ServicePlanBillingType", "IsExternallyBilled", "SP_RequiresExtLicense", "IsFree", "IsPreview",
      "OperatingSystemName", "OperatingSystemFamily", "ServicePlanCreatedDate", "IsUBB", "version", "versionrank","ServicePlanNameOverride")

    nullCols.foreach(col => {
      filterDF = filterDF.withColumn(col, lit(""))
    })

    filterDF = filterDF.withColumn("DataSource", lit("MultiSolution JSON").cast("string"))
    filterDF = filterDF.withColumn("IsMultisolution", lit(1))
    filterDF = filterDF.withColumn("OfferType", lit("MultiSolution").cast("string"))

    val finalDF = filterDF.distinct()

    finalDF
  }

  def joinAllDF(latestSPDF: DataFrame, lastSPMeteredDF: DataFrame, latestOffers: DataFrame, offerdetails: DataFrame, vmDF : DataFrame, multiSolutionDF : DataFrame, oDataDF : DataFrame, manualServicePlanNameOverridesDF : DataFrame, manualOfferNameOverridesDF : DataFrame): DataFrame = {

    val loJoinedLspDF = latestOffers.join(latestSPDF, Seq("OfferName", "PublisherName"), "left")

    var selectCols = Seq(loJoinedLspDF.col("OfferId"),
      loJoinedLspDF.col("OfferName"),
      latestOffers.col("Version").as("VersionPublished"),
      loJoinedLspDF.col("OfferCreatedDate"),
      loJoinedLspDF.col("ServicePlanName"),
      loJoinedLspDF.col("IsPreview"),
      loJoinedLspDF.col("OfferType"),
      loJoinedLspDF.col("PublisherName"))

    val loJoinedLspDF1 = loJoinedLspDF.select(selectCols: _*)

    val loLspOfferDtlsDF = loJoinedLspDF1.as("L").join(offerdetails.as("R"),
      loJoinedLspDF1.col("OfferName") === offerdetails.col("ServiceNID") &&
        loJoinedLspDF1.col("PublisherName") === offerdetails.col("PublisherName") &&
        loJoinedLspDF1.col("ServicePlanName") === offerdetails.col("SP_ServicePlanNID"), "left")



    val cols2 = Seq(
      loLspOfferDtlsDF.col("L.OfferId"),
      loLspOfferDtlsDF.col("OfferName"),
      loLspOfferDtlsDF.col("VersionPublished"),
      loLspOfferDtlsDF.col("OfferCreatedDate"),
      loLspOfferDtlsDF.col("L.ServicePlanName"),
      loLspOfferDtlsDF.col("IsPreview"),
      loLspOfferDtlsDF.col("OfferType"),
      loLspOfferDtlsDF.col("L.PublisherName"),
      loLspOfferDtlsDF.col("Title").as("OfferTitle"),
      loLspOfferDtlsDF.col("Description").as("OfferDescription"),
      loLspOfferDtlsDF.col("SP_Title").as("ServicePlanTitle"),
      loLspOfferDtlsDF.col("SP_Summary").as("ServicePlanSummary"),
      loLspOfferDtlsDF.col("SP_Description").as("ServicePlanDescription"),
      loLspOfferDtlsDF.col("IsExternallyBilled").as("IsExternallyBilled"),
      loLspOfferDtlsDF.col("SP_RequiresExtLicense").as("SP_RequiresExtLicense")
    )

    val loLspOfferDtlsDF1 = loLspOfferDtlsDF.select(cols2: _*)

    val allDF = loLspOfferDtlsDF1.as("ALL").join(lastSPMeteredDF.as("right"), Seq("PublisherName", "OfferName", "ServicePlanName"), "left")

    val cols3 = Seq(
      allDF.col("ALL.OfferId"),
      allDF.col("ALL.OfferName"),
      allDF.col("ALL.VersionPublished"),
      allDF.col("ALL.OfferCreatedDate"),
      allDF.col("ALL.ServicePlanName"),
      allDF.col("ALL.IsPreview"),
      allDF.col("ALL.OfferType"),
      allDF.col("ALL.PublisherName"),
      allDF.col("ALL.OfferTitle"),
      allDF.col("ALL.OfferDescription"),
      allDF.col("ALL.ServicePlanTitle"),
      allDF.col("ALL.ServicePlanSummary"),
      allDF.col("ALL.ServicePlanDescription"),
      allDF.col("ALL.IsExternallyBilled"),
      allDF.col("version"),
      allDF.col("versionrank"),
      allDF.col("ALL.SP_RequiresExtLicense")
    )

    var allDF1 = allDF.select(cols3: _*).withColumn("OfferType",when(allDF.col("OfferType") === "AppService" && allDF.col("ALL.ServicePlanName").isNotNull, "AppService B+C")
      .when(allDF("OfferType") === "VirtualMachine", "VirtualMachineService")
      .otherwise(allDF("OfferType")))

    val colNames = allDF1.columns.map(x => allDF1.col(x)).toList

    val colWithVMCols = colNames ::: Seq (vmDF.col("OperatingSystemName"), vmDF.col("OperatingSystemFamily")).toList

    allDF1 = allDF1.as("all").join(vmDF.as("vm"), Seq("PublisherName", "OfferName", "ServicePlanName"), "left").
      select( colWithVMCols:_*)

    val colWithVMCols1 = colWithVMCols ::: Seq (oDataDF.col("ServicePlanId"),oDataDF.col("VirtualMachineRepositoryId")).toList

    allDF1 = allDF1.as("all").join(oDataDF.as("odata"), Seq("PublisherName", "OfferName", "ServicePlanName"), "left").
      select( colWithVMCols1:_*)


    val colWithVMCols2 = colWithVMCols1 ::: Seq (manualServicePlanNameOverridesDF.col("PlanNaturalIdentifier")).toList

    allDF1 = allDF1.as("all").join(manualServicePlanNameOverridesDF, allDF1.col("OfferName") === manualServicePlanNameOverridesDF.col("OfferNaturalIdentifier") && allDF1.col("ServicePlanName")===manualServicePlanNameOverridesDF.col("PlanOverrideValue"), "left").
      select( colWithVMCols2:_*)

    val colWithVMCols3 = colWithVMCols2 ::: Seq (manualOfferNameOverridesDF.col("OfferNaturalIdentifier")).toList

    allDF1 = allDF1.as("all").join(manualOfferNameOverridesDF, allDF1.col("OfferName") === manualOfferNameOverridesDF.col("OfferOverrideValue") && allDF1.col("PublisherName")===manualOfferNameOverridesDF.col("PublisherNaturalIdentifier"), "left").
      select( colWithVMCols3:_*)

    val nullCols = Seq("OfferExpiryDate", "LastStagingPublishDate", "LastProductionPublishDate"
     , "ServicePlanPaymentType", "ServicePlanBillingType", "IsFree", "IsMultiSolution",
     "ServicePlanCreatedDate", "TemplateUri")

    nullCols.foreach(col => {
      allDF1 = allDF1.withColumn(col, lit(""))
    })

    allDF1 = allDF1.withColumn("ServicePlanNameOverride",allDF1.col("PlanNaturalIdentifier"))
    allDF1 = allDF1.withColumn("OfferNameOverride",allDF1.col("OfferNaturalIdentifier"))
    allDF1 = allDF1.drop("PlanNaturalIdentifier").drop("OfferNaturalIdentifier")

    allDF1 = allDF1.withColumn("DataSource",lit("PubPortal").cast("string"))
    allDF1 = allDF1.withColumn("IsUBB",when(allDF1.col("OfferType") === "AppService" && allDF1.col("ALL.ServicePlanName").isNotNull,1)
      .otherwise(0))
    allDF1 = allDF1.withColumn("SP_RequiresExtLicense",when(allDF1.col("ALL.SP_RequiresExtLicense") === "True" ,1)
      .otherwise(0))

    allDF1 = DataFrameUtil.union(allDF1, multiSolutionDF)

    allDF1 = allDF1.withColumn("rowKey",concat_ws("_", when(allDF1.col("PublisherName").isNotNull ,
      allDF1.col("PublisherName")).otherwise(""),when(allDF1.col("OfferName").isNotNull ,
      allDF1.col("OfferName")).otherwise(""),when(allDF1.col("ServicePlanName").isNotNull ,
      allDF1.col("ServicePlanName")).otherwise("")))



    //allDF1= allDF1.join(oDataDF, Seq("OfferName", "PublisherName", "ServicePlanName"), "left")
    //allDF1 = allDF1.join(oDataDF,
      //allDF1.col("OfferName") === getODataOffers(spark, inputArgsMap).col("OfferName") &&
        //allDF1.col("PublisherName") === getODataOffers(spark, inputArgsMap).col("PublisherName") &&
        //allDF1.col("ServicePlanName") === getODataOffers(spark, inputArgsMap).col("ServicePlanName"), "left")

    //allDF1 = allDF1.withColumn("LA.ServicePlanId",when(col("LA.ServicePlanId") === "" ,col("RA.ServicePlanId"))
      //.otherwise(col("LA.ServicePlanId")))
    //allDF1 = allDF1.withColumn("ServicePlanId",oDataDF.col("ServicePlanId"))

    val withDateStrDF = DataFrameUtil.convertDateColumnsToString(allDF1, Seq("OfferCreatedDate"))

    withDateStrDF
  }
  def getODataOffers(spark: SparkSession, inputArgsMap: Map[String, String]): DataFrame = {

    try {

      var filePath = getPropertyValue(inputArgsMap, "oDataPath")

      var filteredDF = spark.read.json(filePath)

     // filteredDF = filteredDF.withColumn("IsMultiSolution", filteredDF.col("IsMultiSolution").cast(StringType))
     // filteredDF = filteredDF.withColumn("IsExternallyBilled", filteredDF.col("IsExternallyBilled").cast(StringType))
      //filteredDF = filteredDF.withColumn("IsUBB", filteredDF.col("IsUBB").cast(StringType))
      //filteredDF = filteredDF.withColumn("version", filteredDF.col("version").cast(StringType))
     // filteredDF = filteredDF.withColumn("versionrank", filteredDF.col("versionrank").cast(StringType))
      //filteredDF = DataFrameUtil.convertDateColumnsToString(filteredDF, Seq("OfferCreatedDate"))

      //val newfinalDF = DataFrameUtil.union(finalDF, filteredDF)
      //filteredDF.show()
      filteredDF

    }catch {
      case e: Exception => {
        e.printStackTrace(); val df = null; df
      }
    }
  }
  def readPubPortalTable(tableName: String): DataFrame = {

    var path = inputArgsMap.get(tableName).get
    val finalDF = spark.read.json(path)

    finalDF

  }
}