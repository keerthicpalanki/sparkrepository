package com.microsoft.jobs.order

import java.text.SimpleDateFormat
import java.util.{Date, GregorianCalendar, Properties}

import com.microsoft.common.{ColumnSchema, DataFrameUtil, PushToSQL}
import com.microsoft.config.ConfigReader
import com.microsoft.framework.VNextUtil
import com.microsoft.framework.VNextUtil.extractPreviousPath
import com.microsoft.jobs.offers.ProcessOffersDim.{configReader, inputArgsMap, spark}
import org.apache.spark.sql.functions.{col, sum, _}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object ProcessOrders {

  val MAX_TIME_IN_MILLIS = 253370745000000l

  val LICENSE_TYPE_MAX_TIME = 32503573800000l //2999-12-31
  val LICENSE_TYPE_MIN_TIME = -2209008600000l //1900-01-01

  var spark: SparkSession = null
  val configReader = new ConfigReader("processOrders.conf")
  var inputArgsMap: Map[String, String] = null

  /**
    * This function reads the configuration from
    */
  def setup() = {

    val configProperties = new Properties
    val inputStream = this.getClass.getClassLoader.getResourceAsStream("global.properties")
    configProperties.load(inputStream)

    spark = SparkSession.builder
      .getOrCreate()

    spark.conf.set("spark.sql.crossJoin.enabled", "true")
    spark.conf.set("spark.sql.cbo.enabled", "true")
    spark.conf.set("spark.sql.joinReorder.enabled", "true")


  }

  def processNaturalIdentifier = (identifier: String) => {

    val index = identifier.indexOf("-646")
    val previewIndex = identifier.indexOf("-preview")
    val endIndex: Int = (index, previewIndex) match {
      case (0, 0) => identifier.length
      case (0, x) => x
      case (x, 0) => x
      case _ => identifier.length
    }
    identifier.substring(0, endIndex)
  }

  def getAlternateIDFromRight = (alternateId: String, len: Int) => {
    if (alternateId.length > 36) alternateId takeRight (len) else alternateId
  }

  def main(args: Array[String]): Unit = {

    inputArgsMap = VNextUtil.getInputParametersAsKeyValue(args)
    setup()
    val cosmosOrdersOrderHistoryDF = DataFrameUtil.readDataFrameFromBlobAsTSV(configReader, "orderDim.ingest.tables.cosmosOrdersOrderHistoryEntries", "cosmosOrdersOrderHistoryEntries", inputArgsMap,  spark)
    val cosmosOrdersOrderHistoryEffectiveDate19DF = addEffectiveDate19("orderDim.ingest.tables.cosmosOrdersOrderHistoryEntries", cosmosOrdersOrderHistoryDF)
    val cosmosOrdersAlternateIdToOrderIdDF = DataFrameUtil.readDataFrameFromBlobAsTSV(configReader, "orderDim.ingest.tables.cosmosOrdersAlternateIdToOrderIdMappings", "cosmosOrdersAlternateIdToOrderIdMappings", inputArgsMap,  spark)
    val cosmosOrdersExternalBillingIdsDF = DataFrameUtil.readDataFrameFromBlobAsTSV(configReader, "orderDim.ingest.tables.cosmosOrdersExternalBillingIds", "cosmosOrdersExternalBillingIds", inputArgsMap,  spark)
    val cosmosOrdersMeteredResourcePricesDF = DataFrameUtil.readDataFrameFromBlobAsTSV(configReader, "orderDim.ingest.tables.cosmosOrdersMeteredResourcePrices", "cosmosOrdersMeteredResourcePrices", inputArgsMap,  spark)

    cosmosOrdersOrderHistoryEffectiveDate19DF.persist()
    cosmosOrdersAlternateIdToOrderIdDF.persist()
    cosmosOrdersExternalBillingIdsDF.persist()
    cosmosOrdersMeteredResourcePricesDF.persist()

    val cancelledEventDF = getCancelledEvents(cosmosOrdersOrderHistoryEffectiveDate19DF)
    val effectivePriceDF = getEffectivePriceDates(cosmosOrdersOrderHistoryEffectiveDate19DF, cancelledEventDF)
    val alternateIdDF = getAlternateIds(cosmosOrdersAlternateIdToOrderIdDF)

    // Step 3 of SQL
    val orderHistoryDF = getOrderHistory(cosmosOrdersOrderHistoryEffectiveDate19DF, alternateIdDF, cosmosOrdersExternalBillingIdsDF)
    cosmosOrdersOrderHistoryEffectiveDate19DF.unpersist()
    cosmosOrdersAlternateIdToOrderIdDF.unpersist()
    cosmosOrdersExternalBillingIdsDF.unpersist()
    orderHistoryDF.persist()

    // Step 4 of SQL
    val orderHistoryFixedDF = getOrderHistoryFixed(orderHistoryDF)
    orderHistoryFixedDF.persist()
    effectivePriceDF.persist()
    orderHistoryDF.unpersist()

    // Step 5 of SQL
    val orderApiPricesDF = getOrdersAPIPrices(effectivePriceDF, orderHistoryFixedDF, cosmosOrdersMeteredResourcePricesDF)
    cosmosOrdersMeteredResourcePricesDF.unpersist()
    effectivePriceDF.unpersist()


    // Step 6 of SQL
    val (ordersAPIPricesMainDF, ordersDF, currentOrderStatusDF) = getPricesAndOrderStatus(orderApiPricesDF)
    ordersAPIPricesMainDF.persist()

    // Step 7 of SQL
    val (latestOrderPriceDF, dataMarketSubIDDF) = getLatestOrderPricesAndDataMarketSubIDDF(ordersAPIPricesMainDF)

    //     Get the datasets for the final DF
    //     ordersAPIPricesMainDF will be required for the final join
    //     latestOrderPriceDF will be required for the final join
    val ordersTableDF = DataFrameUtil.partitionDataFrame(configReader, "orderDim.ingest.tables.orders", ordersDF, 0).where("RN = 1")
    val publisherDimDF = DataFrameUtil.readDataFrameFromBlob(configReader, "orderDim.ingest.tables.publisherDim", "publisherDim", inputArgsMap,  spark)

    val offerDimDF = DataFrameUtil.readDataFrameFromBlob(configReader, "orderDim.ingest.tables.offerDim", "offerDim", inputArgsMap,  spark)
    val distinctOffersLangHistoricalDF = DataFrameUtil.readDataFromBlobAndPartitionBy(configReader, "orderDim.ingest.tables.distinctOffersLanguageHistorical", 0, "distinctOffersLanguageHistorical", inputArgsMap,  spark)
    val distinctServicePlansByMarketHistoricalDF = getDistinctServicePlansByMarketHistoricalDF()

    val transform = udf(processNaturalIdentifier)
    val ordersTableNewDF = ordersTableDF.withColumn("OfferNaturalIdentifierSubStr", transform(ordersTableDF.col("OfferNaturalIdentifier")))

    val servicePlanNameOverridesDF = DataFrameUtil.readDataFrameFromBlob(configReader, "orderDim.ingest.tables.servicePlanNameOverrides", "servicePlanNameOverrides", inputArgsMap,  spark)

    val odataOfferOnboardingsSPDF = DataFrameUtil.readDataFromBlobAndPartitionBy(configReader, "orderDim.ingest.tables.odataOfferOnboardingsSP", 1, "odataOfferOnboardingsSP", inputArgsMap,  spark)

    var finalDF = generateFinalDataFrameForMergeAndUpdate(ordersTableNewDF, publisherDimDF, offerDimDF, servicePlanNameOverridesDF,
      distinctOffersLangHistoricalDF, distinctServicePlansByMarketHistoricalDF, latestOrderPriceDF, ordersAPIPricesMainDF, odataOfferOnboardingsSPDF, dataMarketSubIDDF)

    val finalOrderAPIPricesDF = processAPIPrices(ordersAPIPricesMainDF)

    finalDF = processDFForResourceURI(finalDF)

    finalDF = finalDF.withColumn("CurrentStatus", when(lower(col("CurrentStatus")) === "converted","Cancelled").otherwise(col("CurrentStatus")))

    val multosolutionDF = readMultiSolutionTable("multiSolution")

    finalDF = DataFrameUtil.union(finalDF, multosolutionDF)

    finalDF = finalDF.withColumn("CustomerRegion",lit(""))

    val colNames = finalDF.columns.map(x => finalDF.col(x)).toList

    val selectCols = colNames ::: Seq(dataMarketSubIDDF.col("DataMarketSubscriptionID")).toList

    finalDF = finalDF.join(dataMarketSubIDDF,
      finalDF.col("ParentOrderId") === dataMarketSubIDDF.col("OrderId"), "left").select( selectCols:_*)

    // Add steps for ResourceURI and LicenseType
    val finalOrderDimDF = processAzureLicenseType(cosmosOrdersAlternateIdToOrderIdDF, cosmosOrdersExternalBillingIdsDF, finalDF)

    getSubscriptionsFromOrders(finalOrderDimDF)

    VNextUtil.processFinalDataFrameNew(configReader, spark, finalOrderDimDF, "orderDim.files.historicalPath", "orderDim.files.mergedPath", inputArgsMap,  "OrderDim")

    VNextUtil.processFinalDataFrameApi(configReader, spark, finalOrderAPIPricesDF, "orderDim.files.api.historicalPath", "orderDim.files.api.mergedPath", inputArgsMap,  "ApiPrices")

  }

  def addEffectiveDate19(path: String, df: DataFrame): DataFrame = {
    val fileConfig = configReader.getConfig().getConfig(path)

    import scala.collection.JavaConversions._
    val configList = fileConfig.getConfigList("schema")

    var columnSchema = configList.map(x => {
      val schema = new ColumnSchema(x)
      schema
    }).toList

    val effectiveDateCol = columnSchema.filter(x => x.name.equals("EffectiveDate"))(0)

    val format = effectiveDateCol.sourceFormat.substring(0, 19)

    val tempDF = df.withColumn(effectiveDateCol.name + "19", substring(df.col("EffectiveDate_Orig"), 0, 19))
    val dateUDF = udf(DataFrameUtil.convertDateStringUDF)
    val finalDF = tempDF.withColumn(effectiveDateCol.name + "19", dateUDF(tempDF.col(effectiveDateCol.name + "19"), lit("yyyy-MM-dd HH:mm:ss"), lit(null)))
    finalDF
  }

  def getDistinctServicePlansByMarketHistoricalDF(): DataFrame = {

    val df = DataFrameUtil.readDataFrameFromBlob(configReader, "orderDim.ingest.tables.distinctServicePlansByMarketHistorical", "distinctServicePlansByMarketHistorical", inputArgsMap,  spark)
    val readDF = df.where(df.col("PP_Effect") =!= "FreeTrial")
    val finalDF = readDF.groupBy("OfferId", "ServicePlanNID").agg(sum(readDF.col("PP_Amount")).as("PP_Amount"))

    finalDF.where(finalDF.col("PP_Amount") === 0)

  }

  def getCancelledEvents(df: DataFrame): DataFrame = {

    val groupedDF = df.groupBy(df.col("OrderID"), df.col("Name")).agg(df.col("OrderID").as("ParentOrderID"), min(df.col("EffectiveDate")).as("CancelDate")).
      where(df.col("Name") === "Canceled")

    groupedDF.drop("OrderID")

  }

  def getEffectivePriceDates(cosmosOrderHistoryDF: DataFrame, cancelledDF: DataFrame): DataFrame = {

    val joinedDF = cosmosOrderHistoryDF.as("oh").join(cancelledDF.as("ce"), cosmosOrderHistoryDF.col("OrderID") === cancelledDF.col("ParentOrderID"), "left")
    val selectDF = joinedDF.select(
      joinedDF.col("oh.OrderID").as("OrderID"),
      joinedDF.col("ce.ParentOrderID").as("ParentOrderID"),
      when(joinedDF.col("ce.Name").isNotNull, joinedDF.col("ce.Name")).otherwise("Active").as("Name"),
      when(joinedDF.col("ce.ParentOrderID").isNotNull, joinedDF.col("ce.CancelDate")).otherwise(lit(MAX_TIME_IN_MILLIS).cast(LongType)).as("MaxEffectiveDate"),
      joinedDF.col("EffectiveDate"), joinedDF.col("CancelDate"))

    val groupedDF = selectDF.groupBy("OrderID", "ParentOrderId", "Name", "MaxEffectiveDate").agg(min("EffectiveDate").as("MinEffectiveDate"))

    val finalDF = groupedDF.select(col("OrderID").as("ParentOrderID"), col("Name"), col("MinEffectiveDate"), col("MaxEffectiveDate"))

    finalDF

  }

  def getAlternateIds(alternateIdDF: DataFrame): DataFrame = {

    val substringUDF = udf[String, String, Int](getAlternateIDFromRight)
    val finalDF = alternateIdDF.select(alternateIdDF.col("OrderId"), alternateIdDF.col("AlternateID"),
      alternateIdDF.col("EffectiveDate"), substringUDF(alternateIdDF.col("AlternateID"), lit(36)).as("AlternateIDNew"))
      .where(length(alternateIdDF.col("AlternateID")) >= 36) // TO DO - use unique idenitfier
    finalDF.drop("AlternateID").withColumnRenamed("AlternateIDNew", "AlternateID")
  }

  /**
    * step 3 of the sql
    *
    * @param df
    * @param alternateIDDF
    * @return
    */
  def getOrderHistory(cosmosOrdersOrderHistoryDF: DataFrame, alternateIDDF: DataFrame, externalBillingIdsDF: DataFrame): DataFrame = {

    val readDF = DataFrameUtil.partitionDataFrame(configReader, "orderDim.ingest.tables.dedupeOrdersHistory", cosmosOrdersOrderHistoryDF, 1)
    val dedupDF = readDF.where(readDF.col("DuplicateRank") === 1)

    val orderHistoryPrimeDF = dedupDF.as("o").join(alternateIDDF.as("a"), Seq("OrderID", "EffectiveDate"))

    val selectCols = Seq(
      orderHistoryPrimeDF("Name"),
      orderHistoryPrimeDF.col("o.OrderID").as("ParentOrderID"),
      orderHistoryPrimeDF.col("a.AlternateID").as("OrderID"),
      orderHistoryPrimeDF.col("o.Product_PublisherNaturalIdentifier").as("PublisherNaturalIdentifier"),
      orderHistoryPrimeDF.col("o.Product_OfferNaturalIdentifier").as("OfferNaturalIdentifier"),
      orderHistoryPrimeDF.col("o.Product_ServicePlanNaturalIdentifier").as("PlanNaturalIdentifier"),
      orderHistoryPrimeDF.col("o.Prices_Monthly_Amount").as("Price"),
      orderHistoryPrimeDF.col("o.Prices_Monthly_CurrencyCode").as("CurrencyCode"),
      orderHistoryPrimeDF.col("o.CustomerProvidedOrderName"),
      orderHistoryPrimeDF.col("o.EffectiveDate"), orderHistoryPrimeDF.col("Id")
    )

    val orderHistoryPrimeDF1 = orderHistoryPrimeDF.select(selectCols: _*)

    //
    val orderHistoryDF = dedupDF.as("o").join(alternateIDDF.as("a"), dedupDF.col("OrderID") === alternateIDDF.col("OrderID") &&
      dedupDF.col("EffectiveDate") === alternateIDDF.col("EffectiveDate"), "left")
    val orderHistoryDF2 = orderHistoryDF.select(selectCols: _*).where(col("a.OrderID").isNull)

    val finalOrderHistoryPrimeDF = DataFrameUtil.union(orderHistoryPrimeDF1, orderHistoryDF2)

    val externalBillingDF = externalBillingIdsDF.withColumnRenamed("OrderID", "ParentOrderID")

    val orderHistoryExternalBillingDF = finalOrderHistoryPrimeDF.as("o").join(externalBillingDF.as("e"), Seq("ParentOrderID", "EffectiveDate"), "left")
      .where(finalOrderHistoryPrimeDF.col("OrderID") =!= externalBillingDF.col("Billing_Id") ||
        (finalOrderHistoryPrimeDF.col("OrderID").isNull || externalBillingDF.col("Billing_Id").isNull)
      )

    val df = DataFrameUtil.partitionDataFrame(orderHistoryExternalBillingDF, "ParentOrderID", "EffectiveDate ASC,Name desc", "Ranks", 1)

    val finalDF = df.select(
      col("Ranks"),
      col("Name"),
      col("ParentOrderID"),
      col("OrderID"), col("CustomerProvidedOrderName"), // TBD CustomerProvidedOrderName
      col("PublisherNaturalIdentifier"), col("OfferNaturalIdentifier"), col("PlanNaturalIdentifier"), col("Price"), col("CurrencyCode"),
      col("EffectiveDate"), col("Billing_Id").as("ExternalBillingID"), col("Billing_SystemName").as("BillingSystemName"), col("ID"))

    finalDF
  }


  def getOrderHistoryFixed(df: DataFrame): DataFrame = {
    // get Migrated Orders DF
    val migratedDF = df.select(df.col("ParentOrderID").as("Migrated_ParentOrderID"), df.col("Name")).where(df.col("Name") === "External Billing Migrated").drop("Name").distinct()

    val oPrimeDF = df.select(
      df.col("OrderID").as("OrderID1"),
      df.col("ExternalBillingID").as("E1"), df.col("BillingSystemName").as("B1"),
      df.col("ParentOrderID").as("P1"), df.col("Ranks").as("R1"))

    val oPrimeDF2 = df.select(
      df.col("OrderID").as("OrderID2"),
      df.col("ExternalBillingID").as("E2"), df.col("BillingSystemName").as("B2"),
      df.col("ParentOrderID").as("P2"), df.col("Ranks").as("R2"))

    val tempOrderHistoryFixDF1 = df.as("o").join(oPrimeDF.as("o1"),
      (df.col("ParentOrderID") === oPrimeDF.col("P1") && df.col("Ranks") - 1 === oPrimeDF.col("R1")), "left")
      .join(oPrimeDF2.as("o2"), (df.col("ParentOrderID") === oPrimeDF2.col("P2") && df.col("Ranks") - 2 === oPrimeDF2.col("R2")), "left")
      .join(migratedDF.as("m"), df.col("ParentOrderID") === migratedDF.col("Migrated_ParentOrderID"), "left")
      .select(
        lit(1).as("Dedupe"), df.col("Name"), df.col("ParentOrderID"),
        coalesce(df.col("OrderID"), oPrimeDF.col("OrderID1"), oPrimeDF2.col("OrderID2")).as("OrderID"),
        df.col("PublisherNaturalIdentifier"), df.col("OfferNaturalIdentifier"),
        df.col("PlanNaturalIdentifier"), df.col("Price"), df.col("CurrencyCode"), df.col("EffectiveDate"), df.col("ID"),
        coalesce(df.col("ExternalBillingID"), oPrimeDF.col("E1"), oPrimeDF2("E2")).as("ExternalBillingID"),
        coalesce(df.col("BillingSystemName"), oPrimeDF.col("B1"), oPrimeDF2("B2")).as("BillingSystemName"),
        migratedDF.col("Migrated_ParentOrderID"), df.col("CustomerProvidedOrderName"))
      .where(migratedDF.col("Migrated_ParentOrderID").isNull)

    val orderHistoryFixDF1 = tempOrderHistoryFixDF1.drop("Migrated_ParentOrderID")

    val orderHistoryFixDF2 = df.as("o").join(oPrimeDF.as("o1"),
      (df.col("ParentOrderID") === oPrimeDF.col("P1") && df.col("Ranks") - 1 === oPrimeDF.col("R1")), "left")
      .join(migratedDF.as("m"), df.col("ParentOrderID") === migratedDF.col("Migrated_ParentOrderID"), "left")
      .withColumn("OrderID", coalesce(df.col("OrderID"), oPrimeDF.col("OrderID1")))
      .withColumn("ExternalBillingID", coalesce(df.col("ExternalBillingID"), oPrimeDF.col("E1")))
      .withColumn("BillingSystemName", coalesce(df.col("BillingSystemName"), oPrimeDF.col("B1")))
      .where(migratedDF.col("Migrated_ParentOrderID").isNotNull)

    val partitionedDF = DataFrameUtil.partitionDataFrame(orderHistoryFixDF2, "OrderID,ExternalBillingID", "EffectiveDate DESC", "Dedupe", 1)

    val partitionedDF1 = partitionedDF.select(
      partitionedDF.col("Dedupe"),
      partitionedDF.col("Name"), partitionedDF.col("ParentOrderID"),
      partitionedDF.col("OrderID"),
      partitionedDF.col("PublisherNaturalIdentifier"), partitionedDF.col("OfferNaturalIdentifier"),
      partitionedDF.col("PlanNaturalIdentifier"), partitionedDF.col("Price"), partitionedDF.col("CurrencyCode"), partitionedDF.col("EffectiveDate"), partitionedDF.col("ID"),
      partitionedDF.col("ExternalBillingID"), partitionedDF.col("BillingSystemName"), partitionedDF.col("CustomerProvidedOrderName"))

    val orderHistoryFixDF = DataFrameUtil.union(orderHistoryFixDF1, partitionedDF1)

    val finalPartitionedDF = DataFrameUtil.partitionDataFrame(orderHistoryFixDF, "ParentOrderID", "EffectiveDate asc,Name desc", "Ranks", 1)

    val orderHistoryFixedDF = finalPartitionedDF.select(
      finalPartitionedDF.col("Ranks"),
      finalPartitionedDF.col("Name"),
      finalPartitionedDF.col("ParentOrderID"),
      finalPartitionedDF.col("OrderID"),
      finalPartitionedDF.col("PublisherNaturalIdentifier"),
      finalPartitionedDF.col("OfferNaturalIdentifier"),
      finalPartitionedDF.col("PlanNaturalIdentifier"),
      finalPartitionedDF.col("Price"),
      finalPartitionedDF.col("CurrencyCode"),
      finalPartitionedDF.col("EffectiveDate"),
      finalPartitionedDF.col("ExternalBillingID"),
      finalPartitionedDF.col("BillingSystemName"),
      finalPartitionedDF.col("ID"),
      finalPartitionedDF.col("Dedupe"),
      finalPartitionedDF.col("CustomerProvidedOrderName")
    ).where(finalPartitionedDF.col("Dedupe") === 1).drop("Dedupe")

    orderHistoryFixedDF
  }

  /**
    * Step 5 of the SQL File
    *
    * @param effectivePriceDatesDF
    * @param orderHistoryFixedDF
    * @param cosmosOrderedMeteredResourcesDF
    * @return
    */
  def getOrdersAPIPrices(effectivePriceDatesDF: DataFrame, orderHistoryFixedDF: DataFrame, cosmosOrderedMeteredResourcesDF: DataFrame): DataFrame = {

    val checkTimeUDF = udf(verifyTimeUDF)

    val orderHistoryFixedDF1 = orderHistoryFixedDF.select(
      orderHistoryFixedDF.col("ParentOrderID").as("P1"),
      orderHistoryFixedDF.col("Ranks").as("R1"),
      orderHistoryFixedDF.col("Name").as("N1"),
      orderHistoryFixedDF.col("EffectiveDate").as("EffDate1"))

    val orderHistoryFixedDF2 = orderHistoryFixedDF.select(
      orderHistoryFixedDF.col("ParentOrderID").as("P2"),
      orderHistoryFixedDF.col("Ranks").as("R2"),
      orderHistoryFixedDF.col("Name").as("N2"),
      orderHistoryFixedDF.col("ExternalBillingID").as("BillingID2"),
      orderHistoryFixedDF.col("BillingSystemName").as("SystemName2"),
      orderHistoryFixedDF.col("EffectiveDate").as("EffDate2"))

    val orderApiDF1 = orderHistoryFixedDF.as("o").join(effectivePriceDatesDF.as("e"), Seq("ParentOrderID"), "left")
      .join(orderHistoryFixedDF1.as("o2"), orderHistoryFixedDF.col("ParentOrderID") === orderHistoryFixedDF1.col("P1") &&
        orderHistoryFixedDF.col("Ranks") + 1 === orderHistoryFixedDF1.col("R1"), "left")
      .join(orderHistoryFixedDF2.as("oPrime"), orderHistoryFixedDF.col("ParentOrderID") === orderHistoryFixedDF2.col("P2") &&
        orderHistoryFixedDF.col("Ranks") - 1 === orderHistoryFixedDF2.col("R2"), "left")
      .join(cosmosOrderedMeteredResourcesDF.as("mrp"), orderHistoryFixedDF.col("Id") === cosmosOrderedMeteredResourcesDF.col("OrderHistoryEntryId"), "left")
      .select(
        orderHistoryFixedDF.col("Name").as("EventStartName"),
        when(lower(effectivePriceDatesDF.col("Name")) === "canceled"
          && effectivePriceDatesDF.col("MaxEffectiveDate") === orderHistoryFixedDF1.col("EffDate1"), "Cancelled")
          .when(lower(orderHistoryFixedDF1.col("N1")).isin("trial ended", "promotion discount ended") &&
            orderHistoryFixedDF1.col("EffDate1") > unix_timestamp(current_date()), "Active")
          .when(orderHistoryFixedDF1.col("N1").isNull, "Active")
          .otherwise(orderHistoryFixedDF1.col("N1")).as("EventEndName"),

        orderHistoryFixedDF.col("ParentOrderID").as("ParentOrderID"),
        coalesce(orderHistoryFixedDF.col("OrderID"), orderHistoryFixedDF.col("ParentOrderID")).as("OrderID"),
        orderHistoryFixedDF.col("PublisherNaturalIdentifier").as("PublisherNaturalIdentifier"),
        orderHistoryFixedDF.col("OfferNaturalIdentifier").as("OfferNaturalIdentifier"),
        orderHistoryFixedDF.col("PlanNaturalIdentifier").as("PlanNaturalIdentifier"),
        orderHistoryFixedDF.col("CurrencyCode").as("CurrencyCode"),
        orderHistoryFixedDF.col("CustomerProvidedOrderName"), // This is new .. TBD need to discuss

        when(cosmosOrderedMeteredResourcesDF.col("MeterName").isNull, orderHistoryFixedDF.col("Price"))
          .when(cosmosOrderedMeteredResourcesDF.col("ID").isNotNull, cosmosOrderedMeteredResourcesDF.col("Amount"))
          .otherwise(lit(null).cast(FloatType)).as("Price"),

        coalesce(cosmosOrderedMeteredResourcesDF.col("MeterName"), lit("Monthly").cast(StringType)).as("MeterName"),

        when(lower(orderHistoryFixedDF1.col("N1")) === "promotion discount ended", "Promotion")
          .when(lower(orderHistoryFixedDF1.col("N1")) === "trial ended", "Trial")
          .otherwise("None").as("DiscountType"),

        when(checkTimeUDF(orderHistoryFixedDF.col("EffectiveDate")), orderHistoryFixedDF.col("EffectiveDate") + 1000)
          .when(orderHistoryFixedDF.col("EffectiveDate") === orderHistoryFixedDF2.col("EffDate2"), orderHistoryFixedDF.col("EffectiveDate") + 2000)
          .when(orderHistoryFixedDF.col("EffectiveDate") - 1000 === orderHistoryFixedDF2.col("EffDate2"), orderHistoryFixedDF.col("EffectiveDate") + 2000)
          .otherwise(orderHistoryFixedDF.col("EffectiveDate")).as("PriceStartDate"),

        when(orderHistoryFixedDF1.col("EffDate1").isNull, effectivePriceDatesDF.col("MaxEffectiveDate"))
          .when(checkTimeUDF(orderHistoryFixedDF1.col("EffDate1")), orderHistoryFixedDF1.col("EffDate1"))
          .when(orderHistoryFixedDF1.col("EffDate1") === orderHistoryFixedDF.col("EffectiveDate"), orderHistoryFixedDF.col("EffectiveDate") + 1000)
          .when(orderHistoryFixedDF1.col("EffDate1") - 1000 === orderHistoryFixedDF.col("EffectiveDate"), orderHistoryFixedDF1.col("EffDate1") + 1000)
          .otherwise(orderHistoryFixedDF1.col("EffDate1") - 1000).as("PriceEndDate"),

        orderHistoryFixedDF.col("ID").as("MeteredPriceID"),
        coalesce(orderHistoryFixedDF.col("ExternalBillingID"), orderHistoryFixedDF2.col("BillingID2")).as("ExternalBillingID"),
        coalesce(orderHistoryFixedDF.col("BillingSystemName"), orderHistoryFixedDF2.col("SystemName2")).as("BillingSystemName"),
        orderHistoryFixedDF.col("Name"),
        orderHistoryFixedDF1.col("EffDate1"),
        effectivePriceDatesDF.col("MaxEffectiveDate"),
        orderHistoryFixedDF.col("EffectiveDate"),
        effectivePriceDatesDF.col("MinEffectiveDate"),
        orderHistoryFixedDF1.col("N1")
      )
      .where(
        lower(orderHistoryFixedDF.col("Name")) =!= "canceled" &&       // Cond 1
          (isnull(orderHistoryFixedDF1.col("EffDate1")) ||
            (orderHistoryFixedDF1.col("EffDate1") <= effectivePriceDatesDF.col("MaxEffectiveDate"))) &&      // condition 2
          ( (orderHistoryFixedDF.col("EffectiveDate") < effectivePriceDatesDF.col("MaxEffectiveDate") ) || // sub condition 3a
            effectivePriceDatesDF.col("MinEffectiveDate") ===  effectivePriceDatesDF.col("MaxEffectiveDate")) // sub condition 3b
          && (
          when(((effectivePriceDatesDF.col("MinEffectiveDate") === orderHistoryFixedDF.col("EffectiveDate")) && (lower(orderHistoryFixedDF.col("Name")) === "price changed")), false)
            .when(((effectivePriceDatesDF.col("MinEffectiveDate") === orderHistoryFixedDF.col("EffectiveDate")) && (lower(orderHistoryFixedDF1.col("N1")) === "placed")), false).otherwise(true)
          )
      )

    val finalDF = orderApiDF1.
      drop(orderApiDF1.col("Name"))
      .drop(orderApiDF1.col("EffDate1"))
      .drop(orderApiDF1.col("MaxEffectiveDate"))
      .drop(orderApiDF1.col("EffectiveDate"))
      .drop(orderApiDF1.col("MinEffectiveDate"))
      .drop(orderApiDF1.col("N1"))

    finalDF
  }

  /**
    * Step 6
    * This function is important as it creates two tables that are used for merge operation later
    * viz OrdersAPIPrices and Orders
    * these are referred to in Stored Proc as [AM_Import].[orders].[Orders]
    * The dataframes that are returned are ordersAPIPricesMainDF, orderWithStatusDF respectively
    */
  def getPricesAndOrderStatus(ordersAPIPricesDF: DataFrame): (DataFrame, DataFrame, DataFrame) = {

    // Need to test this date conversion
    val partitionedDF1 = DataFrameUtil.partitionDataFrame(ordersAPIPricesDF, "ParentOrderID,OrderID", "PriceStartDate Desc", "OrderPriceRank", 1)
    val dateUDF = udf(extractDateUDF)
    val partitionedDFNew = partitionedDF1.withColumn("PriceStartDay", dateUDF(col("PriceStartDate")))

    val ordersAPIPricesMainDF = DataFrameUtil.partitionDataFrame(partitionedDFNew, "OrderID,PriceStartDay", "PriceStartDate Desc", "OrderPricePerDayRank", 1)

    val cosmosOrdersOrdersDF = DataFrameUtil.readDataFrameFromBlobAsTSV(configReader, "orderDim.ingest.tables.cosmosOrdersOrders", "cosmosOrdersOrders", inputArgsMap,  spark)

    val tDF = ordersAPIPricesMainDF.as("a").join(cosmosOrdersOrdersDF.as("b"), col("a.OrderID") === col("b.Id"), "left")
      .withColumn("SubscriptionId", when(cosmosOrdersOrdersDF.col("Payer_Type") === "AzureSubscriptionId", cosmosOrdersOrdersDF.col("Payer_Value"))
        .otherwise(lit(null).cast(StringType)))

    val groupByForTempOrders = configReader.getValueForKey("orderDim.ingest.tables.tempOrders.groupBy").toString

    val orderDF = DataFrameUtil.groupByDataFrame(configReader, tDF, groupByForTempOrders)
      .agg(min(col("PriceStartDate")).as("PurchaseDate"), max(col("PriceEndDate")).as("CancelledDate"))

    val columnsList = orderDF.columns.map(x => "a." + x) :+ "t.CustomerProvidedOrderName"

    val orderWithStatusDF = orderDF.as("a").join(tDF.as("t"), Seq("OrderId")).select("t.EventEndName", columnsList: _*).withColumnRenamed("EventEndName", "Status")

    val admOrderStatusDF = ordersAPIPricesMainDF.select(
      ordersAPIPricesMainDF.col("OrderID"),
      ordersAPIPricesMainDF.col("ParentOrderID").as("OrderConvertedFrom"),
      ordersAPIPricesMainDF.col("EventEndName").as("OrderStatus"),
      ordersAPIPricesMainDF.col("PriceEndDate"),
      ordersAPIPricesMainDF.col("OrderPriceRank")
    ).where(ordersAPIPricesMainDF.col("OrderPriceRank") === 1 && ordersAPIPricesMainDF.col("OrderID").isNotNull)

    val tempDF = ordersAPIPricesMainDF.select("OrderId", "DiscountType", "OrderPriceRank").where(col("DiscountType").isin(Seq("Trial", "Promotion"): _*))
    val ordersApiSelectedColDF = tempDF.withColumnRenamed("OrderPriceRank", "LastTrialRank")

    val currentOrderStatusDF = admOrderStatusDF.join(ordersApiSelectedColDF, Seq("OrderID"), "left")
      .select(
        col("OrderId"),
        col("OrderStatus"),
        col("PriceEndDate").as("TrialEndDate"), // TBD - No EndDate, so using PriceEndDate.
        col("OrderConvertedFrom"),
        col("DiscountType"),
        col("LastTrialRank")
      ).where(col("LastTrialRank") === 1)

    (ordersAPIPricesMainDF, orderWithStatusDF, currentOrderStatusDF)

  }

  /**
    * Step 7
    */
  def getLatestOrderPricesAndDataMarketSubIDDF(orderApiPricesDF: DataFrame): (DataFrame, DataFrame) = {

    val latestOrderPriceDF = orderApiPricesDF.select("OrderID", "EventEndName", "CurrencyCode", "Price", "OrderPriceRank")
      .where(orderApiPricesDF.col("OrderPriceRank") === 1)
      .groupBy("OrderID", "EventEndName", "CurrencyCode").agg(sum("Price").as("Price"))

    val dataMarketSubIDDF = orderApiPricesDF.select(
      orderApiPricesDF.col("OrderID"),
      orderApiPricesDF.col("ExternalBillingID").as("DataMarketSubscriptionID"),
      orderApiPricesDF.col("BillingSystemName")
    ).where(orderApiPricesDF.col("BillingSystemName") === "DALLAS")

    (latestOrderPriceDF, dataMarketSubIDDF)
  }

  def generateFinalDataFrameForMergeAndUpdate(ordersTable: DataFrame,
                                              publisherDimDF: DataFrame,
                                              offerDimDF: DataFrame,
                                              servicePlanNameOverridesDF: DataFrame,
                                              distinctOffersLangHistoricalDF: DataFrame,
                                              distinctSPByMarketHistoricalDF: DataFrame,
                                              latestOrderPriceDF: DataFrame,
                                              ordersAPIPricesMainDF: DataFrame,
                                              odataOfferOnboardingsSPDF: DataFrame, dataMarketSubIDDF: DataFrame): DataFrame = {


    var trialandPromoordersDF = ordersAPIPricesMainDF.
      select(
        col("ParentOrderId").as("ParentOrderId"),
        col("EventEndName").as("Event1"),
        col("PriceEndDate").as("EndDate1"),
        col("DiscountType").as("DiscountType1"),
        col("OrderID").as("OrderID1"), col("OrderPriceRank").as("Rank1")
      ).where(col("DiscountType").isin("Trial", "Promotion")).distinct()

    trialandPromoordersDF = DataFrameUtil.partitionDataFrame(trialandPromoordersDF, "OrderID1", "Event1 Desc,EndDate1 Desc", "LastTrialRank", 1)

    val latestOrderPriceDF1 = latestOrderPriceDF.select(
      col("EventEndName").as("EventEndName1"),
      col("CurrencyCode").as("CurrencyCode1"),
      col("OrderID"),
      col("Price")
    )

    val offerDimDF1 = offerDimDF.select(
      offerDimDF.col("OfferName").as("OfferName1"),
      offerDimDF.col("ServicePlanName").as("ServicePlanName1"),
      offerDimDF.col("OfferId").as("OfferId1"),
      offerDimDF.col("IsPreview").as("IsPreview1"),
      offerDimDF.col("OfferType").as("OrderType"),
      offerDimDF.col("SP_RequiresExtLicense").as("SP_RequiresExtLicense1"),
      offerDimDF.col("IsExternallyBilled").as("IsExternallyBilled1"))

    val ordersDF = ordersTable.withColumn("PublisherNaturalIdentifier", when(col("PublisherNaturalIdentifier").isNull, "").otherwise(col("PublisherNaturalIdentifier")))

    var joinedDF = ordersDF.as("o").join(publisherDimDF.as("pd"), ordersDF.col("PublisherNaturalIdentifier") === publisherDimDF.col("PublisherName"), "left")

      .join(offerDimDF.as("od"), offerDimDF.col("OfferName") === ordersDF.col("OfferNaturalIdentifierSubStr") &&
        offerDimDF.col("ServicePlanName") === ordersDF.col("PlanNaturalIdentifier")
        && offerDimDF.col("PublisherName") === publisherDimDF.col("PublisherName"), "left") // Join using PublisherID

      .join(offerDimDF1.as("od1"), offerDimDF1.col("OfferName1") === ordersDF.col("OfferNaturalIdentifierSubStr") &&
      offerDimDF1.col("ServicePlanName1") === ordersDF.col("PlanNaturalIdentifier") &&
      offerDimDF.col("OfferName").isNull, "left")

      .join(servicePlanNameOverridesDF.as("spo"), Seq("PublisherNaturalIdentifier", "OfferNaturalIdentifier", "PlanNaturalIdentifier"), "left")

      .join(odataOfferOnboardingsSPDF.as("sp"), offerDimDF.col("OfferId") === odataOfferOnboardingsSPDF.col("OfferDraftId") &&
        coalesce(servicePlanNameOverridesDF.col("PlanOverrideValue"), ordersDF.col("PlanNaturalIdentifier")) === odataOfferOnboardingsSPDF.col("ServicePlanNaturalIdentifier") &&
        odataOfferOnboardingsSPDF.col("ServicePlanRank") === 1)

      .join(distinctOffersLangHistoricalDF.as("aol"),
        when(servicePlanNameOverridesDF.col("PlanOverrideValue").isNull, ordersDF.col("PlanNaturalIdentifier"))
          .otherwise(servicePlanNameOverridesDF.col("PlanOverrideValue")) === distinctOffersLangHistoricalDF.col("SP_ServicePlanNID")
          && offerDimDF.col("OfferId") == distinctOffersLangHistoricalDF.col("OfferId")
          && distinctOffersLangHistoricalDF.col("VersionOrder") === 1, "left")

      .join(distinctSPByMarketHistoricalDF.as("pricing"),
        when(servicePlanNameOverridesDF.col("PlanOverrideValue").isNull, ordersDF.col("PlanNaturalIdentifier"))
          .otherwise(servicePlanNameOverridesDF.col("PlanOverrideValue")) === distinctSPByMarketHistoricalDF.col("ServicePlanNID")
          && coalesce(offerDimDF.col("OfferId"), offerDimDF1.col("OfferId1")) === distinctSPByMarketHistoricalDF.col("OfferId"), "left")

      .join(trialandPromoordersDF, ordersDF.col("OrderId") === trialandPromoordersDF.col("OrderID1")
        && trialandPromoordersDF.col("LastTrialRank") === 1,
        "left")

      .join(latestOrderPriceDF1.as("lop"), Seq("OrderID"), "left")

      .select(
        lit("").cast(StringType).as("VMId"),
        lit("").cast(StringType).as("DeploymentId"),
        ordersDF.col("OrderId").as("OrderId"),
        ordersDF.col("CustomerProvidedOrderName").as("CustomerProvidedOrderName"), // TBD
        coalesce(col("pd.PublisherName"), when(ltrim(rtrim(ordersDF.col("PublisherNaturalIdentifier"))).isNotNull, ltrim(rtrim(ordersDF.col("PublisherNaturalIdentifier"))))
          .otherwise("")).as("PublisherName"),
        coalesce(offerDimDF.col("OfferName"), offerDimDF1.col("OfferName1"), ordersDF.col("OfferNaturalIdentifier")).as("OfferName"),
        coalesce(offerDimDF.col("ServicePlanName"), offerDimDF1.col("ServicePlanName1"), ordersDF.col("PlanNaturalIdentifier")).as("ServicePlanName"),
        ordersDF.col("SubscriptionId").as("AzureSubscriptionId"), // TBD
        ordersDF.col("PurchaseDate").as("PurchaseDate"),
        ordersDF.col("CancelledDate").as("CancelDate"),
        coalesce(offerDimDF.col("IsPreview"), offerDimDF1.col("IsPreview1")).as("IsPreview"),
        latestOrderPriceDF1.col("EventEndName1").as("CurrentStatus"), //EventEndName has been replaced as OrderStatus getPricesAndOrderStatus
        lit("").cast(StringType).as("CustomerRegion"),
        latestOrderPriceDF1.col("CurrencyCode1").as("CurrencyCode"),
        lit("").cast(StringType).as("OrderStatusFromSO"),
        lit("").cast(StringType).as("ScheduledCancellationDateFromSO"),
        lit(0).cast(IntegerType).as("IsMultiSolution"),
        when(trialandPromoordersDF.col("DiscountType1") === "Trial", trialandPromoordersDF.col("EndDate1")).otherwise(lit(-1).cast(LongType)).as("TrialEndDate"), // TBD Using PriceEndDate/TrialEndDate for EndDate
        when(trialandPromoordersDF.col("DiscountType1") === "Trial", 1).otherwise(0).as("isTrial"),
        ordersDF.col("PublisherNaturalIdentifier"),
        ordersDF.col("ParentOrderId"),
        offerDimDF1.col("OrderType"),
        offerDimDF.col("rowKey").as("OfferRowKey"),
        when( ((distinctOffersLangHistoricalDF.col("SP_RequiresExtLicense").isNull &&  odataOfferOnboardingsSPDF.col("IsExternallyBilled") === true) ||
          (distinctOffersLangHistoricalDF.col("SP_RequiresExtLicense") === true)), lit("Free"))
            .when(latestOrderPriceDF1.col("Price") === 0, "Free")
            .when(latestOrderPriceDF1.col("Price") > 0, "Paid")
            .when(((distinctOffersLangHistoricalDF.col("SP_RequiresExtLicense") === false) ||
              (distinctOffersLangHistoricalDF.col("SP_RequiresExtLicense").isNull &&  coalesce(odataOfferOnboardingsSPDF.col("IsExternallyBilled"), lit("True")) =!= "True")) &&
              (distinctSPByMarketHistoricalDF.col("PP_Amount") === 0), "Free")
            .when(((distinctOffersLangHistoricalDF.col("SP_RequiresExtLicense") =!= true) ||
              (distinctOffersLangHistoricalDF.col("SP_RequiresExtLicense").isNull &&  coalesce(odataOfferOnboardingsSPDF.col("IsExternallyBilled"), lit("True")) =!= "True")) &&
              (distinctSPByMarketHistoricalDF.col("PP_Amount") > 0), "Paid")
          .otherwise("Unknown").as("ServicePlanBillingType"),
        when((distinctOffersLangHistoricalDF.col("SP_RequiresExtLicense").isNull &&  odataOfferOnboardingsSPDF.col("IsExternallyBilled") === "True"), lit("BYOL"))
          .when(((distinctOffersLangHistoricalDF.col("SP_RequiresExtLicense") === false) ||
            (distinctOffersLangHistoricalDF.col("SP_RequiresExtLicense").isNull &&  coalesce(odataOfferOnboardingsSPDF.col("IsExternallyBilled"), lit("False")) =!= "False")), "Billed through Azure")
          .otherwise("Unknown").as("ServicePlanPaymentType")
      ).where(col("PublisherNaturalIdentifier") =!= "DataMarket" && col("OrderId").isNotNull)

    joinedDF = joinedDF.withColumn("DataSource", lit("EventStore").cast("string"))
    joinedDF = joinedDF.withColumn("Location", lit(""))
    joinedDF = joinedDF.withColumn("CloudGeo", lit(""))
    joinedDF = joinedDF.withColumn("HasUsedGovCloud", lit(""))
    joinedDF = joinedDF.withColumn("IsFairfax", lit(""))

    val dateInStrDF = DataFrameUtil.convertDateColumnsToString(joinedDF, Seq("CancelDate", "PurchaseDate", "ScheduledCancellationDateFromSO", "TrialEndDate"))

    val joinedDFWithKey = dateInStrDF.withColumn("rowKey", dateInStrDF.col("OrderId"))

    DataFrameUtil.fillDefaultValuesForNullColumns(joinedDFWithKey)

  }

  def processAPIPrices(df : DataFrame) : DataFrame = {

    val datesInStrDF  = DataFrameUtil.convertDateColumnsToString(df, Seq("PriceStartDate", "PriceEndDate"))
    val dfWithKey = datesInStrDF.withColumn("rowKey", concat_ws("_", when(datesInStrDF.col("OrderId").isNotNull, datesInStrDF.col("OrderId")).otherwise(""),
      when(datesInStrDF.col("PriceStartDateLong").isNotNull, datesInStrDF.col("PriceStartDateLong")).otherwise(""),
      when(datesInStrDF.col("PriceEndDateLong").isNotNull, datesInStrDF.col("PriceEndDateLong")).otherwise("")))

    dfWithKey.dropDuplicates("rowKey")

  }

  def verifyTimeUDF = (time: Long) => {
    val dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val dateStored = new Date(time)
    val dateStr = dateFormatter.format(dateStored)
    val dateParts = dateStr.split(" ")
    if (dateParts(1).equalsIgnoreCase("23:59:59")) true else false
  }

  def extractDateUDF = (time: Long) => {
    val dateFormatter = new SimpleDateFormat("yyyy-MM-dd")
    val dateStored = new Date(time)
    val dateOnlyStr = dateFormatter.format(dateStored)
    val dateOnly = dateFormatter.parse(dateOnlyStr)
    dateOnly.getTime
  }

  /**
    * This function generates the subscription ids from the order dimension data
    * @param orderDF
    * @return
    */
  def getSubscriptionsFromOrders(orderDF : DataFrame): DataFrame = {
    val subscriptionDF = orderDF.select("AzureSubscriptionId")
    val subscriptionPath = inputArgsMap.get("subscriptionPath").get
    subscriptionDF.write.mode(SaveMode.Overwrite).parquet(subscriptionPath)
    subscriptionDF
  }

  /***
    * 		select distinct AOM.AlternateId,COE.Billing_systemName , COE.EffectiveDate
		into #OrderBillingSystemDetails
		from AM_Import.[dbo].[Cosmos_Orders_AlternateIdToOrderIdMappings] AOM(nolock)
		left join aM_import.[dbo].[Cosmos_Orders_ExternalBillingIds] COE(nolock)
			on cast(AOM.ParentOrderId as nvarchar(36)) = cast(COE.OrderId as nvarchar(36))
		where COE.EffectiveDate IS NOT NULL

		;with easubscriptionchannel as
		(
			select
				   asd.mocpsubscriptionid as azuresubscriptionid,
				   asd.AMDMSubscriptionId,
				   ed.Channel,
				   ed.CountryName as EnrollmentCountryName,
				   CAST(ed.StartEffectiveDate as date) as eastarteffectivedate,
				   CAST(ed.EndEffectiveDate as date) as eaendeffectivedate,
				   CAST(ed.AmendmentStartDate as date) as eaamendmentstartdate,
				   CAST(ed.AmendmentEndDate as date) as eaamendmentenddate,
				   CAST(ead.ERAStartsOn as date) as erastartsondate,
				   CAST(ead.ERAEndsOn as date) as eraendsondate,
				   CAST(asd.ASStartsOn as date) as asstartsondate,
				   CAST(asd.ASEndsOn as date) as asendsondate
			from amdm.AccountSubscriptionDim asd (nolock)
				 inner join amdm.EnrollmentAccountDim ead (nolock)
					on ead.aivaccountid = asd.aivaccountid
				 inner join amdm.EnrollmentDim ed (nolock)
					on ed.aivenrollmentid = ead.aivenrollmentid
		)

		UPDATE o
			SET
				AzureLicenseType =  CASE
										WHEN sd.IsCSP = 1 THEN 'CSP'
										--WHEN b.EAUsageID IS NOT NULL THEN easc2.Channel  --6/7 James -Added to check how it was being billed before running second check
										WHEN sd.Azuresubscriptionid IS NOT NULL
										THEN
											CASE
											WHEN isnull(T.Billing_SystemName,'') <> 'ENTERPRISEAGREEMENT' and easc.channel is not null then easc.channel
											WHEN T.Billing_SystemName = 'ENTERPRISEAGREEMENT' and easc.Channel IS NOT NULL THEN easc.Channel
											WHEN T.Billing_SystemName = 'ENTERPRISEAGREEMENT' and easc.channel is null then 'Direct Enterprise'
											WHEN sd.offertype = 'EA' THEN 'Direct Enterprise'
											ELSE 'Direct - PAYG'
											END
										ELSE 'Direct - PAYG'
									END
				--,AzureSubscriptionCountryName = IIF(easc.EnrollmentCountryName IS NOT NULL, easc.EnrollmentCountryName, sd.CountryName)
		from amdm.orderdim o (nolock)
			--LEFT JOIN amdm.OrderBilledRevenueFact b ON o.AMDMOrderiD = b.AMDMOrderID
			--LEFT JOIN amdm.OrderChargeFact c ON o.AMDMOrderiD = c.AMDMOrderID

		Left join #OrderBillingSystemDetails T on o.orderid = T.alternateId
				AND cast(o.PurchaseDate as date) >= ISNULL(T.EffectiveDate,'1900-01-01')
				and cast(o.PurchaseDate as date) <= ISNULL(T.EffectiveDate,'2999-12-31')
			left join amdm.SubscriptionDim sd (nolock)
				/* Commented by Girish on 03/06: We shouldn't restrict subscriptions based on purchase date since SubscriptionDim is Type 1 dimension.
				on sd.amdmsubscriptionid = o.amdmsubscriptionid
				and (o.PurchaseDate >= sd.CreatedDate or o.PurchaseDate >= sd.CreatedDate)
				and (o.PurchaseDate <= sd.EndDate)
    */
				on sd.azuresubscriptionid = o.azuresubscriptionid
			left join easubscriptionchannel easc  (nolock)
				on easc.azuresubscriptionid = o.azuresubscriptionid
					/* Commented by Girish on 03/06: Used AzureSubscriptionId for better lookup.
					easc.amdmsubscriptionid = o.amdmsubscriptionid*/
					and cast(o.PurchaseDate as date) >= ISNULL(easc.EAStartEffectiveDate,'1900-01-01')
					and cast(o.PurchaseDate as date) <= ISNULL(easc.EAEndEffectiveDate,'2999-12-31')
					and cast(o.PurchaseDate as date) >= ISNULL(easc.EAAmendmentStartDate,'1900-01-01')
					and cast(o.PurchaseDate as date) <= ISNULL(easc.EAAmendmentEndDate,'2999-12-31')
					and cast(o.PurchaseDate as date) >= ISNULL(easc.erastartsondate,'1900-01-01')
					and cast(o.PurchaseDate as date) <= ISNULL(easc.eraendsondate,'2999-12-31')
					and cast(o.PurchaseDate as date) >= ISNULL(easc.asstartsondate,'1900-01-01')
					and cast(o.PurchaseDate as date) <= ISNULL(easc.asendsondate,'2999-12-31')

    * @param cosmosOrdersAlternateIdToOrderIdDF
    * @param cosmosOrdersExternalBillingIdsDF
    * @return
    */
  def processAzureLicenseType(cosmosOrdersAlternateIdToOrderIdDF:DataFrame,
                              cosmosOrdersExternalBillingIdsDF:DataFrame,
                              orderDimDF: DataFrame) : DataFrame = {


    val acntSubDF = DataFrameUtil.readDataFrameFromBlobAsTSV(configReader, "orderDim.ingest.tables.accountSubscription", "accountSubscription", inputArgsMap, spark)
    val accountSubscriptionDF = acntSubDF
      .withColumn("StartsOn", when(col("StartsOn") === -1, LICENSE_TYPE_MIN_TIME).otherwise(col("StartsOn")))
      .withColumn("EndsOn", when(col("EndsOn") === -1, LICENSE_TYPE_MAX_TIME).otherwise(col("EndsOn")))

    val enrollmentDimDF = DataFrameUtil.readDataFrameFromBlob(configReader, "orderDim.ingest.tables", "enrollmentDim", inputArgsMap, spark)

    val enAcntDF= DataFrameUtil.readDataFrameFromBlobAsTSV(configReader, "orderDim.ingest.tables.enrollmentAccount", "enrollmentAccount", inputArgsMap, spark)
    val enrollmentAccountDF = enAcntDF
      .withColumn("StartsOn", when(col("StartsOn") === -1, LICENSE_TYPE_MIN_TIME).otherwise(col("StartsOn")))
      .withColumn("EndsOn", when(col("EndsOn") === -1, LICENSE_TYPE_MAX_TIME).otherwise(col("EndsOn")))

    val subscriptionDF = DataFrameUtil.readDataFrameFromBlobAsTSV(configReader, "orderDim.ingest.tables.subscriptions", "subscriptions", inputArgsMap, spark)

    val orderBillingSystemDetailsDF = cosmosOrdersAlternateIdToOrderIdDF.as("a").join(cosmosOrdersExternalBillingIdsDF.as("b"),
      cosmosOrdersAlternateIdToOrderIdDF.col("OrderId") === cosmosOrdersExternalBillingIdsDF.col("OrderId"), "left")
      .where(cosmosOrdersExternalBillingIdsDF.col("EffectiveDate").isNotNull)
      .select(
        cosmosOrdersAlternateIdToOrderIdDF.col("AlternateId"),
        cosmosOrdersExternalBillingIdsDF.col("Billing_SystemName"),
        cosmosOrdersExternalBillingIdsDF.col("EffectiveDate")
      )

    val subscriptionChannelDF = accountSubscriptionDF
      .join(enrollmentAccountDF,
        Seq("AccountId"), "inner")
      .join(subscriptionDF, accountSubscriptionDF.col("SubscriptionId") === subscriptionDF.col("Id"), "inner")
      .join(enrollmentDimDF, enrollmentDimDF.col("EnrollmentId") === enrollmentAccountDF.col("EnrollmentId"))
      .select(
        subscriptionDF.col("MOCPSubscriptionGuid").as("AzureSubscriptionId"),
        enrollmentDimDF.col("Channel"),
        enrollmentDimDF.col("CountryName").as("EnrollmentCountryName"),
        enrollmentDimDF.col("StartEffectiveDateLong").as("EAStartEffectiveDate"),
        enrollmentDimDF.col("EndEffectiveDateLong").as("EAEndEffectiveDate"),
        enrollmentDimDF.col("AmendmentStartDateLong").as("EAAmendmentStartDate"),
        enrollmentDimDF.col("AmendmentEndDateLong").as("EAAmendmentEndDate"),
        enrollmentAccountDF.col("StartsOn").as("ERAStartsOnDate"),
        enrollmentAccountDF.col("EndsOn").as("ERAEndsOnDate"),
        accountSubscriptionDF.col("StartsOn").as("ASStartsOnDate"),
        accountSubscriptionDF.col("EndsOn").as("ASEndsOnDate")
      )

    val finalDF = orderDimDF
      .join(orderBillingSystemDetailsDF,
        orderDimDF.col("orderId") === orderBillingSystemDetailsDF.col("alternateId") &&
          orderDimDF.col("PurchaseDate") >= coalesce(orderBillingSystemDetailsDF.col("EffectiveDate"), lit(LICENSE_TYPE_MIN_TIME)) &&
          orderDimDF.col("PurchaseDate") <= coalesce(orderBillingSystemDetailsDF.col("EffectiveDate"), lit(LICENSE_TYPE_MAX_TIME)), "left")
      .join(subscriptionDF, subscriptionDF.col("MOCPSubscriptionGuid") === orderDimDF.col("AzureSubscriptionId"), "left")
      .join(subscriptionChannelDF, orderDimDF.col("AzureSubscriptionId") === subscriptionChannelDF.col("AzureSubscriptionId") &&
        (orderDimDF.col("PurchaseDate") >= coalesce(subscriptionChannelDF.col("EAStartEffectiveDate"), lit(LICENSE_TYPE_MIN_TIME))) &&
        (orderDimDF.col("PurchaseDate") <= coalesce(subscriptionChannelDF.col("EAEndEffectiveDate"), lit(LICENSE_TYPE_MAX_TIME))) &&
        (orderDimDF.col("PurchaseDate") >= coalesce(subscriptionChannelDF.col("EAAmendmentStartDate"), lit(LICENSE_TYPE_MIN_TIME))) &&
        (orderDimDF.col("PurchaseDate") <= coalesce(subscriptionChannelDF.col("EAAmendmentEndDate"), lit(LICENSE_TYPE_MAX_TIME))) &&
        (orderDimDF.col("PurchaseDate") >= coalesce(subscriptionChannelDF.col("ERAStartsOnDate"), lit(LICENSE_TYPE_MIN_TIME))) &&
        (orderDimDF.col("PurchaseDate") <= coalesce(subscriptionChannelDF.col("ERAEndsOnDate"), lit(LICENSE_TYPE_MAX_TIME))) &&
        (orderDimDF.col("PurchaseDate") >= coalesce(subscriptionChannelDF.col("ASStartsOnDate"), lit(LICENSE_TYPE_MIN_TIME))) &&
        (orderDimDF.col("PurchaseDate") <= coalesce(subscriptionChannelDF.col("ASEndsOnDate"), lit(LICENSE_TYPE_MAX_TIME))), "left")
      .select(
        (
          //          when(subscriptionDF.col("IsCSP") === lit(1), "CSP").
          when(subscriptionDF.col("MOCPSubscriptionGuid").isNotNull,
            (
              when( coalesce(orderBillingSystemDetailsDF.col("Billing_SystemName"), lit("")) =!= "ENTERPRISEAGREEMENT"
                && subscriptionChannelDF.col("Channel").isNotNull, subscriptionChannelDF.col("Channel"))
                .when(coalesce(orderBillingSystemDetailsDF.col("Billing_SystemName"), lit("")) =!= "ENTERPRISEAGREEMENT"
                  && subscriptionChannelDF.col("Channel").isNotNull, subscriptionChannelDF.col("Channel"))
                .when(coalesce(orderBillingSystemDetailsDF.col("Billing_SystemName"), lit("")) =!= "ENTERPRISEAGREEMENT"
                  && subscriptionChannelDF.col("Channel").isNull, "Direct Enterprise")
                //              .when(subscriptionDF.col("offerType") === "EA", "Direct Enterprise")
                .otherwise("Direct - PAYG")
              ))
            .otherwise("Direct - PAYG")
          ).as("AzureLicenseType"),
        orderDimDF.col("rowKey"),
        orderDimDF.col("OrderId")
      )

    orderDimDF.join(finalDF, Seq("rowKey", "OrderId"))
  }


  def processDFForResourceURI(orderDimDF : DataFrame) : DataFrame = {

    val mergedFilePathPrevious = VNextUtil.extractPreviousPath(inputArgsMap, "mergedPath")

    val oneTimeStore = inputArgsMap.get("oneTimeStoreAPI")

    var storeAPIDF : DataFrame = null

    if (mergedFilePathPrevious == null || mergedFilePathPrevious == None ) {

      storeAPIDF = DataFrameUtil.readDataFrameFromBlob(configReader, "orderDim.ingest.tables.offerDim", "oneTimeStoreAPI", inputArgsMap, spark)

    } else {

      storeAPIDF = DataFrameUtil.readDataFrameFromBlob(configReader, "orderDim.ingest.tables.offerDim", "storeAPI", inputArgsMap, spark)

    }

    val finalDF = orderDimDF.join(storeAPIDF, Seq("OrderID")).
      select(
        storeAPIDF.col("resourceUrl").as("ResourceURI"),
        md5(lower(storeAPIDF.col("resourceUrl"))).as("ResourceURIHash"),
        orderDimDF.col("rowKey")
      )

    orderDimDF.join(finalDF, Seq("rowKey"))
  }

  def readMultiSolutionTable(tableName : String ) : DataFrame = {

    val df = DataFrameUtil.readDataFrameFromBlobAsParquet(configReader, "ordersDim.ingest.tables.multiSolution","multiSolution", inputArgsMap,  spark)

    var multiTempDF = DataFrameUtil.readDataFrameFromBlobAsTSV(configReader, "ordersDim.ingest.tables.multiSolutionTemp","multiSolutionTemp", inputArgsMap,  spark)

    //multiTempDF= multiTempDF.withColumn("OfferName", parsePublisherOfferFromTemplateUri(multiTempDF.col("TemplateUri"),"O"))
    //multiTempDF= multiTempDF.withColumn("PublisherName", parsePublisherOfferFromTemplateUri(multiTempDF.col("TemplateUri"),"P"))

    multiTempDF= multiTempDF.withColumn("TemplateUriHash", md5(lower(multiTempDF.col("TemplateUri"))))

    var selectCols = Seq(df.col("ResourceURI").as("ResourceURI"),
      md5(lower(df.col("ResourceURI"))).as("ResourceURIHash")
    )

    var finalDF =  df.select(selectCols: _*).where(col("OrderNumber").isNotNull)

    finalDF =finalDF.withColumn("OrderId", finalDF.col("ResourceURIHash"))

    finalDF =finalDF.withColumn("PurchaseDate", min(finalDF.col("StartDateTime")))

    finalDF = finalDF.select("ResourceURI","ResourceURIHash", "OrderId", "PurchaseDate")

    val nullCols = Seq("VMId","DeploymentId", "CustomerProvidedOrderName", "PublisherName", "OfferName"
      , "ServicePlanName", "AzureSubscriptionId", "PurchaseDate", "CancelDate",
      "IsPreview", "CurrentStatus"  , "CustomerRegion", "CurrencyCode",
      "OrderStatusFromSO", "ScheduledCancellationDateFromSO","TrialEndDate",
      "isTrial", "PublisherNaturalIdentifier","ParentOrderId","ServicePlanBillingType","ServicePlanPaymentType","Location","CloudGeo","HasUsedGovCloud","IsFairfax","OfferRowKey"
    )

    nullCols.foreach(col => {
      finalDF = finalDF.withColumn(col, lit(""))
    })

    finalDF = finalDF.withColumn("DataSource", lit("Cosmos_UsageDaily_MultiSolution").cast("string"))
    finalDF = finalDF.withColumn("IsMultisolution", lit(1))
    finalDF = finalDF.withColumn("OrderType", lit("MultiSolution").cast("string"))

    finalDF = DataFrameUtil.convertDateColumnsToString(finalDF, Seq("CancelDate", "PurchaseDate", "ScheduledCancellationDateFromSO", "TrialEndDate"))
    finalDF  = finalDF.withColumn("rowKey", finalDF.col("OrderId"))



   // val colNames = finalDF.columns.map(x => finalDF.col(x)).toList

   // val selectCols = colNames ::: Seq(multiTempDF.col("PublisherName"),multiTempDF.col("OfferName")).toList

    finalDF = finalDF.join(multiTempDF, finalDF.col("ResourceURIHash") === multiTempDF.col("TemplateUriHash"),"left")
    finalDF
  }
  def parsePublisherOfferFromTemplateUri(templateUri : String, mode : String) : String = {

    var templateUriClean = templateUri.replace("https://gallery.azure.com/artifact/","")
    templateUriClean = templateUriClean.replace("/Artifacts/maintemplate.json","")

    templateUriClean = templateUriClean.split("/")(1)

    //templateUriClean = templateUriClean.split(".")

    if(mode == "P") {
     // templateUriClean = templateUriClean(0)
    }
    else {
     // templateUriClean = templateUriClean(1)
    }
    return templateUriClean
  }
}