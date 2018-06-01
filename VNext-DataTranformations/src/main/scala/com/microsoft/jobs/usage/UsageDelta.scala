package com.microsoft.jobs.usage

import java.util.Properties

import com.microsoft.common.DataFrameUtil
import com.microsoft.config.ConfigReader
import com.microsoft.framework.VNextUtil
import com.microsoft.jobs.order.ProcessOrders._
import org.apache.spark.sql.{DataFrame, RelationalGroupedDataset, SparkSession}
import org.apache.spark.sql.functions.{when, _}
import org.apache.spark.sql.types._

object  UsageDelta {

  var spark: SparkSession = null
  val configReader = new ConfigReader("orderUsageDelta.conf")
  var inputArgsMap: Map[String, String] = null

  /**
    * This function reads the configuration from
    */
  def setup() : Unit = {


    spark = SparkSession.builder
      .getOrCreate()

  }

  def main(args: Array[String]): Unit = {

    inputArgsMap = VNextUtil.getInputParametersAsKeyValue(args)
    setup()

    val ordersDF = DataFrameUtil.readDataFrameFromBlob(configReader, "ordersUsage.ingest.tables.orderDim", "orderDim", inputArgsMap, spark)
    val cosmosMarketplaceCoreComputeUsageDF = DataFrameUtil.readDataFrameFromBlob(configReader, "ordersUsage.ingest.tables.cosmosMarketplaceCoreComputeUsage", "cosmosMarketplaceCoreComputeUsage",  inputArgsMap,  spark)
    val cosmosMarketplaceU_DF = cosmosMarketplaceCoreComputeUsageDF.withColumn("UsageDateTime", concat(unix_timestamp(col("StartDateTime"), "YYYYMMDD"), lit(" 23:59:59.99")))
    val vmUsageOrderNumberMappingsDF = DataFrameUtil.readDataFrameFromBlobAsTSV(configReader, "ordersUsage.ingest.tables.exceptionVmUsageOrderNumberMappings", "exceptionVmUsageOrderNumberMappings",  inputArgsMap,  spark)

    val orderApiPricesDF = DataFrameUtil.readDataFrameFromBlob(configReader, "ordersUsage.ingest.tables.ordersAPIPrices", "ordersAPIPrices", inputArgsMap,  spark)

    val orderAPIOrderIdsDF = orderApiPricesDF.select(col("OrderId")).distinct()
    val t1DF = processOrderApiPrices(orderApiPricesDF)

    val nestedJoinedDF = joinTables(ordersDF, cosmosMarketplaceU_DF, vmUsageOrderNumberMappingsDF, orderApiPricesDF, orderAPIOrderIdsDF)
    val tDF = processNestedJoins(nestedJoinedDF)
    val orderUsageFactDF = finalJoin(tDF, t1DF)

    orderUsageFactDF.show()

  }


  def processOrderApiPrices(orderApiPricesDF: DataFrame ): DataFrame = {
    val cols = orderApiPricesDF.columns.map( x => orderApiPricesDF.col(x)).toList

    val nestedOrderApiPricesDF  = orderApiPricesDF.select(cols: _*)
      .withColumn("Price_StartDate", col("PriceStartDate")).withColumn("Price_EndDate", col("PriceEndDate") + lit(86399) )
      .where(
      col("PriceStartDate") -
        when(col("PriceEndDate") === VNextUtil.TIMESTAMP_MAX_VAL, lit(System.currentTimeMillis()).cast(LongType))
        .otherwise(col("PriceEndDate"))  > 1)

    val partitionedFirstDF = DataFrameUtil.partitionDataFrame(configReader, "ordersUsage.ingest.tables.ordersAPIPrices", nestedOrderApiPricesDF, 0)

    val partitionedSecondDF = DataFrameUtil.partitionDataFrame(configReader, "ordersUsage.ingest.tables.ordersAPIPrices1", partitionedFirstDF, 0)

    val t1DF = partitionedSecondDF.where(col("RN1") === 1)

    t1DF

  }

  def joinTables( ordersDF : DataFrame, cosmosMarketplaceDF : DataFrame, vmUsageOrderNumberMappingsDF : DataFrame,
                  orderApiPricesDF: DataFrame, orderAPIOrderIdsDF : DataFrame) : DataFrame = {

    val getStringBetween = udf(getSubStringBetween)
    val joinedDF = cosmosMarketplaceDF.as("u").join(vmUsageOrderNumberMappingsDF.as("m"), col("u.OrderNumber") === col("m.OrderNumber") , "left")
      .join(ordersDF.as("od"), col("u.OrderNumber") === col("od.OrderId") , "left")
      .join(orderApiPricesDF.as("oap"), col("od.OrderId") === col("oap.OrderId") &&
        col("u.StartDateTime").between(col("oap.PriceStartDate"), col("oap.PriceEndDate")) &&
        col("u.MeteredResourceGuid") === col("oap.MeterName"), "left")
      .join(orderApiPricesDF.as("oap1"), col("od.OrderId") === col("oap1.OrderId") &&
        col("u.UsageDateTime").between(col("oap1.PriceStartDate"), col("oap1.PriceEndDate")) &&
        col("u.MeteredResourceGuid") === col("oap1.MeterName"), "left")
      .join(orderAPIOrderIdsDF.as("oapi"), col("od.OrderId") === col("oapi.OrderId") , "left")
      .where(col("u.OrderNumber").isNotNull && col("u.PartNumber").isNotNull && col("u.StartDateTime") > "2018-04-02" && col("u.MeteredResourceGUID").like("%CORE%"))
          .select(
            col("u.OrderNumber").as("OrderId"),
            col("od.AMDMOrderId").as("AMDMOrderId"),
            col("u.MeteredResourceGUID").as("MeteredResourceGUID"),
            col("u.ResourceURI").as("ResourceURI"),
            col("u.Location").as("AzureRegion"),
            col("u.MeteredRegion").as("MeteredRegion"),
            col("u.MeteredService").as("MeteredService"),
            col("u.MeteredServiceType").as("MeteredServiceType"),
            col("u.Project").as("Project"),
            col("u.ServiceInfo1").as("ServiceInfo1"),
            col("u.ServiceInfo2").as("ServiceInfo2"),
            col("u.StartDateTime").as("UsageDate"),
            when(col("u.MeteredResourceGuid") === "SHAREDCORE", lit(0.1666).cast(FloatType))
              .when(col("u.MeteredResourceGuid").like("%CORE%"), regexp_replace(col("u.MeteredResourceGuid"), "CORE", "").cast(FloatType))
              .otherwise(lit(null).cast(StringType)).as("NumberOfCores"),
            when(col("u.Properties").like("%ServiceType\":\"%"), getStringBetween(col("u.Properties")))
              .otherwise(lit(null).cast(StringType)).as("VMSize"),
            col("u.PartNumber").as("PartNumber"),
            col("u.Tags").as("Tags"),
            col("u.Properties").as("Properties"),
            col("u.Quantity").cast(FloatType).as("Quantity"),
            when(col("u.MeteredResourceGuid") === "SHAREDCORE", col("u.Quantity").cast(FloatType) * lit(0.1666).cast(FloatType))
              .when(col("u.MeteredResourceGuid").like("%CORE%"), col("u.Quantity").cast(FloatType) * regexp_replace(col("u.MeteredResourceGuid"), "CORE", "").cast(FloatType))
              .otherwise(lit(0).cast(FloatType)).as("CoreQuantity"),
            when(col("u.ResourceURI").like("%Microsoft.ClassicCompute%"), lit("RDFE"))
              .when(col("u.ResourceURI").like("%Microsoft.Compute%"), lit("CRP"))
              .otherwise(lit("Unknown")).as("VMComputeStack"),
            col("u.SubscriptionGuid").as("SubscriptionGuid"),
            when(col("oap.OrderID").isNotNull, when(col("oap.DiscountType") === "None" && col("oap.Price").cast(DoubleType) == 0.0, lit("Free"))
                .when(col("oap.DiscountType") === "None" && col("oap.Price").cast(FloatType) > 0.0, lit("Paid"))
                .when(col("oap.DiscountType").isNotNull, col("oap.DiscountType"))
                .when(col("oap.Orderid").isNull, lit("OrderAPI Missing Order"))
                .otherwise(lit("OrderAPI Missing Price"))
            ).when(col("oap1.OrderID").isNotNull, when(col("oap1.DiscountType") === "None" && col("oap1.Price").cast(DoubleType) == 0.0, lit("Free"))
              .when(col("oap1.DiscountType") === "None" && col("oap1.Price").cast(FloatType) > 0.0, lit("Paid"))
              .when(col("oap1.DiscountType").isNotNull, col("oap1.DiscountType"))
              .when(col("oap1.Orderid").isNull, lit("OrderAPI Missing Order"))
              .otherwise(lit("OrderAPI Missing Price"))
            ).when(col("oapi.Orderid").isNull, lit("OrderAPI Missing Order"))
              .otherwise(lit("OrderAPI Missing Price")).as("OrderState")
          )

    joinedDF
  }

  def processNestedJoins(nestedJoinedDF : DataFrame) : DataFrame = {
    val selectedDF = nestedJoinedDF.select(
      col("OrderId"),
      col("AMDMOrderId"),
      col("MeteredResourceGuid"),
      col("ResourceURI"),
      col("AzureRegion"),
      col("MeteredRegion"),
      col("MeteredService"),
      col("MeteredServiceType"),
      col("Project"),
      col("ServiceInfo1"),
      col("ServiceInfo2"),
      col("UsageDate"),
      col("NumberOfCores"),
      col("VMSize"),
      col("PartNumber"),
      col("Tags"),
      col("Properties"),
      col("SUM(Quantity)").as("Quantity"), // To aggregate
      col("SUM(Quantity) * NumberOfCores").as("CoreQuantity"), // To aggregate
      lit(0).cast(IntegerType).as("IsCoreImage"),
      lit(0).cast(IntegerType).as("IsMultiSolution"),
      lit(1).cast(IntegerType).as("IsVMUsage"),
      lit(null).cast(StringType).as("TemplateUri"),
      lit(null).cast(StringType).as("CorrelationId"),
      lit(null).cast(StringType).as("MultiSolutionOfferName"),
      lit("UsageDaily").cast(StringType).as("UsageDataSource"),
      lit(null).cast(StringType).as("UsageAfterCancelDate"),
      lit(null).cast(StringType).as("VMUsageDefaultPrice"),
      lit(null).cast(StringType).as("InfluencedCost"),
      col("VMComputeStack"),
      lit(null).cast(StringType).as("Cluster"),
      lit(System.currentTimeMillis()).cast(LongType).as("AMDMCreatedDate"),
      lit(System.getProperty("user.name")).as("AMDMCreatedBy"),
      lit(System.currentTimeMillis()).cast(LongType).as("AMDMUpdatedDate"),
      lit(System.getProperty("user.name")).as("AMDMUpdatedBy"),
      col("OrderState")
    )
    selectedDF
  }

  def finalJoin(tDF : DataFrame, t1DF : DataFrame) : DataFrame = {
    val joinedDF = tDF.as("t").join(t1DF.as("oalp"), col("t.OrderId") === col("oalp.OrderId")
      and col("t.MeteredResourceGuid") === col("oalp.MeterName")
      and unix_timestamp(col("t.UsageDate"), "yyyy-MM-dd HH:mm:ss.S").cast(TimestampType).between(col("oalp.Price_StartDate").cast(TimestampType), col("oalp.Price_EndDate").cast(TimestampType))
    ).withColumn("OrderState1", when(col("OrderState") === "OrderAPI Missing Price",
      when(col("oalp.DiscountType") === "None" && col("oalp.Price").cast(DoubleType) == 0.0, lit("Free"))
        .when(col("oalp.DiscountType") === "None" && col("oalp.Price").cast(DoubleType) > 0.0, lit("Paid"))
        .when(col("oalp.DiscountType").isNotNull, col("oalp.DiscountType"))
        .otherwise(lit("OrderAPI Missing Price"))
    ).otherwise(col("OrderState"))
    ).drop(col("OrderState")).withColumnRenamed("OrderState1", "OrderState")

    val partitionedDF = DataFrameUtil.partitionDataFrame(configReader, "ordersUsage.ingest.tables.nonTableVmInfo", joinedDF, 0)

    val resultDF = partitionedDF.where(col("RN1") === 1)

    resultDF
  }

  private def getSubStringBetween = (identifier: String) => {
    val pattern1 : String = "%ServiceType\":\"%"
    val pattern2 : String = "%\"%"

    val startIndex = identifier.indexOf(pattern1)
    var endIndex = identifier.indexOf(pattern2, startIndex)
    endIndex = (startIndex, endIndex) match {
      case (0, x) => x
      case (x, 0) => x
      case _ => identifier.length
    }
    identifier.substring(startIndex, endIndex - 1)
  }

}
