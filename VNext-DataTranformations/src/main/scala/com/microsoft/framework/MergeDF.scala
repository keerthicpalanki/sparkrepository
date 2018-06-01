package com.microsoft.framework

import com.microsoft.common.DataFrameUtil
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer

object MergeDF {

  def addStartEndDates ( dataFrame: DataFrame) : DataFrame = {
    dataFrame.withColumn("StartDate", unix_timestamp()).withColumn("EndDate", lit(VNextUtil.TIMESTAMP_MAX_VAL))
  }



  def insertUpdateDataFrames( oldDF1 : DataFrame, newDF1 : DataFrame, rowKey: List[String]): DataFrame ={

    val oldDF = oldDF1.withColumn("rowKey", lower(col("rowKey")))
    val newDF = newDF1.withColumn("rowKey", lower(col("rowKey")))


    val newRowKeysDF = newDF.select("rowKey").withColumnRenamed("rowKey", "rowKeyDelta")

    println("The old dataframe schema has " + oldDF.columns.size + " columns")
    oldDF.printSchema()

    println("The new dataframe schema has " + newDF.columns.size + " columns ")
    newDF.printSchema()

    println("newRowKeysDF schema is " + newRowKeysDF.columns.size + " columns")
    newRowKeysDF.printSchema()

    //Do a left join with old DF
    val joinedDF = oldDF.join(newRowKeysDF, oldDF.col("rowKey") === newRowKeysDF.col("rowKeyDelta"), "left")

    println("Joined Data frame schema has " + joinedDF.columns.size + " columns ")
    joinedDF.printSchema()

    val notUpdatedDF = joinedDF.filter( joinedDF.col("rowKeyDelta").isNull)

    val notUpdatedFinalDF = notUpdatedDF.drop("rowKeyDelta")

    val mergedDF = DataFrameUtil.union(notUpdatedFinalDF, newDF)


    mergedDF
  }



  def getHistoricalDF ( oldDF : DataFrame, pNewDF : DataFrame) : DataFrame = {

    println("The columns in the previous Merge DF are " + oldDF.columns.size)
    oldDF.printSchema()

    val newDF = addStartEndDates(pNewDF)
    println("The columns in the new merge DF are " + newDF.columns.size)
    newDF.printSchema()

    val newWithRowKeyDF = newDF.select("rowKey").withColumnRenamed("rowKey", "rowKeyToDrop")
    println("RowKey only schema")
    newWithRowKeyDF.printSchema()

    val updatedOldDF = oldDF.join(newWithRowKeyDF, oldDF.col("rowKey") === newWithRowKeyDF.col("rowKeyToDrop"), "left")
          .withColumn("NewEndDate", when( col("rowKeyToDrop") === null, col("EndDate"))
                                                .otherwise(unix_timestamp() - 24*60*60)) // day -1

    println("UpdatedOldDF shcema")
    updatedOldDF.printSchema()

    val modifiedOldDF = updatedOldDF.drop("EndDate").drop("EndDate").drop("rowKeyToDrop").withColumnRenamed("NewEndDate", "EndDate")

    println("The columns in updatedOldDF is " + modifiedOldDF.columns.size)
    modifiedOldDF.printSchema()

    DataFrameUtil.union(modifiedOldDF, newDF)
  }
}
