package com.microsoft.common

import java.text.SimpleDateFormat
import java.util.Date
import java.util.regex.Pattern

import com.microsoft.config.ConfigReader
import com.microsoft.framework.VNextUtil
import com.microsoft.framework.VNextUtil.getPropertyValue
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, RelationalGroupedDataset, SparkSession}

import scala.collection.mutable.ListBuffer

object DataFrameUtil extends  java.io.Serializable{

  val log = Logger.getLogger(getClass().getName())

  val DEFAULT_STRING = ""
  val DEFAULT_INT =  -99999

  def extractColumnsFromDF(df: DataFrame, cols: List[String]): DataFrame = {

    var colNames = new ListBuffer[String]
    df.schema.fields.foreach(x => {
      if (cols.contains(x.name)) {
        colNames.append(x.name)
      }
    })
    val selectedCols = colNames.mkString(", ")
    df.select(selectedCols)
  }

  def generateRowKey(df: DataFrame, col1: String, col2: String): DataFrame = {

    val generateKey = udf(generateRowKey2)
    val rowKeyDF = df.withColumn("rowkey", generateKey(df.col(col1), df.col(col2)))
    rowKeyDF
  }

  def generateRowKey2 = (col1: String, col2: String) => {
    col1 + "," + col2
  }

  def generateRowKey(df: DataFrame, col1: String, col2: String, col3: String): DataFrame = {

    val generateKey = udf(generateRowKey3)
    val rowKeyDF = df.withColumn("rowkey", generateKey(df.col(col1), df.col(col2), df.col(col3)))
    rowKeyDF
  }

  def generateRowKey3 = (col1: String, col2: String, col3: String) => {
    col1 + "," + col2 + "," + col3
  }

  def genNullValue = () => None: Option[String]

  def readDataFrameFromBlobAsTSV(configReader: ConfigReader, basePath: String, tableName:String, inputArgsMap : Map[String, String], spark: SparkSession): DataFrame = {

    var inputFilePath = VNextUtil.getPropertyValue(inputArgsMap, tableName)

    var df : DataFrame = null
    try {

      val fileConfig = configReader.getConfig().getConfig(basePath)

      import scala.collection.JavaConversions._
      val configList = fileConfig.getConfigList("schema")

      var columnSchema = configList.map(x => {
        val schema = new ColumnSchema(x)
        schema
      }).toList

      df = spark.read.schema(generateDefaultSchema(columnSchema)).option("sep", "\t").csv(inputFilePath)
      df = applySchemaToDF(df, columnSchema)
      if(df==null)
        log.info(tableName +" is null")
      df
    } catch {

      case e: Exception => log.error(e.printStackTrace()); df
    }
  }

  def applySchemaToDF(df: DataFrame, schema: List[ColumnSchema]): DataFrame = {

    var modifiedDF = df

    val dateFormatUDF = udf(convertDateStringUDF)

    schema.foreach(x => {


      modifiedDF = x.dataType.toLowerCase() match {
        case "int" => modifiedDF.withColumn(x.name, modifiedDF.col(x.name).cast(IntegerType))
        case "double" => modifiedDF.withColumn(x.name, modifiedDF.col(x.name).cast(DoubleType))
        case "float" =>   modifiedDF.withColumn(x.name, modifiedDF.col(x.name).cast(FloatType))
        case "bool" => modifiedDF.withColumn(x.name, modifiedDF.col(x.name).cast(BooleanType))
        case "date" => modifiedDF.withColumn(x.name + "_Orig", modifiedDF.col(x.name)).withColumn(x.name, dateFormatUDF(modifiedDF.col(x.name), lit(x.sourceFormat), lit(x.extractPattern)))
        case _ => modifiedDF
      }
    })
    modifiedDF
  }

  def partitionDataFrame(df: DataFrame, partitionBy: String, orderby: String, partitionByColumnName: String, mode: Int): DataFrame = {

    val partitionCols = new ListBuffer[Column]
    partitionBy.split(",").foreach(x => partitionCols.append(df.col(x)))

    import org.apache.spark.sql.expressions.Window

    val orderByCols = new ListBuffer[Column]
    orderby.split(",").foreach(x => { // The partition columns may have sort order as well.
      val orders = x.split(" ")
      if (orders.size == 2) {
        orders(1).toLowerCase() match {
          case "desc" => orderByCols.append(df.col(orders(0)).desc)
          case _ => orderByCols.append(df.col(orders(0)).asc)
        }
      } else {
        orderByCols.append(df.col(x).desc)
      }
    })

    val byPartitionWindow = Window.partitionBy(partitionCols: _*).orderBy(orderByCols: _*)

    val partitionedDF = mode match {
      case 0 => df.withColumn(partitionByColumnName, row_number().over(byPartitionWindow))
      case 1 => df.withColumn(partitionByColumnName, rank().over(byPartitionWindow))
    }
    partitionedDF
  }

  def readDataFromBlobAndPartitionBy(configReader: ConfigReader, basePath: String, mode: Int,  tableName:String, inputArgsMap: Map[String, String], spark: SparkSession): DataFrame = {

    val df = DataFrameUtil.readDataFrameFromBlob(configReader, basePath, tableName, inputArgsMap, spark)

    val partitionedDF = DataFrameUtil.partitionDataFrame(configReader, basePath, df, mode)

    partitionedDF
  }

  /**
    * This function is used to read the json file. In case there is no configuration entry provided, the entire json file is converted to a DF and returned.
    * In case there is an entry in the config file, columns are sselected acordingly
    * @param configReader
    * @param basePath
    * @param tableName
    * @param inputArgsMap
    * @param spark
    * @return
    */
  def readDataFrameFromBlob(configReader: ConfigReader, basePath: String, tableName:String, inputArgsMap: Map[String, String], spark: SparkSession): DataFrame = {

    var path = VNextUtil.getPropertyValue(inputArgsMap, tableName)

    val df = spark.read.json(path)

      try {
        val selectColumns = if (configReader.getValueForKey(basePath + ".selectColumns") != None)
          configReader.getValueForKey(basePath + ".selectColumns").toString
        else
          "*"
        val transformedDF = DataFrameUtil.selectAndRenameColumns(df, selectColumns)
        transformedDF
      } catch {
        case e: Exception => df
      }
  }

  def readDataFrameFromBlob(tableName:String, inputArgsMap: Map[String, String], spark: SparkSession): DataFrame = {

    var path = VNextUtil.getPropertyValue(inputArgsMap, tableName)

   spark.read.json(path)
  }

  def selectAndRenameColumns(df: DataFrame, selectExpr: String): DataFrame = {

    val selectColAliasList: ListBuffer[(String, String)] = new ListBuffer[(String, String)]

    val selectCols = selectExpr.split(",")
    selectCols.foreach(col => {
      var colAlias = col.split(" as ")

      colAlias = colAlias.size match {
        case 0 => colAlias = col.split(" AS "); colAlias
        case _ => colAlias
      }

      val tuple = colAlias.length match {
        case 1 => (colAlias(0).trim, null)
        case 2 => (colAlias(0).trim, colAlias(1).trim)
      }
      selectColAliasList.append(tuple)
    })
    var transformedDF = df
    for ((name, alias) <- selectColAliasList) {
      alias match {
        case null => None
        case aliasName: String => transformedDF = transformedDF.withColumnRenamed(name, aliasName)
      }
    }
    transformedDF
  }

  def selectColumnsFromDFNew(df:DataFrame, selectExpr: String) : DataFrame = {

    val selectColAliasList: ListBuffer[(String, String)] = new ListBuffer[(String, String)]

    val selectCols = selectExpr.split(",")
    selectCols.foreach(col => {
      var colAlias = col.split(" as ")

      colAlias = colAlias.size match {
        case 0 => colAlias = col.split(" AS "); colAlias
        case _ => colAlias
      }

      val tuple = colAlias.length match {
        case 1 => (colAlias(0).trim, null)
        case 2 => (colAlias(0).trim, colAlias(1).trim)
      }
      selectColAliasList.append(tuple)
    })

    val selectColumns = selectColAliasList.map( _._1).mkString(",")

    var transformedDF = df.select(selectColumns)

    for ((name, alias) <- selectColAliasList) {
      alias match {
        case null => None
        case aliasName: String => transformedDF = transformedDF.withColumnRenamed(name, aliasName)
      }
    }
    transformedDF
  }

  /**
    *
    * @param configReader
    * @param basePath
    * @param df
    * @param mode 0 is for row_number and 1 for rank function
    * @return
    */
  def partitionDataFrame(configReader: ConfigReader, basePath: String, df: DataFrame, mode: Int): DataFrame = {

    val partitionBy = configReader.getValueForKey(basePath + ".partitionBy").toString
    val partitionCols = new ListBuffer[Column]
    partitionBy.split(",").foreach(x => partitionCols.append(df.col(x)))

    import org.apache.spark.sql.expressions.Window

    val orderbyCol = configReader.getValueForKey(basePath + ".orderBy").toString

    val orderByCols = orderbyCol.split(",")
    var orderCols = new ListBuffer[Column]
    orderByCols.foreach( x => {
      val orderArray = x.split(" ")
      var order = if (orderArray.size > 1) orderArray(1).trim else "desc"
      val column = order.toLowerCase() match {
        case "asc" => df.col(orderArray(0).trim).asc
        case "desc" => df.col(orderArray(0).trim).desc
        case _ => df.col(orderArray(0).trim).asc
      }
      orderCols.append(column)
    })

    val byPartitionWindow = Window.partitionBy(partitionCols: _*).orderBy(orderCols:_*)

    val partitionByColumnName = configReader.getValueForKey(basePath + ".partitionByColName").toString
    val partitionedDF = mode match {
      case 0 => df.withColumn(partitionByColumnName, row_number().over(byPartitionWindow))
      case 1 => df.withColumn(partitionByColumnName, rank().over(byPartitionWindow))
    }
    partitionedDF
  }

  def groupByDataFrame(configReader: ConfigReader, df: DataFrame, groupByForTempOrders: String): RelationalGroupedDataset = {
    val groupByArray = groupByForTempOrders.split(',')
    val colList = new ListBuffer[Column]
    groupByArray.foreach(x => colList.append(df.col(x)))
    df.groupBy(colList: _*)
  }

  /**
    * This function is used to generate the schema for the csv file that we are processing. This schema is read from
    * the configuration file and is then applied to the RDD.
    *
    * @param schema This is the deserialized version of the configuration file
    * @return This function returns the row schema for the dataFrame. This schema is of Type StuctType
    *
    */
  def generateDefaultSchema(schema: List[ColumnSchema]): StructType = {
    var dfSchema: StructType = null
    val colSchema: ListBuffer[StructField] = new ListBuffer[StructField]

    schema.foreach(column => {
      colSchema.append(getDefaultStructField(column))
    })
    StructType(colSchema.toList)
  }

  /**
    * Generates a StructField data type for a column. This is used to apply a schema for the column of a row
    *
    * @param column The column information
    * @return Schema for this column
    */
  def getStructField(column: ColumnSchema): StructField = {
    new StructField(
      name = column.name,
      dataType = column.dataType.toLowerCase match {
        case "string" => DataTypes.StringType
        case "int" => DataTypes.IntegerType
        case "double" => DataTypes.DoubleType
      },
      nullable = column.nullable match {
        case false => false
        case _ => true
      }
    )
  }

  def getDefaultStructField(column: ColumnSchema): StructField = {
    new StructField(
      name = column.name,
      dataType = DataTypes.StringType,
      nullable = column.nullable match {
        case false => false
        case _ => true
      })
  }

  def convertDateStringUDF = ( fromStr : String, fromFormat: String, extractFormat: String) => {

    if (fromStr == null) {
      -1
    } else {
      val fromDateStr = if (extractFormat != null) {
        val pattern = Pattern.compile(extractFormat)
        val matched = pattern.matcher(fromStr)
        if (matched.find()) {
          matched.group(0)
        } else {
          println("Date String format is " + fromStr + " in format " + fromFormat + " extract Format " + extractFormat)
          ""
        }
      } else {
        fromStr
      }
      val finalDateStr = if (fromDateStr.contains('T'))
        fromDateStr.replaceFirst("T", " ")
      else
        fromDateStr

      if ( fromDateStr.equals("")) {
        -1
      } else {
        val dateFormatter = new SimpleDateFormat(fromFormat)
        val date = dateFormatter.parse(finalDateStr)
        date.getTime
      }
    }
  }

  def convertDateInMillisToString = ( timeInMill : Long, toFormat : String) => {
    if ( timeInMill == null || timeInMill == -1) {
      ""
    } else {
      try {
        val date = new Date(timeInMill)
        val dateFormatter = new SimpleDateFormat(toFormat)
        dateFormatter.format(date)
      } catch {
        case e: Exception => ""
      }
    }
  }

  def convertDateColumnsToString(df : DataFrame, columns : Seq[String]):DataFrame = {

    val dfWithNull = fillDefaultValuesForDateColumns(df, columns)

    val convertDateUDF  = udf(convertDateInMillisToString)
    var tempDF = dfWithNull
    for (colName <- columns) {
      tempDF = tempDF.withColumnRenamed(colName, colName+"Long")
      tempDF = tempDF.withColumn(colName, convertDateUDF(tempDF.col(colName+"Long"), lit("yyyy-MM-dd HH:mm:ss.SSS")))
    }
    tempDF
  }

  def fillDefaultValuesForNullColumns(df : DataFrame) : DataFrame = {

    val stringCols = df.schema.filter( x => x.dataType.typeName.equalsIgnoreCase("String")).map( x => x.name)

    val otherCols = df.schema.map(x => x.name).diff(stringCols)

    df.na.fill(DEFAULT_STRING, stringCols).na.fill(DEFAULT_INT, otherCols)
  }

  def fillDefaultValuesForDateColumns(df : DataFrame, cols : Seq[String]): DataFrame ={
    df.na.fill(-1, cols)
  }

  def union(df1 : DataFrame, df2 : DataFrame) : DataFrame = {

    val firstDFCols = df1.columns

    val secondDFCols = df2.columns

    if (firstDFCols.length != secondDFCols.length) {
      throw new VNextException(s"""The number of columns don't match. """ +
        s"""The first dataset has (${firstDFCols.mkString(",")})"""  +
        s"""And the second dataset has (${secondDFCols.mkString(",")})""")
    }

    val missingCols = firstDFCols.map( x => x.toLowerCase()).diff(secondDFCols.map(x => x.toLowerCase()))

    if ( missingCols.length > 0 ) throw new VNextException(s"""The columns """ +
      s"""in the first dataset has (${missingCols.mkString(",")}) not found in the second dataset""")

    val missingColsIn2ndDS = secondDFCols.map( x => x.toLowerCase()).diff(firstDFCols.map(x => x.toLowerCase()))

    if ( missingCols.length > 0 ) throw new VNextException(s"""The columns """ +
      s"""in the second dataset has (${missingColsIn2ndDS.mkString(",")}) not found in the first dataset""")

    val secondCols = firstDFCols.map( x => df2.col(x))

    val secondDFWithSameName = df2.select(secondCols:_*)

    df1.union(secondDFWithSameName)
  }

  def readDataFrameFromBlobAsParquet(configReader: ConfigReader, basePath: String, tableName:String, inputArgsMap: Map[String, String], spark: SparkSession): DataFrame = {

    var path = VNextUtil.getPropertyValue(inputArgsMap, tableName)

    val df = spark.read.parquet(path)

    try {
      val selectColumns = if (configReader.getValueForKey(basePath + ".selectColumns") != None)
        configReader.getValueForKey(basePath + ".selectColumns").toString
      else
        "*"
      val transformedDF = DataFrameUtil.selectAndRenameColumns(df, selectColumns)
      transformedDF
    } catch {
      case e: Exception => df
    }
  }
}
