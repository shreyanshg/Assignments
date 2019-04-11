package assignemnt2

import java.sql.Date

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.types.{DateType, LongType, TimestampType}

import scala.collection.JavaConverters._

/** Assignment 2
  *
  * Created by Shreyansh Gupta on 2019-04-02
  */
object Assignment2 {

  case class Notification(from: String, to: String, departureDate: Date, status: String)

  def main(args: Array[String]): Unit = {

    val inputFilePath = "/assignment2/assignment2.csv"
    Logger.getLogger("org").setLevel(Level.ERROR)
    val readConfig = ConfigFactory.parseString(
      s"""
        |{
        | "type": "csv",
        | "option": {
        |   "path": $inputFilePath,
        |   "resource": true,
        |   "header": "true"
        | }
        |}
      """.stripMargin)
    val sc = initialiseSparkContext()
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val readDF = readCsv(sqlContext, readConfig)

    val currentDate = Date.valueOf("2018-09-01") // new Date(DateTime.now().getMillis)
    val colList = List("userd id", "origin", "destination")
    val schema = readDF.schema
    val notificationRDD = readDF.withColumn("search date time", $"search date time".cast(TimestampType).cast(LongType))
      .withColumn("departure date", $"departure date".cast(DateType))
      .filter($"departure date" >= currentDate)
      .rdd.groupBy(row =>
      colList.map(columnName => row.getAs[String](columnName))
    ).map(groupedTuple => {
      val rowList = groupedTuple._2.toList

      val searchDateTimeColumnNum = schema.fieldIndex("search date time")
      val maxSearchTimeRow = rowList.maxBy(_.getLong(searchDateTimeColumnNum))
      val from = maxSearchTimeRow.getAs[String]("origin")
      val to = maxSearchTimeRow.getAs[String]("destination")
      val departureDate = maxSearchTimeRow.getAs[Date]("departure date")
      val pageName = maxSearchTimeRow.getAs[String]("page name")
      val status = pageName match {
        case "thankyou" => "booked"
        case _ => pageName
      }
      Notification(from, to, departureDate, status)
    })
    notificationRDD.foreach{notification =>
      println("************")
      println(s"Your status for booking from ${notification.from} to ${notification.to} on ${notification.departureDate} " +
        s"is - ${notification.status.toUpperCase}")
      println("************")
    }
  }

  def initialiseSparkContext(): SparkContext = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName(this.getClass.getSimpleName)

    new SparkContext(conf)
  }

  /** Read csv as DataFrame
    *
    * @param sqlContext SQLContext
    * @param config Config
    * @return DataFrame
    */
  def readCsv(sqlContext: SQLContext, config: Config): DataFrame = {
    val optionConfig = config.getConfig("option")
    val filePath = optionConfig.getString("path")
    val resourceFile = if (optionConfig.hasPath("resource")) {
      optionConfig.getBoolean("resource")
    } else {
      false
    }
    val updatedFilePath = if (resourceFile) {
      getClass.getResource(filePath).getFile
    } else {
      filePath
    }

    val fileDelimiter = if (optionConfig.hasPath("delimiter")) {
      optionConfig.getString("delimiter")
    } else {
      ","
    }
    val inferSchema = if (optionConfig.hasPath("inferSchema")) {
      optionConfig.getString("inferSchema")
    } else {
      "false"
    }
    val header = if (optionConfig.hasPath("header")) {
      optionConfig.getString("header")
    } else {
      "false"
    }
    val quote = if (optionConfig.hasPath("quote")) {
      optionConfig.getString("quote")
    } else {
      "\""
    }
    val escape = if (optionConfig.hasPath("escape")) {
      optionConfig.getString("escape")
    } else {
      "\\"
    }
    val nullValue = if (optionConfig.hasPath("nullValue")) {
      optionConfig.getString("nullValue")
    } else {
      "null"
    }
    val mode = if (optionConfig.hasPath("mode")) {
      optionConfig.getString("mode")
    } else {
      "PERMISSIVE"
    }
    val charset = if (optionConfig.hasPath("charset")) {
      optionConfig.getString("charset")
    } else {
      "UTF-8"
    }
    val comment = if (optionConfig.hasPath("comment")) {
      optionConfig.getString("comment")
    } else {
      "#"
    }
    val parserLib = if (optionConfig.hasPath("parserLib")) {
      optionConfig.getString("parserLib")
    } else {
      "commons"
    }
    val dateFormat = if (optionConfig.hasPath("dateFormat")) {
      optionConfig.getString("dateFormat")
    } else {
      "null"
    }

    val csvOptions = Map(
      "header" -> header,
      "delimiter" -> fileDelimiter,
      "inferSchema" -> inferSchema,
      "quote" -> quote,
      "escape" -> escape,
      "nullValue" -> nullValue,
      "mode" -> mode,
      "charset" -> charset,
      "comment" -> comment,
      "parserLib" -> parserLib,
      "dateFormat" -> dateFormat
    )
    val readDF = sqlContext.read.format("com.databricks.spark.csv")
      .options(csvOptions).load(updatedFilePath)

    val selectedColumnsDF = if (optionConfig.hasPath("columnNames") &&
      !optionConfig.getStringList("columnNames").isEmpty) {
      val columnNameList = optionConfig.getStringList("columnNames").asScala.toList
      addHeader(readDF.select(columnNameList.head, columnNameList.tail: _*), config)
    }
    else {
      addHeader(readDF, config)
    }
    selectedColumnsDF
  }

  /** Adds column names in the dataframe if 'headersList' key is present in the config
    *
    * @param dataFrame Dataframe in which column names are to be added
    * @return Dataframe with added header columns
    */
  def addHeader(dataFrame: DataFrame, config: Config): DataFrame = {
    if (config.hasPath("headersList")) {
      val columnNameList: List[String] = config.getStringList("headersList").asScala.toList
      val dataFrameColumnNameList = dataFrame.columns.toList
      require(columnNameList.length == dataFrameColumnNameList.length,
        "Header column count can not be different from input file column count")
      val columnNameTupleList = dataFrameColumnNameList.zip(columnNameList)
      val columnList = columnNameTupleList.map {
        case (oldName, newName) => dataFrame(oldName).as(newName)
      }
      dataFrame.select(columnList: _*)
    } else {
      dataFrame
    }
  }

}
