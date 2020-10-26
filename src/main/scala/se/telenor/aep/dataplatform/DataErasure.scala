package se.telenor.aep.dataplatform

import se.telenor.analytics.etl.common.spark.Logging
import org.apache.spark.sql.{ DataFrame, SparkSession }
import com.typesafe.config.{ Config, ConfigFactory }
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import ch.cern.sparkmeasure

object DataErasure extends Logging {

  val spark = SparkSession.builder()
    .appName("DataErasure")
    .getOrCreate()

  def getBlacklist(blacklistBaseFilePath: String, prevSuccessRunDate: String, currRunDate: String, firstRun: String): DataFrame = {
    //if (prevSuccessRunDate != "None") {
    //  log.info("Blacklist delta will be calculated as previous success run date is found: " + prevSuccessRunDate)

    //  val prevSuccessBl = spark.read.format("csv")
    //    .option("header", "true")
    //    .option("inferSchema", "true")
    //    .load(blacklistBaseFilePath + "/ingestion_date=" + prevSuccessRunDate + "/*")

    //  val currBl = spark.read.format("csv")
    //    .option("header", "true")
    //    .option("inferSchema", "true")
    //    .load(blacklistBaseFilePath + "/ingestion_date=" + currRunDate + "/*")

    //  currBl.except(prevSuccessBl).toDF()
    //} else {
    //  log.info("Blacklisting will be performed on whole list as previous success run date is not found.")
    //  spark.read.format("csv")
    //    .option("header", "true")
    //    .option("inferSchema", "true")
    //    .option("escape", "\"")
    //    .option("encoding", "UTF-8")
    //    .load(blacklistBaseFilePath + "/ingestion_date=" + currRunDate + "/*")
    //}

    if (firstRun.toLowerCase != "true") {
      log.info("Its not a first run.")

      //spark.read.format("csv")
      //.option("header", "true")
      //.option("inferSchema", "true")
      //.option("escape", "\"")
      //.option("encoding", "UTF-8")
      //.load(blacklistBaseFilePath + "/ingestion_date=" + currRunDate + "/*")

      spark.sql(
        s"""
           |SELECT DISTINCT
           |org_pers_id, account, subs_id, msisdn
           |FROM operations_matrix.blacklist
           |WHERE ingestion_date = "$currRunDate"
           |""".stripMargin)

    } else {

      log.info("Its a first run.")
      val currentBLDf = spark.sql(
        s"""
           |SELECT DISTINCT
           |org_pers_id, account, subs_id, msisdn
           |FROM operations_matrix.blacklist
           |WHERE ingestion_date = "$currRunDate"
           |""".stripMargin)

      val oldBlDf = spark.sql(
        s"""
          |SELECT DISTINCT
          |org_pers_id, account, subs_id, msisdn
          |FROM operations_matrix.blacklist
          |WHERE ingestion_date < "$currRunDate"
          |AND msisdn is not null
          |""".stripMargin)

      val finalBlDf = broadcast(currentBLDf.alias("df1"))
        .join(oldBlDf.alias("df2"), col("df1.subs_id") === col("df2.subs_id"), "left")
        .select(col("df1.org_pers_id"), col("df1.account"), col("df1.subs_id"), coalesce(col("df1.msisdn"), col("df2.msisdn")).alias("msisdn"))
        .dropDuplicates("subs_id")

      //val onlyInOldDataDf = oldBlDf.except(intermediateJoinBlDf)

      //val finalBlDf = intermediateJoinBlDf.union(onlyInOldDataDf)

      finalBlDf.toDF()
    }

  }

  def getTableDataAndCols(db: String, table: String, joinQueryToBuildTable: String): (DataFrame, Array[String]) = {
    log.info("Join query passed from configuration file is: " + joinQueryToBuildTable)
    val (wholeTableDf, columns) = if (joinQueryToBuildTable != "" && joinQueryToBuildTable != null && joinQueryToBuildTable != "None") {
      val df = spark.sql(joinQueryToBuildTable)
      (df, df.schema.fieldNames)
    } else {
      val df = spark.sql("SELECT * FROM " + db + "." + table)
      (df, df.schema.fieldNames)
    }
    (wholeTableDf, columns)
  }

  def getHighestOrderFilterCol(columns: Array[String]): Option[Array[String]] = {
    val highestOrderBlCol = columns.indexWhere(x => x.toLowerCase == "org_pers_id") match {
      case -1 => columns.indexWhere(x => x.toLowerCase == "org_id") match {
        case -1 => columns.indexWhere(x => x.toLowerCase == "account") match {
          case -1 => columns.indexWhere(x => x.toLowerCase == "subs_id") match {
            case -1 => columns.indexWhere(x => x.toLowerCase == "subsid") match {
              case -1 => columns.indexWhere(x => x.toLowerCase == "user_key") match {
                case -1 => columns.indexWhere(x => x.toLowerCase == "msisdn") match {
                  case -1 => None
                  case _ => Some(Array("msisdn", "msisdn"))
                }
                case _ => Some(Array("subs_id", "user_key"))
              }
              case _ => Some(Array("subs_id", "subsid"))
            }
            case _ => Some(Array("subs_id", "subs_id"))
          }
          case _ => Some(Array("account", "account"))
        }
        case _ => Some(Array("org_pers_id", "org_id"))
      }
      case _ => Some(Array("org_pers_id", "org_pers_id"))
    }
    highestOrderBlCol
  }

  def getBlFilteringKeys(filterCol: String, df: DataFrame): DataFrame = {
    val blFilteringKeys = filterCol match {
      case "" => df.select().toDF() // return blank df TODO: logic for joins when no blcol exists
      case _ => df.select(filterCol).distinct().where(col(filterCol).notEqual("NULL") || col(filterCol).notEqual(null)).toDF() //.collect().map(row => row.mkString) //bl data can have duplicates. distinct will reduce values in IN /NOT IN clause
    }
    blFilteringKeys
  }

  def getCleanData(wholeTableDf: DataFrame, filterCol: String, blDf: DataFrame): DataFrame = {

    val blJoinCol = blDf.schema.fieldNames(0)
    val intermediateDf = wholeTableDf.select(col(filterCol), col("ds")).distinct() //.toDF()

    val affectedPartitionsDf = intermediateDf
      .as("df1")
      .join(broadcast(blDf.as("df2")), col(s"df1.$filterCol") === col(s"df2.$blJoinCol"), "inner")
      .select("df1.ds").distinct() //.toDF()

    val dataFromAffectedPartitionsDf = wholeTableDf
      .as("df1")
      .join(broadcast(affectedPartitionsDf.as("df2")), col("df1.ds") === col("df2.ds"), "inner")
      .select("df1.*")

    //below code is to avoid a join on whole table which has duplicates as all the partitions are being processed..
    //val whereIn = "\"" + affectedPartitionsDf.collect().map(row => row.mkString).mkString("\",\"") + "\""
    //val dataFromAffectedPartitionsDf = wholeTableDf.where(s""" ds in ($whereIn)  """)

    val nonBlDatafromAffectedPartitionsDf = dataFromAffectedPartitionsDf
      .as("df1")
      .join(broadcast(blDf.as("df2")), col(s"df1.$filterCol") === col(s"df2.$blJoinCol"), "leftanti")
      .select("df1.*")

    nonBlDatafromAffectedPartitionsDf
  }

  def writeData(df: DataFrame, db: String, table: String, partitionCol: String = "ds"): Unit = {
    val tmpTable = table + "_tmp"
    df.createOrReplaceTempView(tmpTable)

    val insertStmt =
      s"""INSERT OVERWRITE TABLE $db.$table
        PARTITION($partitionCol)
        SELECT *
        FROM $tmpTable
        """.stripMargin
    log.info("Writing to disk using query:-> " + insertStmt)
    spark.sql(insertStmt)
  }

  def writeJobMatrix(
    jobMatrixDf: DataFrame,
    dataTable: String = "",
    blacklistFilterCol: String = "",
    userQuery: String = "",
    runDate: String): Unit = {

    val numBlacklistRecord = jobMatrixDf
      .select("recordsRead")
      .where("""  stageId = "1" """).take(1).map(row => row.mkString).mkString

    val dataErasureMatrixTmp = jobMatrixDf
      .select("recordsRead", "bytesRead", "recordsWritten", "bytesWritten")
      .where("""  stageId = "10" """)

    val dataErasureMatrix = dataErasureMatrixTmp
      .withColumn("table", lit(dataTable))
      .withColumn("user_query", lit(userQuery))
      .withColumn("blacklist_col", lit(blacklistFilterCol))
      .withColumn("blacklist_records", lit(numBlacklistRecord))
      .withColumnRenamed("recordsRead", "records_read")
      .withColumnRenamed("recordsWritten", "records_written")
      .withColumnRenamed("bytesRead", "bytes_read")
      .withColumnRenamed("bytesWritten", "bytes_written")
      .withColumn("ts", lit(System.currentTimeMillis()))
      .withColumn("table_name", lit(dataTable))
      .withColumn("ds", lit(runDate))
      .select(
        "table",
        "user_query",
        "blacklist_col",
        "blacklist_records",
        "records_read",
        "records_written",
        "bytes_read",
        "bytes_written",
        "ts",
        "table_name",
        "ds")

    dataErasureMatrix.coalesce(1).createOrReplaceTempView("tmpTbl")
    //jobMatrixDf.show(100, false)
    val insertStmt =
      s"""INSERT INTO TABLE operations_matrix.data_erasure_matrix
        PARTITION(table_name, ds)
        SELECT *
        FROM tmpTbl
        """.stripMargin
    spark.sql(insertStmt)

  }

  def main(args: Array[String]): Unit = {

    val appConf = ConfigFactory.load()
    val jc = ConfParser(appConf).getConf
    val stageMetrics = sparkmeasure.StageMetrics(spark)
    spark.conf.set("hive.exec.dynamic.partition", true)
    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")

    val db = jc.erasureDb
    val table = jc.erasureTable
    val joinQueryToBuildTable = jc.joinQueryToBuildTable
    val prevSuccessRunDate = "[0-9]{4}-[0-9]{2}-[0-9]{2}".r.findFirstMatchIn(jc.prevSuccessRunDate).getOrElse("None").toString

    val blDf = getBlacklist(jc.blacklistFileBasePath, prevSuccessRunDate, jc.currentRunDate, jc.firstRun)

    val (wholeTableDf, tblColumns) = getTableDataAndCols(db, table, joinQueryToBuildTable)
    val highestOrderFilterCol = getHighestOrderFilterCol(tblColumns)

    var (blHighestOrderFilterCol, tableHighestOrderFilterCol) = ("", "")
    try {
      blHighestOrderFilterCol = highestOrderFilterCol.get(0)
      tableHighestOrderFilterCol = highestOrderFilterCol.get(1)
      log.info("Filter Column from Blacklist and Table respectively are : " + blHighestOrderFilterCol + " & " + tableHighestOrderFilterCol)
    } catch {
      case e: NoSuchElementException => log.error(s"Table $db.$table may not contain any of the blacklist columns (org number, account, subs id, msisdn). " +
        s"Pass a specific join query to tag one of the blacklist columns from configuration file.")
    }

    val blFilteringKeysDf = getBlFilteringKeys(blHighestOrderFilterCol, blDf)

    //wholeTableDf.persist(StorageLevel.MEMORY_AND_DISK)
    //blFilteringKeysDf.persist(StorageLevel.MEMORY_ONLY)
    val cleanDataDf = getCleanData(wholeTableDf, tableHighestOrderFilterCol, blFilteringKeysDf)
    log.info("Columns of DF cleaned of blacklist records are: " + cleanDataDf.schema.mkString(","))
    val finalDf = if (joinQueryToBuildTable.toLowerCase.contains("join")) {
      cleanDataDf.drop(tableHighestOrderFilterCol)
    } else cleanDataDf

    log.info("Columns of the final DF to be written are: " + finalDf.schema.mkString(","))
    //log.info("Total blacklist filtering keys count: " + blFilteringKeysDf.count().toString)
    //log.info("Total count of records to be re-written: " + finalDf.count().toString)

    stageMetrics.runAndMeasure { writeData(finalDf, db, table) }
    val jobMatrixDf = stageMetrics.createStageMetricsDF("PerfStageMetrics")
    writeJobMatrix(
      jobMatrixDf,
      table,
      tableHighestOrderFilterCol,
      joinQueryToBuildTable,
      jc.currentRunDate)
    //wholeTableDf.unpersist()
    //blFilteringKeysDf.unpersist()

  }
}
