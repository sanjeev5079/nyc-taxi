package se.telenor.aep.dataplatform

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ DataFrame, SparkSession }
import se.telenor.analytics.etl.common.spark.Logging
import ch.cern.sparkmeasure

object EnrichBlacklist extends Logging {

  val spark = SparkSession.builder()
    .appName("EnrichBlacklist")
    .getOrCreate()

  /**
   * Gets the latest blacklist with all the mapped msisdns from previous days BL.
   * 1. oldBlDf: take all old BLv, distinct on org_pers_id, account, subs_id, msisdn, where msisdn is null.
   * 2. currentBlDf: Get BL from the run days' partition. Remove the duplicates based on "timestamp", "org_pers_id", "account", "subs_id", "msisdn".
   * 3. finalBlDf: Left join currentBlDf and oldBlDf on subs_id. Coalesces msisdn from bothe the DataFrames.
   * @param db
   * @param table
   * @param currRunDate
   * @return :  DataFrame: Latest BL updated with already mapped msisdns.
   */
  def getBlacklist(db: String, table: String, currRunDate: String): DataFrame = {

    val oldBlDf = spark.sql(
      s"""
         |SELECT DISTINCT
         |org_pers_id, account, subs_id, msisdn
         |FROM $db.$table
         |WHERE ingestion_date < "$currRunDate"
         |AND msisdn is not null
         |""".stripMargin)

    val currentBlDf = spark.sql(
      s"""
         |SELECT
         | *
         | FROM $db.$table
         | WHERE ingestion_date = '$currRunDate' """.stripMargin).dropDuplicates(Array("timestamp", "org_pers_id", "account", "subs_id", "msisdn")) //.distinct()

    val finalBlDf = currentBlDf.alias("df1")
      .join(broadcast(oldBlDf.alias("df2")), col("df1.subs_id") === col("df2.subs_id"), "left")
      .select(
        col("df1.timestamp"),
        col("df1.org_pers_id"),
        col("df1.account"),
        col("df1.subs_id"),
        coalesce(
          col("df1.msisdn"),
          col("df2.msisdn")).alias("msisdn"),
        col("df1.ingestion_date"),
        col("df1.lineage"))

    finalBlDf

    /*val sqlStmt = s"""SELECT
                     | *
                     | FROM $db.$table
                     | WHERE ingestion_date = '$currRunDate' """.stripMargin
    log.info(sqlStmt)
    spark.sql(sqlStmt).dropDuplicates(Array("timestamp", "org_pers_id", "account", "subs_id", "msisdn")).distinct()*/
  }

  /**
   * Modified as per BL in Access layer.
   * Gets the latest blacklist with all the mapped msisdns from previous days BL.
   * 1. oldBlDf: take all old BLv, distinct on org_pers_id, account, subs_id, msisdn, where msisdn is null.
   * 2. currentBlDf: Get BL from the run days' partition. Remove the duplicates based on "timestamp", "org_pers_id", "account", "subs_id", "msisdn".
   * 3. finalBlDf: Left join currentBlDf and oldBlDf on subs_id. Coalesces msisdn from bothe the DataFrames.
   * @param currRunDate
   * @return :  DataFrame: Latest BL updated with already mapped msisdns.
   */
  def getBlacklistAccess(currRunDate: String): DataFrame = {

    val oldBlDf = spark.sql(
      s"""
         |SELECT DISTINCT
         |org_pers_id, account, subs_id, msisdn
         |FROM operations_matrix.blacklist_access
         |WHERE ds < "$currRunDate"
         |AND msisdn is not null
         |""".stripMargin)

    val currentBlDf = spark.sql(
      s"""
         |SELECT
         | *
         | FROM operations_matrix.blacklist_raw
         | WHERE ingestion_date = '$currRunDate' """.stripMargin).dropDuplicates(Array("timestamp", "org_pers_id", "account", "subs_id", "msisdn")) //.distinct()

    val finalBlDf = currentBlDf.alias("df1")
      .join(broadcast(oldBlDf.alias("df2")), col("df1.subs_id") === col("df2.subs_id"), "left")
      .select(
        col("df1.timestamp"),
        col("df1.org_pers_id"),
        col("df1.account"),
        col("df1.subs_id"),
        coalesce(
          col("df1.msisdn"),
          col("df2.msisdn")).alias("msisdn"),
        col("df1.lineage"))
      .withColumn("ds", lit(currRunDate))

    finalBlDf
  }

  /**
   * Enriches the new BL with msisdn using subs_extended table from Salsa DB.
   * @param blDf: Blacklist DF.
   * @return: DataFrame: BL Enriched with msisdn.
   */
  def enrichWithMSISDN(blDf: DataFrame): DataFrame = {
    val subsExtendedDf = spark.sql(
      """SELECT
        |DISTINCT subs_id AS subs_id_extended, msisdn AS msisdn_extended
        |FROM access_salsa.subs_extended where subs_id is not null
        |""".stripMargin)

    val originalColSequence = blDf.schema.fieldNames

    val enrichedBlDf = broadcast(blDf
      .as("df1"))
      .join(subsExtendedDf.as("df2"), col(s"df1.subs_id") === col(s"df2.subs_id_extended"), "left")
      .drop(col("df2.subs_id_extended"))
      .drop(col("df1.msisdn"))
      .withColumnRenamed("msisdn_extended", "msisdn")
      .select(originalColSequence.map(col): _*)

    enrichedBlDf
  }

  /**
   * Writes enriched BL back to the run date partition with Overwite mode.
   * @param df: Final enriched BL Df.
   * @param db
   * @param table
   */
  def writeBlacklist(df: DataFrame, db: String, table: String): Unit = {

    //df.coalesce(1).createOrReplaceTempView("enriched_blacklist_tmp")
    df.createOrReplaceTempView("enriched_blacklist_tmp")

    val insertStmt =
      s"""INSERT OVERWRITE TABLE $db.$table
        PARTITION(ds)
        SELECT *
        FROM enriched_blacklist_tmp
        """.stripMargin
    log.info("Writing to disk using query:-> " + insertStmt)
    spark.sql(insertStmt)
  }

  def main(args: Array[String]): Unit = {

    val appConf = ConfigFactory.load()
    val jc = ConfParser(appConf).getConf
    val stageMetrics = sparkmeasure.StageMetrics(spark)
    spark.conf.set("hive.exec.dynamic.partition", true)
    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")

    //val blDf = getBlacklist("operations_matrix", "blacklist", jc.currentRunDate)
    val blDf = getBlacklistAccess(jc.currentRunDate)

    val blDfMSISDN = enrichWithMSISDN(blDf)

    stageMetrics.runAndMeasure { writeBlacklist(blDfMSISDN, "operations_matrix", "blacklist_access") }
    val jobMatrixDf = stageMetrics.createStageMetricsDF("PerfStageMetrics")
    jobMatrixDf.show(100, false)

  }
}
