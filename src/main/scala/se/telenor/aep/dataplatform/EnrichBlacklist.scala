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

  def getBlacklist(db: String, table: String, currRunDate: String): DataFrame = {

    val sqlStmt = s"""SELECT
                     | *
                     | FROM $db.$table
                     | WHERE ingestion_date = '$currRunDate' """.stripMargin
    log.info(sqlStmt)
    spark.sql(sqlStmt).dropDuplicates(Array("timestamp", "org_pers_id", "account", "subs_id", "msisdn")).distinct()
  }

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

  def writeBlacklist(df: DataFrame, db: String, table: String): Unit = {

    df.coalesce(1).createOrReplaceTempView("enriched_blacklist_tmp")

    val insertStmt =
      s"""INSERT OVERWRITE TABLE $db.$table
        PARTITION(ingestion_date, lineage)
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

    val prevSuccessRunDate = "[0-9]{4}-[0-9]{2}-[0-9]{2}".r.findFirstMatchIn(jc.prevSuccessRunDate).getOrElse("None").toString

    val blDf = getBlacklist("operations_matrix", "blacklist", jc.currentRunDate)

    val blDfMSISDN = enrichWithMSISDN(blDf)

    stageMetrics.runAndMeasure { writeBlacklist(blDfMSISDN, "operations_matrix", "blacklist") }
    val jobMatrixDf = stageMetrics.createStageMetricsDF("PerfStageMetrics")
    jobMatrixDf.show(100, false)

  }
}
