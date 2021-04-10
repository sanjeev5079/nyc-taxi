package se.seb.dataplatform

import org.apache.spark.sql.functions.{ col, expr, udf, _ }
import org.apache.spark.sql.{ SparkSession, _ }

class Ingestion(spark: SparkSession, jobConf: JobConfig1) {

  def funLineageDate: ((String) => String) = { (s) =>
    val lineage = "lineage=[0-9]{14}_[aA0-zZ9]{5}".r.findFirstMatchIn(s).getOrElse("error-in-pathname").toString
    if (lineage.contains("=")) lineage.split("=")(1) else lineage
  }

  val lineageDate = udf(funLineageDate)

  def createPartitionsIngestionDate(srcPath: String): String = {
    val filePath = srcPath
    val partitionValue = "ingestion_date=[0-9]{4}-[0-9]{2}-[0-9]{2}".r.findFirstMatchIn(filePath).getOrElse("error-in-pathname").toString
    if (partitionValue.contains("=")) partitionValue.split("=")(1) else partitionValue
  }

  def run() {
    spark.conf.set("hive.exec.dynamic.partition", true)
    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")

    val InputDf = load(jobConf.multiline, jobConf.delimiter, jobConf.header, jobConf.encoding, jobConf.srcPath)
      .withColumn("lineage_date", lineageDate(input_file_name()))
    val withDataTypesDf = convertTypes(InputDf, jobConf.conversions, jobConf.timestampFormat)
    val withPartitionColDf = addPartitionColumnToDataframe(withDataTypesDf, jobConf.srcPath, jobConf.parrtitionColumnSql, jobConf.partitionColumnName)
      .orderBy(col(jobConf.partitionColumnName))
    createHiveTableFromDataframe(withPartitionColDf, jobConf.partitionColumnName, jobConf.fullyQualifiedDestinationTableName, jobConf.destinationPath)
    writeParquetFile(withPartitionColDf, jobConf.fullyQualifiedDestinationTableName, jobConf.partitionColumnName)
  }

  def writeParquetFile(df: DataFrame, fullyQualifiedDestinationTableName: String, partitionColumnName: String) = {
    val tmpTbl = "tmp_tbl"
    df.createOrReplaceTempView(tmpTbl)

    val insertStmt =
      s"""INSERT OVERWRITE TABLE ${fullyQualifiedDestinationTableName}
        PARTITION(${partitionColumnName})
        SELECT *
        FROM $tmpTbl
        """.stripMargin
    spark.sql(insertStmt)
  }

  def load(multiline: String, delimiter: String, header: String, encoding: String, srcPath: String): DataFrame = {
    spark.read
      .option("multiLine", multiline)
      .option("delimiter", delimiter)
      .option("header", header)
      .option("escape", "\"")
      //.option("encoding", "UTF-8")
      .option("encoding", encoding)
      .csv(srcPath)
  }

  def getTimestampformat(col: String, timestampFormat: String) = {

    if (!timestampFormat.isEmpty && timestampFormat.contains("#")) {
      val pairs = timestampFormat.split("#").map(t => t.split("->")).map {
        case Array(k, v) => k -> v
      }.toMap
      pairs(col)
    } else if (!timestampFormat.isEmpty) {
      timestampFormat.split("->")(1)
    } else timestampFormat
  }

  def convertTypes(df: DataFrame, conversions: String, timestampFormat: String): DataFrame = {
    if (!conversions.isEmpty) {
      conversions.toLowerCase.split("#").map(t => t.split("->")).foldLeft(df) {
        (cdf, toType) =>
          val c = toType(0).trim()
          val ctype = toType(1).trim()
          if (ctype.toLowerCase() == "timestamp") {
            val ts = unix_timestamp(col(c), getTimestampformat(c, timestampFormat).toString).cast("timestamp")
            cdf.withColumn(c, ts)
          } else {
            cdf.withColumn(c, col(c).cast(ctype))
          }
      }
    } else {
      df
    }
  }

  def addPartitionColumnToDataframe(df: DataFrame, srcPath: String, parrtitionColumnSql: String, partitionColumnName: String): DataFrame = {
    if (parrtitionColumnSql.toLowerCase() == "ingestion_date") {
      df.withColumn(
        partitionColumnName,
        lit(createPartitionsIngestionDate(srcPath)))
    } else {
      df.withColumn(
        partitionColumnName,
        expr(parrtitionColumnSql))
    }
  }

  def createHiveTableFromDataframe(df: DataFrame, partitionColumnName: String, fullyQualifiedDestinationTableName: String, destinationPath: String) {
    val fields = df.schema.fields.filterNot(
      _.name
        .trim()
        .equalsIgnoreCase(partitionColumnName)) //:+ partitionField
    val bdy =
      fields.map(f => s"${f.name}  ${f.dataType.catalogString}").mkString(",\n")

    val stmt =
      s"""
      CREATE EXTERNAL TABLE IF NOT EXISTS  ${fullyQualifiedDestinationTableName} (
       $bdy )
       PARTITIONED BY ( ${partitionColumnName} string)
       STORED AS PARQUET
       LOCATION '${destinationPath}'
      """
    println(stmt.trim())
    spark.sql(stmt)
  }

}

object Ingestion {
  def apply(
    spark: SparkSession,
    jobConf: JobConfig1): Ingestion =
    new Ingestion(spark, jobConf)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("nyc-taxi")
      .enableHiveSupport()
      .getOrCreate()

    val jobConf = new ConfigParser1(args).getJobConf

    Ingestion(spark, jobConf).run()
  }

}
