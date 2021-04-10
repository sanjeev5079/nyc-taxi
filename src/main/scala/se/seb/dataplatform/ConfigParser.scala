package se.seb.dataplatform

import org.rogach.scallop.{ ScallopConf, ScallopOption }

class ConfigParser1(args: Seq[String]) extends ScallopConf(args) {
  val srcPath: ScallopOption[String] = opt[String](name = "srcPath", required = true)
  val destinationPath: ScallopOption[String] = opt[String](name = "destinationPath", required = true)
  val destinationDBName: ScallopOption[String] = opt[String](name = "destinationDBName", required = true)
  val destinationTableName: ScallopOption[String] = opt[String](name = "destinationTableName", required = true)
  val conversions: ScallopOption[String] = opt[String](name = "conversions", required = true)
  val partitionExpr: ScallopOption[String] = opt[String](name = "partitionExpr", required = true)
  val timestampFormat: ScallopOption[String] = opt[String](name = "timestampFormat", required = true)
  val numOfPartitions: ScallopOption[String] = opt[String](name = "numOfPartitions", required = true)
  val dataQuality: ScallopOption[String] = opt[String](name = "dataQuality", required = true)
  val multiline: ScallopOption[String] = opt[String](name = "multiline", required = true, default = Some("false"))
  val header: ScallopOption[String] = opt[String](name = "header", required = true, default = Some("true"))
  val encoding: ScallopOption[String] = opt[String](name = "encoding", required = true, default = Some("utf-8"))
  val delimiter: ScallopOption[String] = opt[String](name = "delimiter", required = true, default = Some(","))

  verify()

  def getJobConf: JobConfig1 = {
    JobConfig1(
      srcPath = this.srcPath(),
      destinationPath = this.destinationPath(),
      destinationDBName = this.destinationDBName(),
      destinationTableName = this.destinationTableName(),
      conversions = this.conversions(),
      partitionExpr = this.partitionExpr(),
      timestampFormat = this.timestampFormat(),
      numOfPartitions = this.numOfPartitions(),
      dataQuality = this.dataQuality(),
      multiline = this.multiline(),
      header = this.header(),
      encoding = this.encoding(),
      delimiter = this.delimiter())
  }
}

case class JobConfig1(
  srcPath: String,
  destinationPath: String,
  destinationTableName: String,
  destinationDBName: String,
  conversions: String,
  partitionExpr: String,
  timestampFormat: String,
  numOfPartitions: String,
  dataQuality: String,
  multiline: String,
  header: String,
  encoding: String,
  delimiter: String) {

  val fullyQualifiedDestinationTableName = s"${this.destinationDBName}.${this.destinationTableName}"

  val (partitionColumnName, parrtitionColumnSql) = {
    val parts = this.partitionExpr.split("#")

    if (parts.length != 2) {
      sys.error(
        s"expecting two parts of partitionColumnExpr got ${parts.length} when splitting with #.")
    }
    (parts(0).trim().toLowerCase(), parts(1))
  }

  //val numWritePartitions = if (this.numOfPartitions.equals("")) NumPartitionCalculator(destinationPath, fullyQualifiedDestinationTableName, partitionColumnName, run_date).numPartitions else numOfPartitions.toInt
}
