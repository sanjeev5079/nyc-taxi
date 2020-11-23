package se.telenor.aep.dataplatform

import com.typesafe.config.Config

class ConfParser(appConf: Config) {
  def getConf: JobConfig = {
    JobConfig(
      appConf.getString("erasure.blacklistFileBasePath"),
      appConf.getString("erasure.currentRunDate"),
      appConf.getString("erasure.prevSucessRunDate"),
      appConf.getString("erasure.db"),
      appConf.getString("erasure.table"),
      appConf.getString("erasure.joinQueryToBuildTable"))
  }
}

case class JobConfig(
  blacklistFileBasePath: String,
  currentRunDate: String,
  prevSuccessRunDate: String,
  erasureDb: String,
  erasureTable: String,
  joinQueryToBuildTable: String)

object ConfParser {
  def apply(appConf: Config): ConfParser = new ConfParser(appConf)
}