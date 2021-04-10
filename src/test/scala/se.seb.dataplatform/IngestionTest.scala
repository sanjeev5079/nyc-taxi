package se.seb.dataplatform

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ DecimalType, TimestampType, DoubleType, IntegerType }
import org.scalatest.{ BeforeAndAfter, FunSuite, Matchers }

case class Schema(
  name: String,
  age: String,
  address: String,
  commision: String,
  salary: String,
  commision_datetime: String)

class IngestionTest extends FunSuite with BeforeAndAfter with Matchers {

  val spark = SparkSession.builder()
    .appName("Ingestion_test")
    .config("spark.master", "local")
    .getOrCreate()

  val args = Array("--srcPath=", "--destinationPath=", "--destinationDBName=", "--destinationTableName=", "--conversions=", "--partitionExpr=ds#ingestion_date", "--timestampFormat=", "--numOfPartitions=", "--dataQuality=", "--multiline=", "--header=", "--encoding=", "--delimiter=")
  val jobConf = new ConfigParser1(args).getJobConf
  val si = Ingestion(spark, jobConf)

  val testRDD = spark.sparkContext.textFile("src/test/resources/TestDataFile_20200817000000")
  import spark.implicits._
  val testDfRaw = spark.read
    .option("multiLine", "false")
    .option("delimiter", ",")
    .option("header", "true")
    .option("escape", "\"")
    .option("encoding", "UTF-8")
    .csv("src/test/resources/TestDataFile_20200817000000")

  val testDf = testDfRaw
    .withColumn("age", col("age").cast(IntegerType))
    .withColumn("commission", col("commission").cast(DoubleType))
    .withColumn("salary", col("salary").cast(DecimalType(12, 2)))
    .withColumn("commission_datetime", unix_timestamp(col("commission_datetime"), "yyyy-MM-dd HH:mm:ss").cast("timestamp"))

  val testDfWithPartitionCol = testDf.withColumn("ds", when(lit(1).isNotNull, lit("2020-08-17")).otherwise(lit(null)))
  val testDfWithDynamicPartitionCol = testDf
    .withColumn(
      "ds_dynamic",
      expr("date_format(cast(commission_datetime as timestamp),'yyyy-MM-dd')"))

  test("load data into a data frame") {
    val expectedSchema = testDfRaw.schema
    println(expectedSchema)
    val originalSchema = si.load("false", ",", "true", "utf-8", "src/test/resources/TestDataFile_20200817000000").schema
    assert(originalSchema == expectedSchema)
    si.load("false", ",", "true", "utf-8", "src/test/resources/TestDataFile_20200817000000")
      .collect() should contain theSameElementsAs testDfRaw.collect()

  }

  test("data type conversion") {
    //branch - if true
    var expectedSchema = testDf.schema
    val conversion = "age->integer#commission->double#salary->decimal(12,2)#commission_datetime->timestamp"
    val timestampFormat = "commission_datetime->yyyy-MM-dd HH:mm:ss"
    var originalSchema = si.convertTypes(testDfRaw, conversion, timestampFormat).schema
    assert(originalSchema == expectedSchema)
    //branch - else
    expectedSchema = testDfRaw.schema
    originalSchema = si.convertTypes(testDfRaw, "", "").schema
    assert(originalSchema == expectedSchema)
  }

  test("timestamp format") {
    //branch - if true
    var originalFormat = si.getTimestampformat("test_fld", "commission_datetime->yyyy-MM-dd HH:mm:ss#test_fld->yyyy-MM-dd HH")
    assert(originalFormat == "yyyy-MM-dd HH")
    //branch - else if
    originalFormat = si.getTimestampformat("commission_datetime", "commission_datetime->yyyy-MM-dd HH:mm:ss")
    assert(originalFormat == "yyyy-MM-dd HH:mm:ss")
    //branch - else
    originalFormat = si.getTimestampformat("commission_datetime", "")
    assert(originalFormat == "")

  }

  test("extract lineage date") {
    val input = "RAW/EXPORTS/NYC_TAXI/ingestion_date=2020-07-01/lineage=20200713102159_bchid/TAXI.csv"
    val lineage_dt = si.funLineageDate(input)
    assert(lineage_dt == "20200713102159_bchid")
  }

  test("get hive partition/ingestion_date from file path") {
    val input = "RAW/EXPORTS/NYC_TAXI/ingestion_date=2020-07-01/lineage=20200713102159_bchid/TAXI.csv"
    val partitionValue = si.createPartitionsIngestionDate(input)
    assert(partitionValue == "2020-07-01")
  }

  test("run") { pending }

}