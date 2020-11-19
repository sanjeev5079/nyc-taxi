package se.telenor.aep.dataplatform

import java.io.File

import org.scalatest.{ BeforeAndAfter, FunSuite, Matchers }
import com.holdenkarau.spark.testing.{ DataFrameSuiteBase, SharedSparkContext }
import org.apache.spark.sql.{ DataFrame, SparkSession }
import org.apache.spark.SparkConf
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION
import se.telenor.aep.dataplatform.DataErasure

class DataErasureTest extends FunSuite
  with SharedSparkContext
  with DataFrameSuiteBase {

  override def conf: SparkConf = super.conf.set(CATALOG_IMPLEMENTATION.key, "hive")
  //override implicit def enableHiveSupport: Boolean = false
  //Below setup is to by-pass error: access denied org.apache.derby.security.SystemPermission( "engine", "usederbyinternals" )
  System.setSecurityManager(null)

  //instantiate object
  val de = DataErasure
  val testDb = "test_db"
  val testDbLocation = "/tmp/spark/test_db.db"
  val assumedCurrentRunDate = "2020-11-14"

  override def beforeAll() {
    super.beforeAll()
    //Set up database.
    spark.sql(s"drop database if exists $testDb cascade")
    spark.sql(s"create database $testDb location '$testDbLocation'")
    spark.sparkContext.setLogLevel("ERROR")

    //Setup blacklist table
    import spark.implicits._
    val blacklistAccessTestDf = Seq(
      ("2020-11-13 00:00:00", "5566377601", "10000001", "000526646", "71568786268", "20201114104510_kzoVv", "2020-11-14"),
      ("2020-11-13 00:00:00", "8499648962", "96471093", "001987283", "14137486064", "20201114104510_kzoVv", "2020-11-14"),
      ("2020-11-13 00:00:00", "7090155617", "93018863", "001083142", null, "20201114104510_kzoVv", "2020-11-14"),
      ("2020-11-12 00:00:00", "5566377602", "10000001", "006608809", "84988613380", "20201114104510_kzoVv", "2020-11-13"),
      ("2020-11-14 00:00:00", "0978774242", "10000004", "007627198", "85811052438", "20201114104510_kzoVv", "2020-11-15"))
      .toDF("timestamp", "org_pers_id", "account", "subs_id", "msisdn", "lineage", "ds")

    blacklistAccessTestDf.registerTempTable("bl_tmp")
    spark.sql(s"use $testDb")
    spark.sql("create table blacklist_access_test select * from bl_tmp")

    //Setup Account main table test data
    val AccountTestDf = Seq(
      ("2020-10-14 21:12:56", "10000001", "SHOULD BE DELETED", "2020-11-14"),
      ("2020-10-14 20:36:41", "10000000", "SHOULD BE KEPT", "2020-11-14"),
      ("2020-10-13 10:50:15", "10000001", "SHOULD BE DELETED", "2020-11-13"),
      ("2020-10-13 11:08:53", "10000000", "SHOULD BE KEPT", "2020-11-13"),
      ("2020-10-15 11:08:53", "10000003", "SHOULD BE KEPT", "2020-11-15"),
      ("2020-10-15 11:08:53", "10000001", "SHOULD BE KEPT", "2020-11-15"))
      .toDF("date_column", "account", "random_data", "ds")

    AccountTestDf.registerTempTable("account_tmp")
    spark.sql(s"use $testDb")
    spark.sql("create table account_test select * from account_tmp")

    //Setup org_pers_id main table test data
    val OrgPersIdTestDf = Seq(
      ("2020-10-14 21:12:56", "5566377601", "SHOULD BE DELETED", "2020-11-14"),
      ("2020-10-14 20:36:41", "5566377600", "SHOULD BE KEPT", "2020-11-14"),
      ("2020-10-13 10:50:15", "5566377602", "SHOULD BE DELETED", "2020-11-13"),
      ("2020-10-13 11:08:53", "5566377603", "SHOULD BE KEPT", "2020-11-13"),
      ("2020-10-15 11:08:53", "5566377604", "SHOULD BE KEPT", "2020-11-15"))
      .toDF("date_column", "org_id", "random_data", "ds")

    OrgPersIdTestDf.registerTempTable("orgid_tmp")
    spark.sql(s"use $testDb")
    spark.sql("create table orgid_test select * from orgid_tmp")
  }

  def mockExecuteDe(spark: SparkSession, db: String, blTbl: String, mainTbl: String, currRunDt: String): (DataFrame, String) = {
    val testBlDf = de.getBlacklist(spark, testDb, blTbl, currRunDt)
    val (testDf, testTblColumns) = de.getTableDataAndCols(spark, testDb, mainTbl, currRunDt, "")
    val testHighestOrderFilterCol = de.getHighestOrderFilterCol(testTblColumns)
    val testBlHighestOrderFilterCol = testHighestOrderFilterCol.get(0)
    val testTableHighestOrderFilterCol = testHighestOrderFilterCol.get(1)
    val testBlFilteringKeysDf = de.getBlFilteringKeys(testBlHighestOrderFilterCol, testBlDf)
    val (cleanDataDf, affectedPartitions) = de.getCleanData(testDf, testTableHighestOrderFilterCol, testBlFilteringKeysDf)

    (cleanDataDf, affectedPartitions)
  }

  test("1. Testing method : getBlacklist") {

    val expectedDf = spark.sql(s"""SELECT org_pers_id, account, subs_id, msisdn FROM $testDb.blacklist_access_test WHERE ds = "$assumedCurrentRunDate" """)
      .orderBy("org_pers_id", "account", "subs_id", "msisdn")
    val actualDf = de.getBlacklist(spark, testDb, "blacklist_access_test", assumedCurrentRunDate)
      .orderBy("org_pers_id", "account", "subs_id", "msisdn")

    assertDataFrameEquals(actualDf, expectedDf)
  }

  test("2. Testing method : getTableDataAndCols for account deletion") {

    //test 1: table containing account
    //when ds = "2020-11-14"
    val expectedAccountDf = spark.sql(s"""SELECT * FROM $testDb.account_test WHERE ds <= "$assumedCurrentRunDate" """)
      .orderBy("account")
    val expectedAccountTblColumns = expectedAccountDf.schema.fieldNames
    val (actualAccountDf, actualAccountTblColumns) = de.getTableDataAndCols(spark, testDb, "account_test", assumedCurrentRunDate, "")

    assertDataFrameEquals(actualAccountDf.orderBy("account"), expectedAccountDf)
    assert(actualAccountTblColumns.mkString(",") == expectedAccountTblColumns.mkString(","))

    //test 1: table containing org_pers_id
    //when ds = "2020-11-14"
    val expectedOrgidDf = spark.sql(s"""SELECT * FROM $testDb.orgid_test WHERE ds <= "$assumedCurrentRunDate" """)
      .orderBy("org_id")
    val expectedOrgidTblColumns = expectedOrgidDf.schema.fieldNames
    val (actualOrgidDf, actualOrgidTblColumns) = de.getTableDataAndCols(spark, testDb, "orgid_test", assumedCurrentRunDate, "")

    assertDataFrameEquals(actualOrgidDf.orderBy("org_id"), expectedOrgidDf)
    assert(actualOrgidTblColumns.mkString(",") == expectedOrgidTblColumns.mkString(","))

  }

  test("3. Testing method : getHighestOrderFilterCol") {

    val testTblColumns = spark.sql(s"""SELECT * FROM $testDb.orgid_test limit 1 """).schema.fieldNames
    val expectedHighestOrderFilterCol = de.getHighestOrderFilterCol(testTblColumns)

    assert(expectedHighestOrderFilterCol.get(0) == "org_pers_id")
    assert(expectedHighestOrderFilterCol.get(1) == "org_id")
  }

  test("4. Testing method : getBlFilteringKeys") {

    val expectedBlFilteringKeysDf = spark
      .sql(s"""SELECT DISTINCT(org_pers_id) FROM $testDb.blacklist_access_test WHERE ds = "$assumedCurrentRunDate" and org_pers_id is not null""").orderBy("org_pers_id")
    val testBlDf = spark.sql(s"""SELECT org_pers_id, account, subs_id, msisdn FROM $testDb.blacklist_access_test WHERE ds = "$assumedCurrentRunDate" """)
    val actualBlFilteringKeysDf = de.getBlFilteringKeys("org_pers_id", testBlDf).orderBy("org_pers_id")

    assertDataFrameEquals(actualBlFilteringKeysDf, expectedBlFilteringKeysDf)
  }

  test("5. Testing method : getCleanData on org_pers_id") {

    //test data erasure for org_pers_id

    //1. when current run date = '2020-11-14'

    //DO NOT DELETE below logic. It is a working query sample and can be alternatively used for testing.
    /*val expectedOrgidCleanDataDf = spark.sql(
      s"""
         |select e.* from
         |    (select d.* from $testDb.orgid_test d
         |    inner join
         |          (select distinct a.ds from $testDb.orgid_test a
         |          inner join
         |                (select distinct(org_pers_id)
         |                from $testDb.blacklist_access_test
         |                where ds = "$assumedCurrentRunDate") b
         |           on a.org_id = b.org_pers_id
         |           where  a.ds <= "$assumedCurrentRunDate") c
         |     on d.ds = c.ds ) e
         |left anti join $testDb.blacklist_access_test f
         |on e.org_id = f.org_pers_id              """.stripMargin)*/

    val expectedOrgidCleanDataDf = spark.sql(s"""select * from $testDb.orgid_test where org_id = "5566377600" """).orderBy("org_id")
    val (actualOrgidCleanDataDf, actualOrgidAffectedPartitions) =
      mockExecuteDe(
        spark,
        testDb,
        "blacklist_access_test",
        "orgid_test",
        assumedCurrentRunDate)

    assertDataFrameEquals(actualOrgidCleanDataDf, expectedOrgidCleanDataDf)
    assert(actualOrgidAffectedPartitions == "2020-11-14")

    //2. when current run date = '2020-11-13'
    val expectedOrgidCleanDataDf2 = spark.sql(s"""select * from $testDb.orgid_test where org_id = "5566377603" """).orderBy("org_id")
    val (actualOrgidCleanDataDf2, actualOrgidAffectedPartitions2) =
      mockExecuteDe(
        spark,
        testDb,
        "blacklist_access_test",
        "orgid_test",
        "2020-11-13")

    assertDataFrameEquals(actualOrgidCleanDataDf2, expectedOrgidCleanDataDf2)
    assert(actualOrgidAffectedPartitions2 == "2020-11-13")
  }

  test("6. Testing method : getCleanData on account") {

    //test data erasure for account
    //1. when current run date = '2020-11-14'
    val expectedAccCleanDataDf = spark.sql(s"""select * from $testDb.account_test where account = "10000000" """)
    val (actualAccCleanDataDf, actualAccAffectedPartitions) =
      mockExecuteDe(
        spark,
        testDb,
        "blacklist_access_test",
        "account_test",
        assumedCurrentRunDate)

    assertDataFrameEquals(actualAccCleanDataDf.orderBy("account"), expectedAccCleanDataDf.orderBy("account"))
    assert(actualAccAffectedPartitions == "2020-11-13,2020-11-14")

    //2. when current run date = '2020-11-15' -  No record should be returned for writing
    val expectedAccCleanDataDf2 = spark.sql(s"""select * from $testDb.account_test limit 0 """)
    val (actualAccCleanDataDf2, actualAccAffectedPartitions2) =
      mockExecuteDe(
        spark,
        testDb,
        "blacklist_access_test",
        "account_test",
        "2020-11-15")

    assertDataFrameEquals(actualAccCleanDataDf2.orderBy("account"), expectedAccCleanDataDf2.orderBy("account"))
    assert(actualAccAffectedPartitions2 == "")
  }

  override def afterAll() {
    // work around for spark 2.3.x https://github.com/holdenk/spark-testing-base/issues/234
    new File("metastore_db/db.lck").delete()
    new File("metastore_db/dbex.lck").delete()
    super.afterAll()
  }

}
