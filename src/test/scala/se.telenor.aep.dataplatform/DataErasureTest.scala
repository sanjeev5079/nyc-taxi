package se.telenor.aep.dataplatform

import java.io.File

import org.scalatest.{ BeforeAndAfter, FunSuite, Matchers }
import com.holdenkarau.spark.testing.{ DataFrameSuiteBase, SharedSparkContext }
import org.apache.spark.SparkConf
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION

class DataErasureTest extends FunSuite
  with SharedSparkContext
  with DataFrameSuiteBase {

  override def conf: SparkConf = super.conf.set(CATALOG_IMPLEMENTATION.key, "hive")

  val testDbOperations = "operations_test"
  val testDbAccess = "access_test"
  val testDbOperationsLocation = "/tmp/spark/operations_test.db"
  val testDbAccessLocation = "/tmp/spark/access_test.db"

  override def beforeAll() {
    super.beforeAll()
    spark.sql(s"drop database if exists $testDbOperations cascade")
    spark.sql(s"drop database if exists $testDbAccess cascade")

    spark.sql(s"create database $testDbOperations location '$testDbOperationsLocation'")
    spark.sql(s"create database $testDbAccess location '$testDbAccessLocation'")
    spark.sparkContext.setLogLevel("ERROR")
  }

  test("table creation") {
    import spark.implicits._
    val someDF = Seq(
      ("ted", 42, "blue"),
      ("tj", 11, "green"),
      ("andrew", 9, "green")).toDF("name", "age", "color")

    someDF.registerTempTable("tempPerson")

    spark.sql("show databases").show
    spark.sql("select * from tempPerson").show

    assert(1 == 1)

  }

  override def afterAll() {
    // work around for spark 2.3.x https://github.com/holdenk/spark-testing-base/issues/234
    new File("metastore_db/db.lck").delete()
    new File("metastore_db/dbex.lck").delete()
    super.afterAll()
  }

}
