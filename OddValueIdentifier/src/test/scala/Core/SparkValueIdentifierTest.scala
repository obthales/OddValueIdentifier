package Core

import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SparkValueIdentifierTest extends AnyFlatSpec with Matchers {

  private val spark = SparkSession.builder()
  .master("local[1]")
  .appName("SparkValueIdentifierTest")
  .getOrCreate()

  import spark.implicits._

  private val testDf = Seq((1, 2), (1, 3), (1, 3), (2, 4), (2, 4), (2, 4)).toDF("key", "value")
  private val expectedDf = Seq((1, 2), (2, 4)).toDF("key", "value").collect()

  private val valueIdentifier = new SparkValueIdentifier(spark)

  "SparkValueIdentifier" should "return the odd values for each key" in {
    val result = valueIdentifier.IdentifyOddValues(testDf).collect()

    result(0)(0) shouldBe expectedDf(0)(0)
    result(0)(1) shouldBe expectedDf(0)(1)
    result(1)(0) shouldBe expectedDf(1)(0)
    result(1)(1) shouldBe expectedDf(1)(1)
  }
}
