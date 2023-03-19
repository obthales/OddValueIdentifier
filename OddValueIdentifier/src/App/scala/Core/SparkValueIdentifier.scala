package Core

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.count

class SparkValueIdentifier(spark: SparkSession) extends ValueIdentifier {

  def IdentifyOddValues(df: DataFrame): DataFrame = {
    import spark.implicits._

    df.collect().toSeq.map { row =>
        val key = if (row(0) == null) 0 else row(0).toString.toInt
        val value = if (row(1) == null) 0 else row(1).toString.toInt

        (key, value)
      }
      .toDF("key", "value")
      .groupBy("key", "value")
      .agg(
        count("value").as("count")
      )
      .filter("count % 2 != 0")
      .select("key", "value")
  }
}
