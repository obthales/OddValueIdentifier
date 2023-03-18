package Core

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.count

object SparkValueIdentifier extends ValueIdentifier {
  def IdentifyOddValues(df: DataFrame): DataFrame = {
    df.groupBy("key", "value")
      .agg(
        count("value").as("count")
      )
      .filter("count % 2 != 0")
      .select("key", "value")
  }
}
