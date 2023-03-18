package Core

import org.apache.spark.sql.DataFrame

trait ValueIdentifier {
  def IdentifyOddValues(df: DataFrame): DataFrame
}
