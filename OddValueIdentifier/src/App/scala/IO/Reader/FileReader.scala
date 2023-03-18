package IO.Reader

import java.nio.file.Paths
import org.apache.spark.sql.{DataFrame, SparkSession}

trait IFileReader {
  def readFile(filePath: String): DataFrame
}

class FileReader(sc: SparkSession) extends IFileReader {
  def readFile(filePath: String): DataFrame = {
    val fileName = Paths.get(filePath).getFileName
    val extension = fileName.toString.split("\\.").last

    extension match {
      case "csv" => sc.read.csv(filePath)
      case "tsv" => sc.read.format("com.databricks.spark.csv").option("delimiter", "\t").load(filePath)
      case _ => throw new IllegalArgumentException("Unsupported file format.")
    }
  }
}