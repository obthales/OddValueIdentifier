package IO.Writer

import java.io.PrintWriter
import org.apache.spark.sql.{DataFrame, SaveMode}

trait IFileWriter {
  def writeToFile(df: DataFrame, filePath: String)
}

object FileWriter extends IFileWriter {
  def writeToFile(df: DataFrame, filePath: String) = {

    // This should be the better way to write but I got problem for java not finding hadoop libraries locally although the path is set on System Environment
//    df.write
//      .mode(SaveMode.Overwrite)
//      .option("delimiter", "\t")
//      .csv(filePath)

    new PrintWriter(filePath) {
      df.collect()
        .foreach { row =>
        write(row.mkString("\t") + "\n")
      }
      close()
    }
  }
}