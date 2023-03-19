import Core.{SparkValueIdentifier, ValueIdentifier}
import IO.Reader.{FileReader, IFileReader}
import IO.Writer.FileWriter
import org.apache.spark.sql.SparkSession

 object OddValueIdentifier extends App {
  try {
    if (args.length < 3) {
      throw new IllegalArgumentException("Inform 3 parameters: input file, output file and credentials file.")
    }

    val inputFile = args(0)
    val outputFile = args(1)
    val credentialsFile = args(2)

    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("OddValueIdentifier")
      .getOrCreate()

    val fileReader: IFileReader = new FileReader(spark)
    val inputDf = fileReader.readFile(inputFile)

    val valueIdentifier: ValueIdentifier = new SparkValueIdentifier(spark)
    val oddValuesDf = valueIdentifier.IdentifyOddValues(inputDf)

    FileWriter.writeToFile(oddValuesDf, outputFile)
  } catch {
    case e: Exception => println(e.getMessage)
  }
}
