import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

 object OddValueIdentifier extends App {
  try {
    if (args.length < 3) {
      throw new IllegalArgumentException("Inform 3 parameters: input file, output file and credentials file.")
    }

    val inputFile = args(0)
    val outputFile = args(1)
    val credentialsFile = args(2)

    //      val conf = new SparkConf().setAppName("OddValueIdentifier").setMaster("local[1]")
    //      val sc = SparkContext.getOrCreate(conf)
    //
          val spark = SparkSession.builder()
            .master("local[1]")
            .appName("OddValueIdentifier")
            .getOrCreate();

  } catch {
    case e: Exception => println(e.getMessage)
  }
}
