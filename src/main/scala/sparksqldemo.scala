import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object sparksqldemo {
  def main(args: Array[String]): Unit = {
/*
    val spark = SparkSession.builder()
      .appName("spark-program")
      .master("local(*)")
      .getOrCreate()
*/

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name","spark-program")
    sparkconf.set("spark.master","local[*]")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    val df = spark.read
      .format("csv")
      .option("header",true)
      .option("path","C:/Users/nnarse/Desktop/RDD/SPARKSQL/demo.csv")
      .load()

    df.show()
  }

}
