import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object four {

  def main(args: Array[String]): Unit = {

    val spark=SparkSession.builder()
      .appName("spark-program")
      .master("local[*]")
      .getOrCreate()


    //    val sparkconf=new SparkConf()
    //        sparkconf.set("spark.app.name","spark-program")
    //        sparkconf.set("spark.master","local[*]")
    //
    //      val spark=SparkSession.builder()
    //        .config(sparkconf)
    //        .getOrCreate()


    //    val schema=" id Int,Name String,Age Int,Salary Int,city String,details String,mean Int"

    val schema=StructType(List(

      StructField("id",IntegerType,nullable =false),
      StructField("Name",StringType,nullable =false),
      StructField("Age",IntegerType,nullable =false),
      StructField("Salary",IntegerType,nullable =false),
      StructField("city",StringType,nullable =false),
      StructField("details",StringType,nullable =false),
      StructField("mean",IntegerType,nullable =false),
      StructField("_corrupt_record",StringType,nullable =false)

    ))

    val df=spark.read
      .format("csv")
      .option("header",true)
      .schema(schema)
      .option("mode","PARMISSIVE")
      .option("path","C:/Users/nnarse/Desktop/RDD/SPARKSQL/info.csv")
//      .option("columnNameOfCorruptRecord","corrupt_record")
      .load()

    df.coalesce(2)

    df.show(false)
//    df.printSchema()
/*

    df.select(
      col("id")
      ,col("Name")
      ,col("Age"),
      when(col("age")>55,"pensionlable")
        .otherwise("notpensionable")
        .alias("details")
    ).show()
*/

    df.select(
      col("id")
      ,col("Name")
      ,col("Age"),
      when(col("age")<18,"Young")
        .when(col("age")>18 && col("age")<30,"Adult")
        .otherwise("Old")
        .alias("details")
    ).show()


    df.write
      .format("csv")
      .mode(SaveMode.Ignore)
      .option("path","C:/Users/nnarse/Desktop/RDD/SPARKSQL/july27op")
      .save()

  }
}