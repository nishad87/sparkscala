import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.types.DateType
object sparkDFAssignment {
  def main(args: Array[String]): Unit = {

        val spark = SparkSession.builder()
          .appName("spark-program")
          .master("local[*]")
          .getOrCreate()


        val df = spark.read
          .format("csv")
          .option("header",true)
          .option("path","C:/Users/nnarse/Desktop/RDD/SPARKSQL/demo.csv")
          .load()

        df.show()

    val schema_employee=StructType(List(
      StructField("id",IntegerType,nullable =false),
      StructField("name",StringType,nullable =false),
      StructField("age",IntegerType,nullable =false)
    ))

    val employeeDF=spark.read
      .format("csv")
      .option("header",true)
      .schema(schema_employee)
      .option("path","C:/Users/nnarse/Desktop/RDD/DF/1.csv")
      .load()

    employeeDF.show()
    employeeDF.withColumn("is_adult",when(col("age")>=18,"true").otherwise("false")).show();

/////////////////////////

    val schema_student=StructType(List(
      StructField("student_id",IntegerType,nullable =false),
      StructField("score",IntegerType,nullable =false)
    ))

    val studentDF=spark.read
      .format("csv")
      .option("header",true)
      .schema(schema_student)
      .option("path","C:/Users/nnarse/Desktop/RDD/DF/2.csv")
      .load()

    studentDF.withColumn("grade",when(col("score")>=50,"Pass").otherwise("Fail")).show();

    ///////////////////////////


    val schema_transaction=StructType(List(
      StructField("transaction_id",IntegerType,nullable =false),
      StructField("amount",IntegerType,nullable =false)
    ))

    val transactionDF=spark.read
      .format("csv")
      .option("header",true)
      .schema(schema_transaction)
      .option("path","C:/Users/nnarse/Desktop/RDD/DF/3.csv")
      .load()

    // employeeDF.show()
    transactionDF.withColumn("indicator",when(col("amount")>1000,"High")
                                        .when(col("amount")>=500 && col("amount")<=1000,"Medium")
                                        .otherwise("Low")).show();
////////////////////


    val schema_product=StructType(List(
      StructField("product_id",IntegerType,nullable =false),
      StructField("price",DoubleType,nullable =false)
    ))

    val productDF=spark.read
      .format("csv")
      .option("header",true)
      .schema(schema_product)
      .option("path","C:/Users/nnarse/Desktop/RDD/DF/4.1.csv")
      .load()

    // employeeDF.show()
    productDF.withColumn("price_range",when(col("price")<50,"Cheap")
      .when(col("price")>=50 && col("price")<=100,"Moderate")
      .otherwise("Expensive")).show();
///////////


    val schema_event=StructType(List(
      StructField("event_id",IntegerType,nullable =false),
      StructField("date",DateType,nullable =false)
    ))

    val eventDF=spark.read
      .format("csv")
      .option("header",true)
      .schema(schema_event)
      .option("path","C:/Users/nnarse/Desktop/RDD/DF/5.csv")
      .load()

      eventDF.withColumn("is_holiday",when(col("date") === "2024-12-25" || col("date") === "2025-01-01"  ,"true")
      .otherwise("false")).show();
  }

}
