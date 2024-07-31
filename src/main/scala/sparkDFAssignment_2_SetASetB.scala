import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
object sparkDFAssignment_2_SetASetB {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("spark-program")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val students = List(
      (1, "Alice", 92, "Math"),
      (2, "Bob", 85, "Math"),
      (3, "Carol", 77, "Science"),
      (4, "Dave", 65, "Science"),
      (5, "Eve", 50, "Math"),
      (6, "Frank", 82, "Science")
    ).toDF("student_id", "name", "score", "subject")


    val window = Window.partitionBy("subject")

    students.select(col("student_id"), col("name"), col("score"), col("subject"),
        when(col("score") >= 90, "A").
          when(col("score").between(80, 89), "B").
          when(col("score").between(70, 79), "C").
          when(col("score").between(60, 69), "D").
          otherwise("F").alias("Grade")
      )
      .show()

    print("2")
    students.select(col("subject"), avg("score").over(window).alias("Average Score"),
      min("score").over(window).alias("Average Score").alias("Min Score"),
      max("score").over(window).alias("Average Score").alias("Max Score")
    ).distinct().show()

    val student_grade_df = students.withColumn("Grade",
      when(col("score") >= 90, "A").
        when(col("score").between(80, 89), "B").
        when(col("score").between(70, 79), "C").
        when(col("score").between(60, 69), "D").
        otherwise("F"))
    student_grade_df.show()

    val window_student_grade = Window.partitionBy("Grade", "Subject")
    student_grade_df.select(col("Grade"), col("Subject"), count("Grade").over(window_student_grade).alias("Count per grade and subject")).show()


    val product_df = List(
      (1, "Smartphone", 700, "Electronics"),
      (2, "TV", 1200, "Electronics"),
      (3, "Shoes", 150, "Apparel"),
      (4, "Socks", 25, "Apparel"),
      (5, "Laptop", 800, "Electronics"),
      (6, "Jacket", 200, "Apparel")
    ).toDF("product_id", "product_name", "price", "category")

    val enh_product_df = product_df.select(col("product_id"), col("product_name"), col("price"), col("category"),
        when(col("price") > 500, "Expensive").
          when(col("price").between(200, 500), "Moderate").
          otherwise("Cheap").
          alias("Grade")
      )

    val product_df_startsWithS = product_df.filter(col("product_name").startsWith("S"));
    product_df_startsWithS.show()


    val product_df_endsWithS = product_df.filter(col("product_name").endsWith("s"));
    product_df_endsWithS.show()


    enh_product_df.select(col("category"),
        min("price").over(Window.partitionBy("category")).alias("Min price"),
      max("price").over(Window.partitionBy("category")).alias("Min price"),
      sum("price").over(Window.partitionBy("category")).alias("Min price"),
      avg("price").over(Window.partitionBy("category")).alias("Min price")
    ).distinct().show()





  }
}
