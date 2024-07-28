import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, count, date_format, datediff, lower, max, min, to_date, when}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.types.DateType
object sparkDFAssignment {
  def main(args: Array[String]): Unit = {

        val spark = SparkSession.builder()
          .appName("spark-program")
          .master("local[*]")
          .getOrCreate()

    import spark.implicits._

    val data=List(("mohan",78),("Ajay",90),("veer",76)).toDF("Name","Age")
    print("Nishad")
    data.show()

    val employees = List(
      (1, 25, 30000),
      (2, 45, 50000),
      (3, 35, 40000)
    ).toDF("employee_id", "age", "salary")

    employees.withColumn("Details",when(col("age")===25,"Age is 25").
      otherwise("Age is not 25")
    ).show()
    print("Nishad")

    val scoreData = Seq(
      ("Alice", "Math", 80),
      ("Bob", "Math", 90),
      ("Alice", "Science", 70),
      ("Bob", "Science", 85),
      ("Alice", "English", 75),
      ("Bob", "English", 95)
    ).toDF("Student", "Subject", "Score")

    scoreData.groupBy(col("Student")).agg( avg(col("score")).alias("Average") , max(col("score")) ).show()
    print("Nishad")
  val ratingsData = Seq(
      ("User1", "Movie1", 4.5),
      ("User2", "Movie1", 3.5),
      ("User3", "Movie2", 2.5),
      ("User4", "Movie2", 3.0),
      ("User1", "Movie3", 5.0),
      ("User2", "Movie3", 4.0)
    ).toDF("User", "Movie", "Rating")

    ratingsData.groupBy(col("Movie")).agg( avg(col("Rating")) , count(col("Rating")) ).show()
    print("Nishad")
    val weatherData = Seq(
      ("City1", "2022-01-01", 10.0),
      ("City1", "2022-01-02", 8.5),
      ("City1", "2022-01-03", 12.3),
      ("City2", "2022-01-01", 15.2),
      ("City2", "2022-01-02", 14.1),
      ("City2", "2022-01-03", 16.8)
    ).toDF("City", "Date","Temperature")

    weatherData.groupBy(col("City")).agg( min(col("Temperature")) , max(col("Temperature")) , avg(col("Temperature")) ).show()
    print("Nishad")



    //
//    employees.withColumn("Category",when(col("age")<30 //&& col("salary")<35000
//      ,"Young  & Low Salary")
//      //.when (col("age")>=30 && col("age")<=40 && col("salary")>=35000 && col("salary")<=45000,"Middle aged & Medium Salary")
//      .otherwise("Old & High Salary")
//    )
    //employees.show()


//        val df = spark.read
//          .format("csv")
//          .option("header",true)
//          .option("path","C:/Users/nnarse/Desktop/RDD/SPARKSQL/demo.csv")
//          .load()
//
//        df.show()
//
//    val schema_employee=StructType(List(
//      StructField("id",IntegerType,nullable =false),
//      StructField("name",StringType,nullable =false),
//      StructField("age",IntegerType,nullable =false)
//    ))
//
//    val employeeDF=spark.read
//      .format("csv")
//      .option("header",true)
//      .schema(schema_employee)
//      .option("path","C:/Users/nnarse/Desktop/RDD/DF/1.csv")
//      .load()
//
//    employeeDF.show()
//    employeeDF.withColumn("is_adult",when(col("age")>=18,"true").otherwise("false")).show();
//
///////////////////////////
//
//    val schema_student=StructType(List(
//      StructField("student_id",IntegerType,nullable =false),
//      StructField("score",IntegerType,nullable =false)
//    ))
//
//    val studentDF=spark.read
//      .format("csv")
//      .option("header",true)
//      .schema(schema_student)
//      .option("path","C:/Users/nnarse/Desktop/RDD/DF/2.csv")
//      .load()
//
//    studentDF.withColumn("grade",when(col("score")>=50,"Pass").otherwise("Fail")).show();
//
//    ///////////////////////////
//
//
//    val schema_transaction=StructType(List(
//      StructField("transaction_id",IntegerType,nullable =false),
//      StructField("amount",IntegerType,nullable =false)
//    ))
//
//    val transactionDF=spark.read
//      .format("csv")
//      .option("header",true)
//      .schema(schema_transaction)
//      .option("path","C:/Users/nnarse/Desktop/RDD/DF/3.csv")
//      .load()
//
//    // employeeDF.show()
//    transactionDF.withColumn("indicator",when(col("amount")>1000,"High")
//                                        .when(col("amount")>=500 && col("amount")<=1000,"Medium")
//                                        .otherwise("Low")).show();
//////////////////////
//
//
//    val schema_product=StructType(List(
//      StructField("product_id",IntegerType,nullable =false),
//      StructField("price",DoubleType,nullable =false)
//    ))
//
//    val productDF=spark.read
//      .format("csv")
//      .option("header",true)
//      .schema(schema_product)
//      .option("path","C:/Users/nnarse/Desktop/RDD/DF/4.1.csv")
//      .load()
//
//    // employeeDF.show()
//    productDF.withColumn("price_range",when(col("price")<50,"Cheap")
//      .when(col("price")>=50 && col("price")<=100,"Moderate")
//      .otherwise("Expensive")).show();
/////////////
//
//
//    val schema_event=StructType(List(
//      StructField("event_id",IntegerType,nullable =false),
//      StructField("date",DateType,nullable =false)
//    ))
//
//    val eventDF=spark.read
//      .format("csv")
//      .option("header",true)
//      .schema(schema_event)
//      .option("path","C:/Users/nnarse/Desktop/RDD/DF/5.csv")
//      .load()
//
//      eventDF.withColumn("is_holiday",when(col("date") === "2024-12-25" || col("date") === "2025-01-01"  ,"true")
//      .otherwise("false")).show();

    val inventory = List(
      (1, 5),
      (2, 15),
      (3, 25)
    ).toDF("item_id", "quantity")


    inventory.select (col("item_id") , col("quantity") ,
      when (col("quantity")<10 , "Low")
        .when(col("quantity")>=10 && col("quantity")<=20 , "Medium")
        .otherwise("High").alias("stock_level")
    ).show()


    val customers = List(
      (1, "john@gmail.com"),
      (2, "jane@Yahoo.com"),
      (3, "doe@hotmail.com")
    ).toDF("customer_id", "email")


    customers.select (col("customer_id") , col("email") ,
      when (col("email").contains("gmail"), "GMAIL")
        .when(lower(col("email")).contains("yahoo"), "YAHOO")
        .otherwise("OTHER").alias("stock_level")
    ).show()


    val orders = List(
      (1, "2024-07-01"),
      (2, "2024-12-01"),
      (3, "2024-05-01")
    ).toDF("order_id", "order_date")

    orders.select (col("order_id") , col("order_date") ,
     when(date_format(to_date(col("order_date"),"yyyy-MM-dd"),"MMMMM").isin(List("June","July","August"):_*),"Summer")
       .when(date_format(to_date(col("order_date"),"yyyy-MM-dd"),"MMMMM").isin(List("December","January","February"):_*),"Winter")
       .otherwise("Other")
       .alias("Season")
    ).show()


    val logins = List(
      (1, "09:00"),
      (2, "18:30"),
      (3, "14:00")
    ).toDF("login_id", "login_time")

    logins.select (col("login_id") , col("login_time") ,
      when(col("login_time").substr(0,2)<=12,"True").otherwise("False")
        .alias("is_morning"))
    .show()

    val employees1 = List(
      (1, 25, 30000),
      (2, 45, 50000),
      (3, 35, 40000)
    ).toDF("employee_id", "age", "salary")

    employees1.select (col("employee_id") , col("age") , col("salary"),
      when(col("age")<30 && col("salary")<35000,"Young  & Low Salary")
      .when (col("age")>=30 && col("age")<=40 && col("salary")>=35000 && col("salary")<=45000,"Middle aged & Medium Salary")
        .otherwise("Old and High Salary")
          .alias("Category"))
      .show()

    val reviews = List(
      (1, 1),
      (2, 4),
      (3, 5)
    ).toDF("review_id", "rating")

    reviews.select (col("review_id") , col("rating") ,
        when(col("rating")<3,"Bad")
          .when (col("rating")===3 || col("rating")===4,"Good")
          .when (col("rating")===5,"Excellent")
          .otherwise("EXCEPTION")
          .alias("Feedback"),
        when(col("rating")>=3,"True").otherwise("False").alias("is_positive")
      )
      .show()

    val payments = List(
      (1, "2024-07-15"),
      (2, "2024-12-25"),
      (3, "2024-11-01")
    ).toDF("payment_id", "payment_date")

    payments.select (col("payment_id") , col("payment_date") ,
      when(date_format(to_date(col("payment_date"),"yyyy-MM-dd"),"MMMMM").isin(List("January","February","March"):_*),"Q1")
        .when(date_format(to_date(col("payment_date"),"yyyy-MM-dd"),"MMMMM").isin(List("April","May","June"):_*),"Q2")
        .when(date_format(to_date(col("payment_date"),"yyyy-MM-dd"),"MMMMM").isin(List("July","August","September"):_*),"Q3")
        .otherwise("Q4")
        .alias("Quarter")
    ).show()



    val emails = List(
      (1, "user@gmail.com"),
      (2, "admin@yahoo.com"),
      (3, "info@hotmail.com")
    ).toDF("email_id", "email_address")


      emails.select (col("email_id") , col("email_address") ,
      when (col("email_address").contains("gmail"), "Gmail  ")
        .when(lower(col("email_address")).contains("yahoo"), "Yahoo")
        .otherwise("Hotmail").alias("email_domain")
    ).show()


    val scores = List(
      (1, 85, 92),
      (2, 58, 76),
      (3, 72, 64)
    ).toDF("student_id", "math_score", "english_score")

    scores.select (col("student_id") , col("math_score") , col("english_score"),
      when(col("math_score")>80,"A").when(col("math_score").between(60,80),"B").otherwise("C").alias("math_grade"),
        when(col("english_score")>80,"A").when(col("english_score").between(60,80),"B").otherwise("C").alias("english_grade")
      )
    .show()


    val weather = Seq(
      (1, 25, 60),
      (2, 35, 40),
      (3, 15, 80)
    ).toDF("day_id", "temperature", "humidity")

    weather.select (col("day_id") , col("temperature") , col("humidity"),
        when(col("temperature")>30,"true").otherwise("false").alias("is_hot"),
        when(col("humidity")>30,"true").otherwise("false").alias("is_humid")
      )
      .show()

    val tasks = List(
      (1, "2024-07-01", "2024-07-10"),
      (2, "2024-08-01", "2024-08-15"),
      (3, "2024-09-01", "2024-09-05")
    ).toDF("task_id", "start_date", "end_date")

    tasks.select (col("task_id") , col("start_date") , col("end_date"),
        datediff(col("end_date"),col("start_date")).alias("No of days"),
        when(datediff(col("end_date"),col("start_date"))<=7,"Short").
          when(datediff(col("end_date"),col("start_date")).between(7,14),"Medium").
          otherwise("Long").alias("task_duration")
      )
      .show()

    val documents = List(
      (1, "The quick brown fox"),
      (2, "Lorem ipsum dolor sit amet"),
      (3, "Spark is a unified analytics engine")
    ).toDF("doc_id", "content")


    documents.select (col("doc_id") , col("content") ,
          when(col("content").contains("fox"),"Animal Related").
          when(lower(col("content")).contains("lorem"),"Placeholder Text").
          otherwise("Tech related").alias("Content Category")
      )
      .show()



  }








}
