import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
object DataFramesProblemForPractice {

  def employeePerformanceAnalysis(df: DataFrame): Unit = {
    val enhancedDF = df.
      withColumn("amount_category",
        when(col("amount") >= 1000, "High").when(col("amount").between(500, 1000), "Medium").otherwise("Low")).
      withColumn("transaction_month", date_format(to_date(col("transaction_date")), "MMMMM"))
    enhancedDF.show()
    val filteredDFDecMonth = enhancedDF.filter(col("transaction_month")==="December")
    filteredDFDecMonth.show()
    //val filteredDFEnd = enhancedDF.filter(col("movie_name").endsWith("e"))
    //filteredDFEnd.show()
    val windowSpec = Window.partitionBy("transaction_type")
    enhancedDF.select(col("transaction_type"), sum("amount").over(windowSpec).alias("Total")
      , avg("amount").over(windowSpec).alias("Average")
      , min("amount").over(windowSpec).alias("Minimum")
      , max("amount").over(windowSpec).alias("Maximum")).distinct().show()

  }


  def transactionAmountAndDataAnalysis(df: DataFrame): Unit = {
    val enhancedDF = df.
      withColumn("amount_category",
        when(col("amount") >= 1000, "High").when(col("amount").between(500, 1000), "Medium").otherwise("Low")).
      withColumn("transaction_month", date_format(to_date(col("transaction_date")), "MMMMM"))
    enhancedDF.show()
    val filteredDFDecMonth = enhancedDF.filter(col("transaction_month")==="December")
    filteredDFDecMonth.show()
    //val filteredDFEnd = enhancedDF.filter(col("movie_name").endsWith("e"))
    //filteredDFEnd.show()
    val windowSpec = Window.partitionBy("transaction_type")
    enhancedDF.select(col("transaction_type"), sum("amount").over(windowSpec).alias("Total")
      , avg("amount").over(windowSpec).alias("Average")
      , min("amount").over(windowSpec).alias("Minimum")
      , max("amount").over(windowSpec).alias("Maximum")).distinct().show()

  }


  def customerFeedbackAnalysis(df: DataFrame): Unit = {
    val enhancedDF = df.
      withColumn("rating_category",
        when(col("rating") >= 5, "Excellent").when(col("rating").between(3, 5), "Good").otherwise("Poor"))
      .withColumn("Rating_Month",date_format(to_date(col("feedback_date")),"MMMMM"))
    enhancedDF.show()

    val filteredFeedbacks = enhancedDF.filter(col("feedback_text").startsWith("Great"))
    filteredFeedbacks.show()
    val windowSpec = Window.partitionBy("rating_Month")
    enhancedDF.select(  col("Rating_Month"),
      avg("rating").over(windowSpec).alias("Average")
    ).distinct().show()

  }

  def productSalesAnalysis(df: DataFrame): Unit = {

    val enhancedDF = df.
      withColumn("sales_category",
        when(col("sale_amount") >= 500, "High").when(col("sale_amount").between(200, 500), "Medium").otherwise("Low"))
      .withColumn("MonthSales",date_format(to_date(col("sale_date"),"yyyy-MM-dd"),"MMMMM YYYY"))

    val filteredSales = enhancedDF.filter(col("MonthSales") === "January 2024")
    filteredSales.show()

    val windowSpec = Window.partitionBy("MonthSales")

    enhancedDF.withColumn("Sum",
        sum("sale_amount").over(windowSpec))
      .withColumn("Average",
        avg("sale_amount").over(windowSpec))
      .withColumn("Min",
        min("sale_amount").over(windowSpec))
      .withColumn("Max",
        max("sale_amount").over(windowSpec))
      .show()


  }

  def projectBudgetTracking(df:DataFrame):Unit={
    val window_spec = Window.partitionBy("budget status")

    val winDF = df.withColumn("budget status",when(col("spent_amount")>col("budget"),"Over Budget")
        .when(col("spent_amount")===col("budget"),"On Budget")
        .otherwise("Under Budget"))
      .withColumn("Sum",sum("spent_amount") over(window_spec))
      .withColumn("Avg",avg("spent_amount") over(window_spec))
      .withColumn("Max",max("spent_amount") over(window_spec))
      .withColumn("Min",min("spent_amount") over(window_spec))

    winDF.show()

    val filteredDF = winDF.filter(col("project_name").startsWith("New")).select("project_id","project_name","budget","spent_amount")
    filteredDF.show()



  }



  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("spark-program")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    /*
        val ratingData = Seq(
          ("User1", "Movie1", 4.5),
          ("User1", "Movie2", 3.5),
          ("User1", "Movie3", 2.5),
          ("User1", "Movie4", 4.0),
          ("User1", "Movie5", 3.0),
          ("User1", "Movie6", 4.5),

          ("User2", "Movie1", 3.0),
          ("User2", "Movie2", 4.0),
          ("User2", "Movie3", 4.5),
          ("User2", "Movie4", 3.5),
          ("User2", "Movie5", 4.0),
          ("User2", "Movie6", 3.5)
        ).toDF("User", "Movie", "Rating")

        val windowSpec = Window.partitionBy("User").orderBy("Rating")
        val averageRating = ratingData.withColumn("leadnewcolumn",lag("Rating",1).over(windowSpec))


        averageRating.show()

        val transactionsDF =
          List(
            (1, "2023-12-01", 1200, "Credit"),
            (2, "2023-11-15", 600, "Debit"),
            (3, "2023-12-20", 300, "Credit"),
            (4, "2023-10-10", 1500, "Debit"),
            (5, "2023-12-30", 250, "Credit"),
            (6, "2023-09-25", 700, "Debit")
          ).toDF("transaction_id", "transaction_date", "amount", "transaction_type")

        transactionAmountAndDataAnalysis(transactionsDF);

        val feedback = List(
        (1,"2024-01-10",4,"Great service!"),
        (2,"2024-01-15",5,"Excellent!"),
        (3,"2024-02-20",2,"Poor experience."),
        (4,"2024-02-25",3,"Good value."),
        (5,"2024-03-05",4,"Great quality."),
        (6,"2024-03-12",1,"Bad service.")
        ).toDF( "customer_id", "feedback_date", "rating", "feedback_text")


        customerFeedbackAnalysis(feedback);

        val shopData = List(
          (1, "KitKat", 1000.0,"2021-01-01"),
          (1, "KitKat", 2000.0,"2021-01-02"),
          (1, "KitKat", 1000.0,"2021-01-03"),
          (1, "KitKat", 2000.0,"2021-01-04"),
          (1, "KitKat", 3000.0,"2021-01-05"),
          (1, "KitKat", 1000.0,"2021-01-06")
        ).toDF("IT_ID", "IT_Name", "Price","PriceDate")

        val window_shopData = Window.partitionBy("IT_ID").orderBy("PriceDate")
        shopData.withColumn("DiffPrice",col("Price") - lag(col("Price"),1).over(window_shopData)).show()
    */

    val salesDFRaw =
      List(
        (1,"Widget",700,"2024-01-15"),
        (2,"Gadget",150,"2024-01-20"),
        (3,"Widget",350,"2024-02-15"),
        (4,"Device",600,"2024-02-20"),
        (5,"Widget",100,"2024-03-05"),
        (6,"Gadget",500,"2024-03-12")
      ).toDF("sale_id", "product_name", "sale_amount", "sale_date");

    productSalesAnalysis(salesDFRaw);



    /* Nishad
    Question 3 - Same as Set B Q2*/
    /*  Question 4 - Same as Set B Q2 In filter clause use contains*/
    /*  Question 5 - Same as previous Use date_format(col("transaction_date"),"YYYY")*/
    /*  Question 6 - Same as previous Use date_format(col("transaction_date"),"YYYY")*/
    /*  Question 7 - Same as Set B Q2*/
    /*  Question 8 - Same as Set B Q2*/
    /*  Question 8 - Same as Set B Q2*/
    /*Question 6 to 70 - Same as Set B Q2
    * Use date_format(col("purchase_date"),"MMMMM YYYY") for January 2024
    * DF.filter(col("columnname") === "January 2024")
    * If you are using to_date then MM should be capital ie yyyy-MM-dd yyyy and dd has to be lowercase
    *
    * */

    val project_budgets =
      List(
        (1,"New Website", 50000, 55000),
        (2,"Old Software", 30000, 25000),
        (3,"New App", 40000, 40000),
        (4,"New Marketing", 15000, 10000),
        (5,"Old Campaign", 20000, 18000),
        (6,"New Research", 60000, 70000)
      ).toDF("project_id","project_name","budget","spent_amount")


    projectBudgetTracking(project_budgets)



    /*
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



            val salesData = Seq(
              ("Product1", "Category1", 100),
              ("Product2", "Category2", 200),
              ("Product3", "Category1", 150),
              ("Product4", "Category3", 300),
              ("Product5", "Category2", 250),
              ("Product6", "Category3", 180)
            ).toDF("Product", "Category", "Revenue")

            val window_sales = Window.partitionBy("Category").orderBy("Product")

            salesData.withColumn("Running Sales Total",
              sum("revenue") over(window_sales)
            ).show()

            val ratingData = Seq(
              ("User1", "Movie1", 4.5),
              ("User1", "Movie2", 3.5),
              ("User1", "Movie3", 2.5),
              ("User1", "Movie4", 4.0),
              ("User1", "Movie5", 3.0),
              ("User1", "Movie6", 4.5),
              ("User2", "Movie1", 3.0),
              ("User2", "Movie2", 4.0),
              ("User2", "Movie3", 4.5),
              ("User2", "Movie4", 3.5),
              ("User2", "Movie5", 4.0),
              ("User2", "Movie6", 3.5)
            ).toDF("User", "Movie", "Rating")


            val rating_sales = Window.partitionBy("User").rowsBetween(-2,0)

            ratingData.withColumn("Average",
              avg("Rating") over(rating_sales)
            )//.select("User","Average").distinct()
              .show()

            val employees =
              List(
                (1,"John",28,60000),
                (2,"Jane",32,75000),
                (3,"Mike",45,120000),
                (4,"Alice",55,90000),
                (5,"Steve",62,110000),
                (6,"Claire",40,40000)
              ).toDF("employee_id","name","age","salary")

        val enhanced_empDF = employees.
          withColumn("age_group",
          when (col("age")<30,"Young").when(col("age").between(30,50),"Mid").when(col("age")>50,"Senior")).
          withColumn("salary_range", when (col("salary")>100000,"High").when(col("age").between(50000,100000),"Medium").otherwise("Low")
          )

            enhanced_empDF.show()

        val filtered_empDF_startJ = enhanced_empDF.filter(col("name").startsWith("J"))


            filtered_empDF_startJ.show()
            val filtered_empDF_ende = enhanced_empDF.filter(col("name").endsWith("e"))

            filtered_empDF_ende.show()

            val win_age_group = Window.partitionBy("age_group")

            enhanced_empDF.select(col("age_group"),sum("salary").over(win_age_group).alias("Total")
              ,avg("salary").over(win_age_group).alias("Average")
              ,min("salary").over(win_age_group).alias("Minimum")
              ,max("salary").over(win_age_group).alias("Maximum")).distinct().show()


            val movieDF = List(
              (1,"The Matrix",9,136),
              (2,"Inception",8,148),
              (3,"The Godfather",9,175),
              (4,"Toy Story 7",81),
              (5,"The Shawshank Redemption",10,142),
              (6,"The Silence of the Lambs",8,118)
            ).toDF("movie_id","movie_name","rating","duration_minutes")

            val enhanced_movieDF = movieDF.
              withColumn("rating_category",
                when (col("rating")>=8,"Excellent").when(col("age").between(6,8),"Good").otherwise("Average")).
              withColumn("duration_category", when (col("duration_minutes")>150,"Long").when(col("duration_minutes").between(90,150),"Medium").otherwise("Short")
              )

            enhanced_movieDF.show()

            val filteredMovieDFStartT = enhanced_movieDF.filter(col("movie_name").startsWith("T")).select("movie_name")


            filteredMovieDFStartT.show()
            val filteredMovieDFEndE = enhanced_movieDF.filter(col("movie_name").endsWith("e"))

            filtered_empDF_ende.show()

            val winRatingCategory = Window.partitionBy("rating_category")

            enhanced_movieDF.select(col("duration_category"),sum("duration_minutes").over(winRatingCategory).alias("Total")
              ,avg("duration_minutes").over(winRatingCategory).alias("Average")
              ,min("duration_minutes").over(winRatingCategory).alias("Minimum")
              ,max("duration_minutes").over(winRatingCategory).alias("Maximum")).distinct().show()

    */
    val df1=spark.read.format("csv").option("header",true).option("path","C:/Users/nnarse/Desktop/RDD/JOIN/info.csv").load()

    val df2=spark.read.format("csv").option("header",true).option("path","C:/Users/nnarse/Desktop/RDD/JOIN/details.csv").load()


    val condition=df1("id")===df2("id")
    val jointype="inner"
    val  joineddf=df1.join(broadcast(df2),condition,jointype).drop(df1("id"))

    joineddf.show()


    val emp_salary = List(
      (1,"John",1000,"01/01/2016"),
      (1,"John",2000,"02/01/2016"),
      (1,"John",1000,"03/01/2016"),
      (1,"John",2000,"04/01/2016"),
      (1,"John",3000,"05/01/2016"),
      (1,"John",1000,"06/01/2016")
    ).toDF("id","name","salary","date")

    val windowSpec = Window.partitionBy("id").orderBy(to_date(col("date"),"MM\\dd\\yyyy"))
    val win_emp_salary = emp_salary.withColumn("lagsalary",lag("salary",1).over(windowSpec)).
      withColumn("UP_DOWN",when(col("salary") - lag("salary",1).over(windowSpec) < 0,"DOWN").otherwise("UP") )
    win_emp_salary.show()

    val win_emp_salary_current = emp_salary.withColumn("Average Salary",avg("salary").over(windowSpec)).
      withColumn("Diff",col("salary") - avg("salary").over(windowSpec)).show()


    /*
    * Display the students whose total of two highest marks that atre atlease 160
    *
    * */

    val marks =
      List(
        ("A","X",80),
        ("A","Y",70),
        ("A","Z",75),
        ("B","X",90),
        ("B","Y",91),
        ("B","Z",75),
        ("C","X",60),
        ("C","Y",93),
        ("C","Z",81)
      ).toDF("studentid","subjectid","marksobtained")


    val window = Window.partitionBy("studentid").orderBy(col("marksobtained").desc )

    marks.withColumn("rank",row_number() over(window))
      .filter(col("rank")<=2)
      .groupBy(col("studentid")).agg(sum(col("marksobtained")) as("sumOfTwoHighestMarks"))
      .filter(col("sumOfTwoHighestMarks")>=160)
      .show()

    /**
     SELECT student_id,sumOfTwoHighestMarks
     (
     SELECT student_id , SUM(marks) as sumOfTwoHighestMarks
     (
     SELECT studentid,subjectid,marksobtained
     FROM
     (SELECT studentid,subjectid,marksobtained,
     row_number() over (partition by studentid order by marksobtained desc) as rank)
     WHERE rank<=2
     )
     GROUP BY student_id
     )
     WHERE sumOfTwoHighestMarks>=160
     */

    val employee_performance =
      List(
        ("E001","Sales",85,"2024-02-10","Sales Manager"),
        ("E002","HR",78,"2024-03-15","HR Assistant"),
        ("E003","IT",92,"2024-01-22","IT Manager"),
        ("E004","Sales",88,"2024-02-18","Sales Rep"),
        ("E005","HR",95,"2024-03-20","HR Manager")
      ).toDF("employee_id","department","performance_score","review_date","position")

    println(employee_performance.collect().foreach(println))

  }





}