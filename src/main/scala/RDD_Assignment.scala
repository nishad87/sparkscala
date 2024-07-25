import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object RDD_Assignment {
  def main(args:Array[String]):Unit={

    val sc = new SparkContext("local[4]", "nishad")

    val rdd1=sc.textFile("C:/Users/nnarse/Desktop/file1.txt") //--> SPark RDD
    val rdd2= rdd1.flatMap(x=>x.split(" "))
    val rdd3=rdd2.map(x=>(x,1))
    val rdd4=rdd3.reduceByKey((x,y)=>x+y)
    val rdd5=rdd4.sortBy(x=>x._2,false)
    rdd5.collect.foreach(println)//---- local scala variable


    val customerRDD:RDD[String]=sc.textFile("C:/Users/nnarse/Desktop/orders.txt") //--> SPark RDD
    val custRevenueRDD:RDD[(Int,Float)] = customerRDD.map(line=>(line.split(",")(1).trim.toInt,line.split(",")(3).trim.toFloat))
    val custRevenueRDDSum = custRevenueRDD.reduceByKey((x,y)=>{x+y})
    println("Set A Q1")
    custRevenueRDDSum.collect.foreach(println)

    /*
        val urlRDD:RDD[String]=sc.textFile("C:/Users/nnarse/Desktop/url.txt") //--> SPark RDD
        val urlRDDTuple:RDD[(String,Int)] = urlRDD.map(line=>(line.split(",")(2).trim,1))
        val urlRDDSum = urlRDDTuple.reduceByKey((x,y)=>{x+y})
        val sortedUrl=urlRDDSum.sortBy(x=>x._2,false)
        println("Set A Q2")
        sortedUrl.take(10).foreach(println)

        val sensorRDD = sc.textFile("C:/Users/nnarse/Desktop/sensor_data.txt")
                        .map(line=>(line.split(",")(0).toInt,line.split(",")(2).toFloat))
                        .reduceByKey((x,y)=>{(x+y)});

       sensorRDD.collect.foreach(println)
      */
    /*
    1,My Name is Nishad
    2,My Name is Shriyansh

    Array(
    1,My Name is Nishad,
    2,My Name is Shriyansh
    )

    Array(
    My Name is Nishad,
    My Name is Shriyansh
    )

    Array(
    Array(My,Name,is,Nishad),
    Array(My,Name,is,Shriyansh)
    )


    Array(
    My,Name,is,Nishad,
    My,Name,is,Shriyansh
    )

    Array(
    (My,1),
    (Name,1),
    (is,1),
    (Nishad,1),
    (My,1),
    (Name,1),
    (is,1),
    (Shriyansh,1)
    )

        Array(
    (My,2),
    (Name,2),
    (is,2),
    (Nishad,1),
    (Shriyansh,1)
    )
     */

    println("Set A 4")
    val docRDD:RDD[String]=sc.textFile("C:/Users/nnarse/Desktop/RDD/SETA/4.txt") //--> SPark RDD
    val docRDDTuple = docRDD.map(line=>{line.split(",")(1)})
      .flatMap(line=>line.split(" "))
      .map(word=>{(word,1)})
      .reduceByKey( (x,y) => {x+y} )
      .sortBy(x=>{x._2},false)

    docRDDTuple.collect().foreach(println)





    println("Set A 5")
    val productRDD:RDD[String]=sc.textFile("C:/Users/nnarse/Desktop/RDD/SETA/5.txt") //--> SPark RDD
    val productRDDTuple = productRDD.map(line=>{(line.split(",")(0).toInt,line.split(",")(2).toFloat)})
      .reduceByKey( (x,y) => {x+y} )
      .sortBy(x=>{x._1},true)

    productRDDTuple.collect().foreach(println)


    /*
    userid,movieid,rating
    1,1,3
    2,1,4
    3,1,2
    4,1,1
    1,2,5
    2,2,2
    3,2,2
    1,3,3
    2,3,4
    * */



    println("Set B 1")
    val movieRDD:RDD[String]=sc.textFile("C:/Users/nnarse/Desktop/RDD/SETB/1.txt") //--> SPark RDD
    val movieRDDTuple = movieRDD.map(line=>{(line.split(",")(1).toInt,line.split(",")(2).toFloat)})
      .reduceByKey( (x,y) => x.max(y) )
      .sortBy(x=>{x._2},false)

    movieRDDTuple.collect().foreach(println)

    /*
    APPL,'21-JUL-2024',1
    APPL,'21-JUL-2024',2
    APPL,'21-JUL-2024',3
    APPL,'21-JUL-2024',4
    APPL,'21-JUL-2024',5
    ORCL,'21-JUL-2024',11
    ORCL,'21-JUL-2024',12
    ORCL,'21-JUL-2024',13
    ORCL,'21-JUL-2024',14
    ORCL,'21-JUL-2024',25



     */



    println("Set B 2")
    val stockRDD:RDD[String]=sc.textFile("C:/Users/nnarse/Desktop/RDD/SETB/2.txt") //--> SPark RDD
    //stockRDD.collect().foreach(println)
    val stockRDDTuple = stockRDD.map(line=>{
        (
          (line.split(",")(0),line.split(",")(1)),(line.split(",")(2).toFloat,1.toFloat))
      })
      .reduceByKey((x,y)=>{(x._1 + y._1 , x._2 + y._2)})
      .map(t=> {
        (t._1,t._2._1/t._2._2)
      })
      .sortBy(x=> {x._2},false)
      .map(t=>{
        List(t._1._1  , t._1._2 , t._2)})

    stockRDDTuple.take(5).foreach(println)

    /*
    1,'REVIEW 1'
  2,'REVIEW 2'
  3,'REVIEW 3'
  4,'REVIEW 4'
  5,'REVIEW 5'
  6,'REVIEW 6'
  1,'REVIEW 7'
  2,'REVIEW 8'
  3,'REVIEW 9'
  4,'REVIEW 10'
  5,'REVIEW 11'
  6,'REVIEW 12'
  1,'REVIEW 13'
  1,'REVIEW 14'
  2,'REVIEW 15'
  6,'REVIEW 16'
     */


    println("Set B 3")

    val arr = "6,REVIEW 16"
    val no = arr.split(",")(1)
    print(no)

    val productReviewRDD:RDD[String]=sc.textFile("C:/Users/nnarse/Desktop/RDD/SETB/3.txt") //--> SPark RDD
    val productReviewRDDTup = productReviewRDD.map(line=> {
        (line.split(",")(0).toInt, 1)})
      .reduceByKey( (x,y) => x+y)
      .sortBy(x=>{x._2},false)

    productReviewRDDTup.collect().foreach(println)

    /*
    1,'26-07-2024',100
    1,'27-07-2024',101
    2,'27-07-2024',200
    2,'28-07-2024',67
     */


    println("Set B 4")
    val sensorRDD:RDD[String]=sc.textFile("C:/Users/nnarse/Desktop/RDD/SETB/4.txt") //--> SPark RDD
    val sensorRDDTuple = sensorRDD.map(line=>{(line.split(",")(0).toInt,line.split(",")(2).toFloat)})
      .reduceByKey( (x,y) => x.max(y) )
    //.sortBy(x=>{x._2},false)

    sensorRDDTuple.collect().foreach(println)

    /*
     (order_id, customer_id,order_date, order_total)
    1,1,'26-JUL-2024',1000
    2,1,'27-JUL-2024',2000
    3,2,'27-JUL-2024',10000
    4,3,'27-JUL-2024',2000
    5,2,'27-JUL-2024',2000
     */

    println("Set B 5")
    val orderRDD:RDD[String]=sc.textFile("C:/Users/nnarse/Desktop/RDD/SETB/5.txt") //--> SPark RDD
    val orderRDDTuple = orderRDD.map(line=>{(line.split(",")(1).toInt,line.split(",")(3).toFloat)})
      .reduceByKey( (x,y) => x+y)
      .sortBy(x=>{x._2},false)

    orderRDDTuple.collect().foreach(println)
    /*
        val urlRDDSum = urlRDDTuple.reduceByKey((x,y)=>{(x._1 + y._1 , x._2 + y._2)})
        val sortedUrl=urlRDDSum.sortBy(x=>x._2,false)
        println("Set A Q2")
        sortedUrl.take(10).foreach(println)

        val sensorRDD = sc.textFile("C:/Users/nnarse/Desktop/sensor_data.txt")
          .map(line=>(line.split(",")(0).toInt,line.split(",")(2).toFloat))
          .reduceByKey((x,y)=>{(x+y)})
          .sortBy(x=>x._2);

        sensorRDD.collect.foreach(println)



    val rdd2= rdd1.flatMap(x=>x.split(" "))
        val rdd3=rdd2.map(x=>(x,1))
        val rdd4=rdd3.reduceByKey((x,y)=>x+y)
        val rdd5=rdd4.sortBy(x=>x._2,false)


            //rdd5.collect.foreach(println)//---- local scala variable


        //val   rdd6 = rdd5.take(1);

            // (nishad,5)
        //(narse,5)
        //(india , 5)
        //(shri,4)


            for ((i,j) <-rdd5)
              {
                println(i + j)

              }
    */

    /*
        val sc=new SparkContext("local[*]","karthik")

        val rdd1=sc.textFile("C:/Users/nnarse/Desktop/file1.txt")
        val rdd2=rdd1.flatMap(x=>x.split(" "))
        val rdd3=rdd2.map(x=>(x,1))
        val rdd4=rdd3.reduceByKey((x,y)=>x+y)
        rdd4.collect.foreach(println)

        scala.io.StdIn.readLine()
        */
    /*
    val rdd = sc.parallelize(Array(Array(1,2),Array(3,4)))
    //val rdd = sc.parallelize()

    val rdd2 = rdd.map(x=>x)
    rdd2.collect().foreach(arr=>println(arr.deep));
    */
    /*
        val lmap = Map("a" -> List(1, 2), "b" -> List(3, 4));
        lmap.map(x=>println(x._2));


        val rdd =  sc.parallelize(Array(Map("a" -> List(1, 2), "b" -> List(3, 4))));

        //sc.parallelize(Map("a" -> List (1,2) , "b" -> List(3,4)))
        val rdd2 = rdd.map(kv=>kv.map(kv=>kv._1));    // Array(List(List(1,2),List(3,4)))    , Array(List("a","b"))
        //val rdd2 = rdd.map(kv=>{kv});    // Array(List(1,2),List(3,4))
        rdd2.collect.foreach(println)
    */

  }

}


