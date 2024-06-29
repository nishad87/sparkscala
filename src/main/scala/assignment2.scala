import scala.util.control.Breaks

object assignment2 {

  def printNumbers1To5():Unit={
    for (i<-1 to 5 )
      {
        println(i)
      }
  }

  def printEvenNumberFrom2to10UsingWhile():Unit={
    var i = 2
    while (i<=10)
      {
        println(i);
        i=i+2;
      }
  }

  def printSumofNumbers1To50():Unit={
    var sum = 0;
    for (i<-1 to 50 )
    {
      sum = sum + i;
    }
    println("Sum : "+sum)
  }

  def printSquareFrom1To5():Unit={
    for (i<-1 to 5 )
    {
      println(i*i);
    }

  }

  def reverseListUsingWhileLoop():Unit={
    val namesList:List[String] = List("Nishad","Mohan","Narse")
    var i=namesList.length-1;
    while(i>=0)
    {
      println(namesList(i));
      i=i-1;
    }

  }

  def firstFiveMultiplesOf3():Unit={
    for (i<-1 to 5 )
    {
      println(i*3);
    }

  }
  def oddFromOneToFifteenUsingWhile():Unit={
    var i=1;
    while (i<=15 )
    {
      if (i%2==1)
        {
          println(i);
        }
      i=i+1;
    }

  }

  def factorialOfANumber(num:Int):Unit={
    var i=num;
    var fact=1
    while (i>=1 )
    {
      fact = fact * i;
      i=i-1;
    }
  print(fact)
  }

  def charOfStringInReverseOrderUsingForLoop(inputString:String):Unit={

    for (i<-inputString.length-1 to 0 by -1)
      {
        print(inputString.charAt(i));
      }
  }

  def checkPrime(number:Int):Boolean={
    val checkVal = (number/2).toInt;
    var i = 2;
    var isPrime = true;
    val loop = new Breaks;
    loop.breakable {
      while (i <= checkVal) {
        if (number % i == 0) {
          isPrime = false;
          loop.break();
        }
        i=i+1;

      }
    }
    return isPrime;
  }
  def reverseListUsingForLoop():Unit={
    val namesList:List[String] = List("Nishad","Mohan","Narse")
    var i=namesList.length-1;
    for(i <- namesList.length-1 to 0 by -1)
    {
      println(namesList(i));
    }

  }

  def sumOfEvenNumberFrom1to20UsingWhileLoop():Unit={
   var i = 1;
    var sum = 0
    while(i <=20)
    {
      if (i%2==0)
        {
          sum = sum+i
        }
        i=i+1;
    }
    print(sum);

  }

  def starPattern():Unit={

    for (i<-1 to 5)
      {
        for (j<-1 to i)
          {
            print("*")
          }
          println();
      }

  }
  def main (args : Array[String]):Unit={
    printNumbers1To5();
    println("#####")
    printEvenNumberFrom2to10UsingWhile();
    println("#####")
    printSumofNumbers1To50()
    println("#####")
    printSquareFrom1To5()
    println("#####")
    reverseListUsingWhileLoop();
    println("#####")
    firstFiveMultiplesOf3();
    println("#####")
    oddFromOneToFifteenUsingWhile();
    println("#####")
    factorialOfANumber(4);
    println("#####")
    charOfStringInReverseOrderUsingForLoop("Nishad")
    println("#####")
    print(checkPrime(29));
    println("#####")
    reverseListUsingForLoop();
    println("#####")
    sumOfEvenNumberFrom1to20UsingWhileLoop();
    println("#####")
    starPattern();
  }

}
