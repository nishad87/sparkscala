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

  def printPrimeInRange(lowerBound:Int , upperBound:Int)={
    for (i<-lowerBound to upperBound)
      {
        if (checkPrime(i)==true)
          {
            println(i)
          }
      }
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

  def cubeOfNumberFrom1to3UsingWhile():Unit={
    var i=1;
    while(i<=3)
    {
      println(i*i*i);
    }

  }

  def printElementsOfListUsingWhile():Unit={
    val namesList:List[String] =  List("Nishad","Mohan","Narse")
    var i=0
    while(i<=namesList.length-1)
    {
      println(namesList(i));
      i=i+1;
    }

  }

  def countNoOfVowelsUsingFor(inputString:String):Int={
    var counter:Int=0;
    for (i<-0 to inputString.length-1)
      {
        if (List('a','e','i','o','u').contains(inputString.charAt(i).toChar))
          {
            counter = counter+1;
          }
      }
      counter;
  }

  def checkPalindromeString(inputString:String):Boolean={
    var i = 0
    var j = inputString.length-1;
    var isPalindrome:Boolean = true
    val loop = new Breaks

    loop.breakable{
    while (i<=j)
      {
        if (inputString.charAt(i).compare(inputString.charAt(j))!=0)
          {
            isPalindrome = false
            loop.break()
          }
      i = i+1;
      j = j-1;
      }
    }
    isPalindrome
  }

  def noPattern():Unit={
    for (i<-1 to 4)
    {
      for (j<-1 to i)
      {
        print(i)
      }
      println();
    }
  }

  def productOfNumberFrom1To5():Unit={
    var product:Int = 1
    for (i<-1 to 5)
    {
      product = product * i
    }
    println(product);
  }
/*
  def productOfNumberFrom1To5():Unit={
    var product:Int = 1
    for (i<-1 to 5)
    {
      product = product * i
    }
    println(product);
  }
*/
  def checkPerfectSquareUsingWhileLoop(num:Int):Boolean ={
    val loopCheck: Int = num / 2.toInt
    var i = 1 ;
    var isPerfectSquare:Boolean = false;
    val loop = new Breaks

    loop.breakable{
    while (i<=loopCheck)
      {
        if (i*i == num)
          {
            isPerfectSquare = true
            loop.break()
          }
        i = i + 1;
      //  i = i * i;
      }
    }
    isPerfectSquare
  }

  def oddElementsOfArray():Unit={

    var arr = Array(10,20,30,40,50,60,70,80,90,100)
    for (i<-0 to arr.length-1)
      {
        if (i%2 == 1)
          {
            println (arr(i))
          }
      }


  }


  def conditionalSum():Unit={
    var sum = 0
    var arr = Array(10,20,40,60,70,80,90,110,30,22)

    arr.map(num=>{
      if (num%15 == 0)
        {
          sum = sum + num;
        }
    })
    println("Sum : " + sum)
    sum = 0

    // Attempting using traditional way
    for (i<-0 to arr.length-1)
    {

        if (arr(i)%15 == 0)
          {
            sum = sum + arr(i)
          }
          }

    print ("Sum"+sum)


  }
  def main (args : Array[String]):Unit={
    conditionalSum()
    println("PrintPrime")
    printPrimeInRange(1,100);
    oddElementsOfArray();

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
    println("#####")
    printElementsOfListUsingWhile();
    println("#####")
    print(countNoOfVowelsUsingFor("aeif"));
    println("#####")
    noPattern()
    println("Palindrome check")
    println(checkPalindromeString("nayan"))
    println(checkPalindromeString("nishad"))
    println(checkPalindromeString("abc"))
    println(checkPalindromeString("a"))
    println("Product from 1 to 5")
    productOfNumberFrom1To5()
    println("CheckPerfectSquare")
    println(checkPerfectSquareUsingWhileLoop(25));
    println(checkPerfectSquareUsingWhileLoop(125));
    println(checkPerfectSquareUsingWhileLoop(67));
    println(checkPerfectSquareUsingWhileLoop(144));
    //    print(checkPerfectSquareUsingWhileLoop(125));
//    print(checkPerfectSquareUsingWhileLoop(167))

  }

}
